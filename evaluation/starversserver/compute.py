#######################################
# Notes
# To use this service, an entry in the Dataset
# table with the repository name and id must exist!
#######################################
from datetime import datetime
import os
import shutil
import glob
import sys
import re
from uuid import uuid4, UUID
from io import StringIO
import pandas as pd
import zipfile
import time
import requests
import fnmatch
from typing import Optional


from app.services.VersioningService import StarVersService
from app.services.ManagementService import get_id_by_repo_name
from app.services.MetricsService import MetricsService
from app.models.TrackingTaskModel import TrackingTaskDto
from app.persistance.graphdb.GraphDatabaseUtils import recreate_repository, get_all_creation_timestamps, create_engine
from app.LoggingConfig import get_logger, setup_logging
from app.persistance.Database import Session, engine
from app.models.DatasetModel import Dataset
from app.AppConfig import Settings

#######################################
# Logging
#######################################
logger = get_logger(__name__, f"compute.log")
setup_logging()

#######################################
# Input parameters
#######################################
repo_name = sys.argv[1]

# Defaults
versioning_mode = "from_scratch"
functions_to_run = {"v", "sm", "dm"}  # run all by default
start_timestamp = None

# Parse remaining arguments (order-invariant)
for arg in sys.argv[2:]:
    if arg in ["from_scratch", "from_version"]:
        versioning_mode = arg
    elif re.fullmatch(r"\d{8}-\d{6}_\d{3}", arg):  # timestamp
        start_timestamp = arg
    else:
        # parse functions list
        funcs = [f.strip().lower() for f in arg.split(",")]
        valid_funcs = {"v", "sm", "dm"}
        invalid = [f for f in funcs if f not in valid_funcs]
        if invalid:
            raise ValueError(f"Invalid function(s): {invalid}. Allowed: {valid_funcs}")
        functions_to_run = set(funcs)

logger.info(f"Parsed arguments: repo={repo_name}, "
            f"mode={versioning_mode}, timestamp={start_timestamp}, "
            f"functions={functions_to_run}")

#######################################
# Functions
#######################################
def convert_timestamp_str_to_iso(timestamp_str: str) -> datetime:
    """
    Convert a timestamp string 'yyyyMMdd-hhmmss_SSS' to a datetime object.
    """
    if not timestamp_str:
        raise ValueError("Timestamp string cannot be empty or None.")

    if not re.fullmatch(r"\d{8}-\d{6}_\d{3}", timestamp_str):
        raise ValueError(
            f"Invalid timestamp format: '{timestamp_str}'. Expected 'yyyyMMdd-hhmmss_SSS'."
        )

    try:
        dt = datetime.strptime(timestamp_str, "%Y%m%d-%H%M%S_%f")
    except ValueError as e:
        raise ValueError(
            f"Timestamp contains invalid date/time values: '{timestamp_str}'"
        ) from e

    return dt


def wait_for_graphdb(url: str, timeout: int = 60, interval: int = 3):
    """
    Wait for GraphDB to become available.
    """
    logger.info(f"Waiting for GraphDB at {url} to become available...")
    start_time = time.time()

    while time.time() - start_time < timeout:
        try:
            response = requests.get(url)
            if response.status_code == 200:
                logger.info("GraphDB is up and running.")
                return
        except requests.exceptions.ConnectionError:
            logger.debug("GraphDB not yet reachable.")

        time.sleep(interval)

    logger.error(f"Timed out waiting for GraphDB at {url}")
    sys.exit(1)


def run_versioning(repo_name: str, file_timestamp_pairs: list[tuple[str, str]], start_idx: int,
                    versioning_mode: str, tracking_task: TrackingTaskDto, start_timestamp_iso: Optional[datetime]):
    logger.info(f"Run preparations for versioning")

    # Delete all directory starting with a timestamp
    logger.info(f"Deleting all temporary directories starting with a timestamp in {evaluation_dir}")
    for entry in os.listdir(evaluation_dir):
        full_path = os.path.join(evaluation_dir, entry)
        if os.path.isdir(full_path):
            shutil.rmtree(full_path)

    # Unzip all zip files. 
    logger.info(f"Unzipping archives (only *raw.nt) in {evaluation_dir}")
    for _, zip_file in file_timestamp_pairs[start_idx:]:
        # Extract timestamp from filename (without path/extension)
        if start_timestamp_iso:
            filename = os.path.basename(zip_file).replace(".zip", "")
            try:
                file_dt_iso = convert_timestamp_str_to_iso(filename)
            except ValueError:
                logger.warning(f"Skipping file with invalid timestamp format: {filename}")
                continue

            if file_dt_iso < start_timestamp_iso:
                continue
            
        try:
            with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                for member in zip_ref.namelist():
                    if fnmatch.fnmatch(member, "*raw.nt"):
                        logger.debug(f"Extracting {member}")
                        zip_ref.extract(member, evaluation_dir)
        except zipfile.BadZipFile:
            logger.error(f"Bad zip file: {zip_file}")

    # Delete all zip archives
    logger.info(f"Removing archives in {evaluation_dir}")
    for zip_file in zip_files[start_idx:]:
        # Extract timestamp from filename (without path/extension)
        if start_timestamp_iso:
            filename = os.path.basename(zip_file).replace(".zip", "")
            try:
                file_dt_iso = convert_timestamp_str_to_iso(filename)
            except ValueError:
                logger.warning(f"Skipping file with invalid timestamp format: {filename}")
                continue

            if file_dt_iso < start_timestamp_iso:
                continue

        os.remove(zip_file)
    
    logger.info("Preparation finished.")

    logger.info("Starting with building an RDF-star dataset from individual snapshots...")
    versioning_service = StarVersService(tracking_task, repo_name)
    versioning_service.local_file = True 

    if versioning_mode == "from_scratch":
        # Recreating GraphDB repository
        recreate_repository(repo_name)

        # Run initial versioning for the tracking task
        init_version_timestmap, first_file = file_timestamp_pairs[start_idx]
        logger.info(f"First file: {first_file}, Timestamp: {init_version_timestmap}")
        
        init_version_timestmap_iso = convert_timestamp_str_to_iso(init_version_timestmap)
        logger.info(f"Initial version timestamp: {init_version_timestmap_iso}")

        versioning_service.run_initial_versioning(version_timestamp=init_version_timestmap_iso)

        # Iterate over all files, starting from the second oldest
        for timestamp_str, zip_file in file_timestamp_pairs[start_idx+1:]:
            # version snapshot
            version_timestamp = convert_timestamp_str_to_iso(timestamp_str)
            versioning_service.run_versioning(version_timestamp=version_timestamp)

    else: # from_version         
        # Iterate over all files, starting from the second oldest
        for timestamp_str, zip_file in file_timestamp_pairs[start_idx:]:
            # version snapshot
            version_timestamp = convert_timestamp_str_to_iso(timestamp_str)
            versioning_service.run_versioning(version_timestamp=version_timestamp)
    
    # Clean up
    logger.info(f"Cleaning up. Deleting all *.raw.nt files")
    raw_files = glob.glob(os.path.join(evaluation_dir, "*.raw.nt"))
    for raw_file in raw_files:
        os.remove(raw_file)


def run_snapshot_metrics_computation(metrics_service: MetricsService, dataset_id: UUID, file_timestamp_pairs: list[tuple[str,str]], 
                                     start_idx: int, versioning_mode: str, start_timestamp_iso: Optional[datetime]): 
    # Load {repo_name}_timings.csv
    timings_file = os.path.join(evaluation_dir, f"{repo_name}_timings.csv")
    if not os.path.exists(timings_file):
        raise FileNotFoundError(f"Timings file not found: {timings_file}")
    
    timings_df = pd.read_csv(timings_file)
    if timings_df.empty:
        raise ValueError(f"Timings file is empty: {timings_file}")
    
    # Strip leading whitespace from column names
    timings_df.columns = timings_df.columns.str.strip()
    
    # Convert 'timestamp' column to datetime
    timings_df["timestamp"] = pd.to_datetime(timings_df["timestamp"], format="%Y%m%d-%H%M%S_%f", errors="coerce")
    
    if versioning_mode == "from_version" and start_timestamp_iso:
        # Delete all snapshot metrics starting from start_timestamp_iso
        logger.info(f"Deleting snapshot metrics for {repo_name} starting from {start_timestamp_iso}")
        metrics_service.delete_snapshot_metrics_by_dataset_id_and_ts(repo_name, start_timestamp_iso)
    
        logger.info(f"Computing metrics for all snapshots starting from version {start_timestamp_iso}.")
        latest_timestamp, _ = file_timestamp_pairs[start_idx-1]
        latest_timestamp = convert_timestamp_str_to_iso(latest_timestamp)
        for timestamp_str, _ in file_timestamp_pairs[start_idx:]:
            version_timestamp = convert_timestamp_str_to_iso(timestamp_str)
            
            # if there are 0 been insertions and deletions for the row
            # where the version_timestamp matchjes the timestamp column in timings_df
            if not timings_df[(timings_df["timestamp"] == version_timestamp) &
                              (timings_df["insertions"] == 0) &
                              (timings_df["deletions"] == 0)].empty:
                logger.info(f"Skipping metrics computation for {version_timestamp} as there are no changes.")
            else:
                metrics_service.update_class_statistics(dataset_id, repo_name, version_timestamp, latest_timestamp)
                metrics_service.update_property_statistics(dataset_id, repo_name, version_timestamp, latest_timestamp)
            latest_timestamp = version_timestamp
    
    else: # from_scratch
        # Delete all snapshot metrics
        metrics_service.delete_snapshot_metrics_by_dataset_id(repo_name)
    
        # Compute metrics for initial snapshot
        logger.info(f"Computing metrics for initial snapshot,")
        init_version_timestmap, _ = file_timestamp_pairs[start_idx]
        init_version_timestmap_iso = convert_timestamp_str_to_iso(init_version_timestmap)
        metrics_service.update_class_statistics(dataset_id, repo_name, init_version_timestmap_iso, init_version_timestmap_iso)
        metrics_service.update_property_statistics(dataset_id, repo_name, init_version_timestmap_iso, init_version_timestmap_iso)

        logger.info(f"Computing metrics for all other snapshot.")
        latest_timestamp = init_version_timestmap_iso
        for timestamp_str, _ in file_timestamp_pairs[start_idx+1:]:
            version_timestamp = convert_timestamp_str_to_iso(timestamp_str)
            
            # if there are 0 been insertions and deletions for the row
            # where the version_timestamp matchjes the timestamp column in timings_df
            if not timings_df[(timings_df["timestamp"] == version_timestamp) &
                              (timings_df["insertions"] == 0) &
                              (timings_df["deletions"] == 0)].empty:
                logger.info(f"Skipping metrics computation for {version_timestamp} as there are no changes.")
            else:
                metrics_service.update_class_statistics(dataset_id, repo_name, version_timestamp, latest_timestamp)
                metrics_service.update_property_statistics(dataset_id, repo_name, version_timestamp, latest_timestamp)
            latest_timestamp = version_timestamp


def extract_timestamp(file_path: str) -> str: 
    filename = os.path.basename(file_path) 
    timestamp_pattern = re.compile(r'(\d{8}-\d{6}_\d{3})') 
    match = timestamp_pattern.search(filename) 
    return match.group(1) if match else ''


#######################################
# Validation and Preparation
#######################################
# Validate versioning_mode 
allowed_modes = ["from_scratch", "from_version"]
if versioning_mode not in allowed_modes:
    raise ValueError(f"Invalid versioning mode: '{versioning_mode}'. Allowed values: {allowed_modes}")

# Validate combination: from_version requires timestamp 
if versioning_mode == "from_version" and start_timestamp is None:
        raise ValueError("Versioning mode 'from_version' requires a valid start timestamp.")

# Validate start_timestamp
start_timestamp_iso = None
if start_timestamp:
    if not re.fullmatch(r"\d{8}-\d{6}_\d{3}", start_timestamp):
        raise ValueError(
            f"Invalid timestamp format: '{start_timestamp}'. Expected 'yyyyMMdd-hhmmss_SSS'."
        )
    start_timestamp_iso = convert_timestamp_str_to_iso(start_timestamp)

sparql_engine = create_engine(repo_name)
logger.info(f"Versioning mode: {versioning_mode}")
if versioning_mode == "from_version":
    logger.info("Querying all creation timestamps.")
    query = get_all_creation_timestamps()
    sparql_engine.setQuery(query)
    response = sparql_engine.query().convert() 
    if isinstance(response, bytes):
        csv_text = response.decode('utf-8')
        df_creation_timestamps = pd.read_csv(StringIO(csv_text))
    else:
        raise ValueError("Unexpected response format from SPARQL query. Should be CSV bytes.")


    # Convert df_creation_timestamps['valid_from'] to datetime
    df_creation_timestamps["valid_from"] = pd.to_datetime(
        df_creation_timestamps["valid_from"],
        errors="coerce"
    )

    if df_creation_timestamps.empty:
        raise ValueError("No valid creation timestamps found in triple store.")

    latest_ts_iso = df_creation_timestamps["valid_from"].max()

    # Compare
    if start_timestamp_iso <= latest_ts_iso and "v" in functions_to_run:
        raise ValueError(
            f"In the 'from_version' mode and the 'v(ersion)' function, the start timestamp needs to be a snapshot timestamp "
            f"that is newer than the latest timestamp in the triple store.\n"
            f"Provided: {start_timestamp_iso}, Latest in store: {latest_ts_iso}"
        )
    
    if not (df_creation_timestamps["valid_from"] == start_timestamp_iso).any() \
        and ('sm' in functions_to_run or 'dm' in functions_to_run) \
        and not 'v' in functions_to_run:
        raise ValueError(
            f"In the 'from_version' mode, when running the metrics functions "
            f"'dm' or 'sm' without the versioning function 'v', the start timestamp "
            f"needs to be inside the snapshot timestamps that are present in the repository {repo_name}. "
            f"I.e., snapshot and dataset metrics can only be computed for existing snapshots."
        )

# Define tracking task with input parameters
tracking_task = TrackingTaskDto(
    id=uuid4(),
    name=repo_name,
    rdf_dataset_url=""
)

# RDF datasets directory
evaluation_dir = f"/data/evaluation/{tracking_task.name}"
logger.info(f"Evaluation directory: {evaluation_dir}")

# Extract all timestamps from the zip file names
logger.info(f"Extracting timestamps from file names in {evaluation_dir}")
zip_files = glob.glob(os.path.join(evaluation_dir, "*.zip"))

if not zip_files:
    raise FileNotFoundError(f"No zip files found in {evaluation_dir}")

# Build mapping: {timestamp_str: raw.nt-file_path}
logger.info(f"Creating timestamps-file mapping")
file_timestamp_pairs: list[tuple[str,str]] = []
for file_path in zip_files:
    timestamp_str = extract_timestamp(file_path)
    if timestamp_str:
        file_timestamp_pairs.append((timestamp_str, file_path))

# Sort files by timestamp
logger.info("sorting timestamps-file mapping by timestamps")
file_timestamp_pairs.sort(key=lambda x: convert_timestamp_str_to_iso(x[0]))

# Set start index for file_timestamp_pairs based on versioning mode and start_timestamp
start_idx = 0
if start_timestamp:
    # Ensure start_timestamp is in the list
    timestamps_list = [ts for ts, _ in file_timestamp_pairs]
    if start_timestamp not in timestamps_list:
        raise ValueError(
            f"Start timestamp '{start_timestamp}' not found in available archives. "
            f"Valid timestamps: {timestamps_list}"
        )

    # Check if start_timestamp is the very first available timestamp
    if start_timestamp == timestamps_list[0]:
        logger.warning(
            f"Start timestamp '{start_timestamp}' is the first available timestamp. "
            f"Switching versioning mode from '{versioning_mode}' to 'from_scratch'."
        )
        versioning_mode = "from_scratch"

    # Find index of start_timestamp in sorted list
    start_idx = timestamps_list.index(start_timestamp)

# Wait for GraphDB to startup
wait_for_graphdb(f"{Settings().graph_db_url}/rest/repositories")



#######################################
# Versioning and statistics compution
#######################################
with Session(engine) as session:
    # Get id by repo name
    dataset_id = get_id_by_repo_name(repo_name, session)
    dataset = session.get(Dataset, dataset_id) 
    if not dataset:
        raise ValueError(f"Dataset with repo name '{repo_name}' not found in the database.")

    # run versioning
    if "v" in functions_to_run:
        run_versioning(repo_name, file_timestamp_pairs, start_idx, versioning_mode, tracking_task, start_timestamp_iso)

    metrics_service = MetricsService(sparql_engine, session)
    # Compute metrics for all snapshots
    if "sm" in functions_to_run:
        run_snapshot_metrics_computation(metrics_service, dataset_id, file_timestamp_pairs,
            start_idx, versioning_mode, start_timestamp_iso)

    # Compute dataset metrics
    if "dm" in functions_to_run:
        metrics_service.update_static_core_triples(dataset, repo_name)
        metrics_service.update_version_oblivious_triples(dataset, repo_name)

logger.info("Task completed successfully.")


