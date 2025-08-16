#######################################
# Notes
# To use this service, an entry in the Dataset table with the repository name and id must exist!
#######################################
from datetime import datetime
import os
import shutil
import glob
import sys
import re
from uuid import uuid4
from io import StringIO
import pandas as pd
import zipfile
import time
import requests
import fnmatch


from app.services.VersioningService import StarVersService
from app.models.TrackingTaskModel import TrackingTaskDto
from app.utils.graphdb.GraphDatabaseUtils import recreate_repository, \
 get_snapshot_metrics_template, get_dataset_static_core_template, get_all_creation_timestamps, \
 get_dataset_version_oblivious_template, create_engine
from app.enums.DeltaTypeEnum import DeltaType
from app.LoggingConfig import get_logger, setup_logging
from app.Database import Session, engine
from app.models.DatasetModel import Dataset, Snapshot
from app.services.ManagementService import get_id_by_repo_name, \
 delete_snapshot_metrics_by_dataset_id, \
 delete_snapshot_metrics_by_dataset_id_and_ts
from app.AppConfig import Settings

#######################################
# Input parameters
#######################################
repo_name = sys.argv[1]
delta_calc_method = sys.argv[2].upper()
versioning_mode = sys.argv[3] if len(sys.argv) >= 4 else "from_scratch"
start_timestamp = sys.argv[4] if len(sys.argv) >= 5 else None


#######################################
# Logging
#######################################
logger = get_logger(__name__, f"tracking_{repo_name}.log")
setup_logging()


#######################################
# Functions
#######################################
def compute_snapshot_statistics(sparql_engine, session: Session, dataset_id, snapshot_ts_prev: datetime, snapshot_ts: datetime = None):
    # Retrieve metrics from GraphDB via SPARQL query in the csv format
    logger.info(f"Repository name: {repo_name}: Querying snapshot metrics from GraphDB")

    query = get_snapshot_metrics_template(ts_current=snapshot_ts, ts_prev=snapshot_ts_prev)
    sparql_engine.setQuery(query)
    response = sparql_engine.query().convert() 

    # Parse CSV using pandas
    csv_text = response.decode('utf-8')
    df_metrics = pd.read_csv(StringIO(csv_text))

    snapshots = []
    for _, row in df_metrics.iterrows():
        snapshot = Snapshot(
            dataset_id=dataset_id,
            snapshot_ts=snapshot_ts,
            snapshot_ts_prev=snapshot_ts_prev,
            onto_class=row["onto_class"],
            parent_onto_class=row["parent_onto_class"] if pd.notna(row["parent_onto_class"]) else None,
            cnt_class_instances_current=row["cnt_class_instances_current"],
            cnt_class_instances_prev=row["cnt_class_instances_prev"],
            cnt_classes_added=row["cnt_classes_added"],
            cnt_classes_deleted=row["cnt_classes_deleted"]
        )
        snapshots.append(snapshot)
    
    if snapshots:
        logger.info(f"Repository name: {repo_name}: Inserting {len(df_metrics)} computed metrics into 'snapshot' table: set all fields")
        session.add_all(snapshots)
        session.commit()
        for snap in snapshots:
            session.refresh(snap)         


def compute_dataset_metrics(sparql_engine, session, dataset):
    # Update dataset table
    logger.info(f"Repository name: {repo_name}: Updating 'dataset' table")

    # Update dataset table: static core triples 
    query = get_dataset_static_core_template()
    sparql_engine.setQuery(query)
    response = sparql_engine.query().convert() 
    csv_text = response.decode('utf-8')
    df_static_core = pd.read_csv(StringIO(csv_text))
    value_int64 = df_static_core.at[0, "cnt_triples_static_core"]
    dataset.cnt_triples_static_core = int(value_int64) if pd.notna(value_int64) else None

    # Update dataset table: version oblivious triples 
    query = get_dataset_version_oblivious_template()
    sparql_engine.setQuery(query)
    response = sparql_engine.query().convert() 
    csv_text = response.decode('utf-8')
    df_vers_obl = pd.read_csv(StringIO(csv_text))
    df_vers_obl = pd.read_csv(StringIO(csv_text))
    value_int64 = df_vers_obl.at[0, "cnt_triples_version_oblivious"]
    dataset.cnt_triples_version_oblivious = int(value_int64) if pd.notna(value_int64) else None

    logger.info(
        f"Repository name: {repo_name}:\n"
        f"Updating next runtime to {dataset.next_run}\n"
        f"Updating static core triples to {dataset.cnt_triples_static_core}\n"
        f"and version oblivious triples to {dataset.cnt_triples_version_oblivious} in 'dataset' table."
    )

    session.commit()
    session.refresh(dataset)
    

def convert_timestamp_str_to_iso(timestamp_str) -> datetime:
    """
    Convert a timestamp string 'yyyyMMdd-hhmmss_SSS' to a datetime object.
    Validates the format strictly and ensures milliseconds are padded.
    """
    if not timestamp_str:
        raise ValueError("Timestamp string cannot be empty or None.")

    if not re.fullmatch(r"\d{8}-\d{6}_\d{3}", timestamp_str):
        raise ValueError(
            f"Invalid timestamp format: '{timestamp_str}'. Expected 'yyyyMMdd-hhmmss_SSS'."
        )

    # Pad milliseconds to microseconds for parsing
    base, millis = timestamp_str.split('_')
    padded = f"{base}_{millis.ljust(6, '0')}"

    try:
        dt = datetime.strptime(padded, "%Y%m%d-%H%M%S_%f")
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


def extract_timestamp(file_path):
    filename = os.path.basename(file_path)
    timestamp_pattern = re.compile(r'(\d{8}-\d{6}_\d{3})')
    match = timestamp_pattern.search(filename)
    return match.group(1) if match else ''


#######################################
# Validation
#######################################
# Validate versioning_mode 
allowed_modes = ["from_scratch", "from_version"]
if versioning_mode not in allowed_modes:
    raise ValueError(f"Invalid versioning mode: '{versioning_mode}'. Allowed values: {allowed_modes}")

# Validate combination: from_version requires timestamp 
if versioning_mode == "from_version" and start_timestamp is None:
        raise ValueError("Versioning mode 'from_version' requires a valid start timestamp.")

# Validate start_timestamp
if start_timestamp:
    if not re.fullmatch(r"\d{8}-\d{6}_\d{3}", start_timestamp):
        raise ValueError(
            f"Invalid timestamp format: '{start_timestamp}'. Expected 'yyyyMMdd-hhmmss_SSS'."
        )
    start_timestamp_iso = convert_timestamp_str_to_iso(start_timestamp)

sparql_engine = create_engine(repo_name)
logger.info(f"Versioning mode: {versioning_mode}")
if versioning_mode == "from_version":
    query = get_all_creation_timestamps()
    sparql_engine.setQuery(query)
    response = sparql_engine.query().convert() 
    csv_text = response.decode('utf-8')
    df_creation_timestamps = pd.read_csv(StringIO(csv_text))

    # Convert df_creation_timestamps['valid_from'] to datetime
    df_creation_timestamps["valid_from"] = pd.to_datetime(
        df_creation_timestamps["valid_from"],
        format="%Y-%m-%dT%H:%M:%S.%f",  # Adjust if your SPARQL returns a different format
        errors="coerce"
    )

    if df_creation_timestamps.empty:
        raise ValueError("No valid creation timestamps found in triple store.")

    latest_ts_iso = df_creation_timestamps["valid_from"].max()

    # Compare
    if start_timestamp_iso <= latest_ts_iso:
        raise ValueError(
            f"In the 'from_version' versioning mode, the timestamp needs to be a snapshot timestamp "
            f"that is newer than the latest timestamp in the triple store.\n"
            f"Provided: {start_timestamp_iso}, Latest in store: {latest_ts_iso}"
        )


# Validate delta type 
try:
    delta_type = DeltaType[delta_calc_method]
except KeyError:
    logger.info("Invalid delta type. Use 'SPARQL' or 'ITERATIVE'.")
    sys.exit(1)




#######################################
# Cleaning and preparation
#######################################
logger.info("Starting with building an RDF-star dataset from individual snapshots...")
logger.info(f"Repository name: {repo_name}, Delta type: {delta_type}, Versioning mode: {versioning_mode}, start_timestamp: {start_timestamp}")

# Define tracking task with input parameters
tracking_task = TrackingTaskDto(
    id=uuid4(),
    name=repo_name,
    rdf_dataset_url="",
    delta_type=delta_type
)

# RDF datasets directory
evaluation_dir = f"./evaluation/{tracking_task.name}"
logger.info(f"Evaluation directory: {evaluation_dir}")

# Extract all timestamps from the zip file names
logger.info(f"Extracting timestamps from file names in {evaluation_dir}")
zip_files = glob.glob(os.path.join(evaluation_dir, "*.zip"))

# Build mapping: {timestamp_str: raw.nt-file_path}
logger.info(f"Creating timestamps-file mapping")
file_timestamp_pairs = []
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


# Delete snapshot table where dataset_id corresponds to the repository_name
logger.info(f"Deleting snapshot metrics for {repo_name}")
with Session(engine) as session:
    if start_timestamp and versioning_mode == "from_version":
        delete_snapshot_metrics_by_dataset_id_and_ts(repo_name, start_timestamp_iso, session)
    else:
        # Delete all snapshot metrics
        delete_snapshot_metrics_by_dataset_id(repo_name, session)
        
        # Recreate repository
        wait_for_graphdb(f"{Settings().graph_db_url}/rest/repositories")
        recreate_repository(repo_name)


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
    if start_timestamp:
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
    if start_timestamp:
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

#######################################
# Versioning and statistics compution
#######################################
versioning_service = StarVersService(tracking_task, repo_name)
versioning_service.local_file = True 


with Session(engine) as session:
    # Get id by repo name
    dataset_id = get_id_by_repo_name(repo_name, session)
    dataset = session.get(Dataset, dataset_id)

    if versioning_mode == "from_scratch":
        # Run initial versioning for the tracking task
        init_version_timestmap, first_file = file_timestamp_pairs[start_idx]
        logger.info(f"First file: {first_file}, Timestamp: {init_version_timestmap}")
        init_version_timestmap_iso = convert_timestamp_str_to_iso(init_version_timestmap)
        logger.info(f"Initial version timestamp: {init_version_timestmap_iso}")

        versioning_service.run_initial_versioning(version_timestamp=init_version_timestmap_iso)

        # Compute metrics for initial snapshot
        compute_snapshot_statistics(sparql_engine, session, dataset_id, init_version_timestmap_iso, init_version_timestmap_iso)

        # Iterate over all files, starting from the second oldest
        latest_timestamp = init_version_timestmap_iso
        for timestamp_str, zip_file in file_timestamp_pairs[start_idx+1:]:
            version_timestamp = convert_timestamp_str_to_iso(timestamp_str)

            # version snapshot
            versioning_service.run_versioning(version_timestamp=version_timestamp)

            # compute snapshot metrics
            compute_snapshot_statistics(sparql_engine, session, dataset_id, latest_timestamp, version_timestamp)
            latest_timestamp = version_timestamp

            # Compute dataset metrics
            compute_dataset_metrics(sparql_engine, session, dataset)
    else: # from_version 
        # Get the latest timestamp (previouss), considering that the version at start_idx is being processed
        latest_timestamp, _ = file_timestamp_pairs[start_idx-1]
        latest_timestamp = convert_timestamp_str_to_iso(latest_timestamp)
        
        # Iterate over all files, starting from the second oldest
        for timestamp_str, zip_file in file_timestamp_pairs[start_idx:]:
            version_timestamp = convert_timestamp_str_to_iso(timestamp_str)

            # version snapshot
            versioning_service.run_versioning(version_timestamp=version_timestamp)

            # compute snapshot metrics
            compute_snapshot_statistics(sparql_engine, session, dataset_id, latest_timestamp, version_timestamp)
            latest_timestamp = version_timestamp

        # Compute dataset metrics
        compute_dataset_metrics(sparql_engine, session, dataset)

# Clean up
logger.info(f"Deleting all *.raw.nt files")
raw_files = glob.glob(os.path.join(evaluation_dir, "*.raw.nt"))
for raw_file in raw_files:
    os.remove(raw_file)

logger.info("Retro versioning completed successfully.")


