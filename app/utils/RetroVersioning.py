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
from app.utils.graphdb.GraphDatabaseUtils import recreate_repository, get_snapshot_metrics_template, create_engine
from app.enums.DeltaTypeEnum import DeltaType
from app.LoggingConfig import get_logger, setup_logging
from app.Database import Session, engine
from app.models.DatasetModel import Dataset, Snapshot
from app.services.ManagementService import get_id_by_repo_name, delete_snapshot_metrics_by_dataset_id
from app.AppConfig import Settings

# Input parameters
repo_name = sys.argv[1]
delta_calc_method = sys.argv[2].upper()

# Logging
logger = get_logger(__name__, f"tracking_{repo_name}.log")
setup_logging()

# Parameter validation
try:
    delta_type = DeltaType[delta_calc_method]
except KeyError:
    logger.info("Invalid delta type. Use 'SPARQL' or 'ITERATIVE'.")
    sys.exit(1)

logger.info("Starting with building an RDF-star dataset from individual snapshots...")
logger.info(f"Repository name: {repo_name}, Delta type: {delta_type}")


#######################################
# Functions
#######################################
def compute_snapshot_statistics(dataset_id, snapshot_ts_prev: datetime, snapshot_ts: datetime = None):
    # Setup connection to GraphDB for retrieving snapshot metrics
    sparql_engine = create_engine(repo_name)

    # Retrieve metrics from GraphDB via SPARQL query in the csv format
    logger.info(f"Repository name: {repo_name}: Querying snapshot metrics from GraphDB")

    query = get_snapshot_metrics_template(ts_current=snapshot_ts, ts_prev=snapshot_ts_prev)
    logger.info(query)
    sparql_engine.setQuery(query)
    response = sparql_engine.query().convert() 

    # Parse CSV using pandas
    csv_text = response.decode('utf-8')
    df_metrics = pd.read_csv(StringIO(csv_text))

    with Session(engine) as session:
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


def version_snapshot(versioning_service: StarVersService, version_timestamp: datetime, file):
    versioning_service.run_versioning(version_timestamp=version_timestamp)
    
    # Clean up
    logger.info(f"Deleting RDF file: {file}")
    os.remove(file)

#######################################
# Cleaning and preparation
#######################################
# Delete snapshot table where dataset_id corresponds to the repository_name
logger.info(f"Deleting snapshot metrics for {repo_name}")
with Session(engine) as session:
    delete_snapshot_metrics_by_dataset_id(repo_name, session)

# Recreate repository
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

wait_for_graphdb(f"{Settings().graph_db_url}/rest/repositories")
recreate_repository(repo_name)

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

# Delete all directory starting with a timestamp
for entry in os.listdir(evaluation_dir):
    full_path = os.path.join(evaluation_dir, entry)
    if os.path.isdir(full_path):
        logger.info(f"Deleting directory: {full_path}")
        shutil.rmtree(full_path)

# Unzip all zip files
zip_files = glob.glob(os.path.join(evaluation_dir, "*.zip"))
for zip_file in zip_files:
    logger.info(f"Unzipping archive (only *raw.nt): {zip_file}")
    try:
        with zipfile.ZipFile(zip_file, 'r') as zip_ref:
            for member in zip_ref.namelist():
                if fnmatch.fnmatch(member, "*raw.nt"):
                    logger.debug(f"Extracting {member}")
                    zip_ref.extract(member, evaluation_dir)
    except zipfile.BadZipFile:
        logger.error(f"Bad zip file: {zip_file}")


# Delete all zip archives
for zip_file in zip_files:
    logger.info(f"Deleting zip archive: {zip_file}")
    os.remove(zip_file)

# Extract all RDF file names and their timestamps, create a mapping, and sort by timestamp
files = glob.glob(os.path.join(evaluation_dir, f"{tracking_task.name}_*.raw.nt"))
logger.info(f"Number of files: {len(files)}")

timestamp_pattern = re.compile(r'(\d{8}-\d{6}_\d{3})')

def extract_timestamp(file_path):
    filename = os.path.basename(file_path)
    match = timestamp_pattern.search(filename)
    return match.group(1) if match else ''

# Build mapping: {timestamp_str: file_path}
file_timestamp_pairs = []
for file_path in files:
    timestamp_str = extract_timestamp(file_path)
    if timestamp_str:
        file_timestamp_pairs.append((timestamp_str, file_path))

# Sort by timestamp string to ensure chronological order
def convert_timestamp_str_to_iso(timestamp_str):
    # Pad milliseconds to microseconds (3 digits -> 6 digits)
    if '_' in timestamp_str:
        base, millis = timestamp_str.split('_')
        padded = f"{base}_{millis.ljust(6, '0')}"
    else:
        padded = timestamp_str  # fallback if somehow _ is missing
    dt = datetime.strptime(padded, "%Y%m%d-%H%M%S_%f")
    assert isinstance(dt, datetime), "Conversion to datetime failed"

    return dt
file_timestamp_pairs.sort(key=lambda x: convert_timestamp_str_to_iso(x[0]))

init_version_timestmap, first_file = file_timestamp_pairs[0]
logger.info(f"First file: {first_file}, Timestamp: {init_version_timestmap}")
init_version_timestmap_iso = convert_timestamp_str_to_iso(init_version_timestmap)
logger.info(f"Initial version timestamp: {init_version_timestmap_iso}")


#######################################
# Versioning and statistics compution
#######################################
# Init versioning service
versioning_service = StarVersService(tracking_task, repo_name)
versioning_service.local_file = True 

# Run initial versioning for the tracking task
versioning_service.run_initial_versioning(version_timestamp=init_version_timestmap_iso)

# Remove first RDF file after initial versioning
os.remove(first_file)

# Sort by timestamp (oldest first)
file_timestamp_pairs.sort(key=lambda x: convert_timestamp_str_to_iso(x[0]))

# Get id by repo name
with Session(engine) as session:
    dataset_id = get_id_by_repo_name(repo_name, session)
    dataset = session.get(Dataset, dataset_id)

# Compute metrics for initial snapshot
init_timestamp = file_timestamp_pairs[0][1]
compute_snapshot_statistics(dataset_id, init_timestamp, init_timestamp)

# Iterate over all files, starting from the second oldest
latest_timestamp = init_timestamp
for timestamp_str, file in file_timestamp_pairs[1:]:
    version_timestamp = convert_timestamp_str_to_iso(timestamp_str)

    # version snapshot
    version_snapshot(versioning_service, version_timestamp, file)

    # compute snapshot metrics
    compute_snapshot_statistics(dataset_id, latest_timestamp, version_timestamp)
    latest_timestamp = version_timestamp


logger.info("Retro versioning completed successfully.")


