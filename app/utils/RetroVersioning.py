from datetime import datetime
import os
import glob
import sys
import re
import logging

from app.services.VersioningService import StarVersService
from app.models.TrackingTaskModel import TrackingTaskDto
from app.utils.graphdb.GraphDatabaseUtils import create_repository
from app.enums.DeltaTypeEnum import DeltaType

# Logging
repo_name = sys.argv[1]
if not os.path.exists('/code/logs'):
    os.makedirs('/code/logs')
with open(f'/code/logs/build_rdf_star_dataset_{repo_name}.log', "w") as log_file:
    log_file.write("")
logging.basicConfig(handlers=[logging.FileHandler(filename=f"/code/logs/build_rdf_star_dataset_{repo_name}.log", 
                                                  encoding='utf-8', mode='a+')],
                    format="%(asctime)s %(name)s:%(levelname)s:%(message)s", 
                    datefmt="%F %A %T", 
                    level=logging.INFO)
try:
    delta_type = DeltaType[sys.argv[2].upper()]
except KeyError:
    logging.info("Invalid delta type. Use 'SPARQL' or 'ITERATIVE'.")
    sys.exit(1)

logging.info("Starting with building an RDF-star dataset from individual snapshots...")
logging.info(f"Repository name: {repo_name}, Delta type: {delta_type}")

# Define tracking task with input parameters
tracking_task = TrackingTaskDto(
    id="123",
    name=repo_name,
    rdf_dataset_url="",
    delta_type=delta_type
)

# RDF datasets directory
evaluation_dir = f"./evaluation/{tracking_task.name}"
logging.info(f"Evaluation directory: {evaluation_dir}")

# Get initial versioning timestamp from evaluation directory
def get_initial_version_timestamp(evaluation_dir, tracking_task_name):
    rdf_files = glob.glob(os.path.join(evaluation_dir, f"{tracking_task_name}_*.raw.nt"))
    if not rdf_files:
        return None

    # Regex to extract timestamp of form: YYYYMMDD-HHMMSS_mmm
    timestamp_pattern = re.compile(r'(\d{8}-\d{6}_\d{3})')

    def extract_timestamp(file_path):
        filename = os.path.basename(file_path)
        match = timestamp_pattern.search(filename)
        return match.group(1) if match else ''

    # Sort rdf files based on extracted timestamp string
    rdf_files.sort(key=lambda x: extract_timestamp(x))

    # Extract timestamp from the first file after sorting
    first_file = rdf_files[0]
    return extract_timestamp(first_file)

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

init_version_timestmap = get_initial_version_timestamp(evaluation_dir, tracking_task.name)
init_version_timestmap_iso = convert_timestamp_str_to_iso(init_version_timestmap)
logging.info(f"Initial version timestamp: {init_version_timestmap_iso}")

# Create repository
create_repository(tracking_task.name)

# Init versioning service
versioning_service = StarVersService(tracking_task)
versioning_service.local_file = True 

# Run initial versioning for the tracking task
versioning_service.run_initial_versioning(version_timestamp=init_version_timestmap_iso)

# Extract all RDF file names and their timestamps, create a mapping, and sort by timestamp
rdf_files = glob.glob(os.path.join(evaluation_dir, f"{tracking_task.name}_*.raw.nt"))
timestamp_pattern = re.compile(r'(\d{8}-\d{6}_\d{3})')

def extract_timestamp(file_path):
    filename = os.path.basename(file_path)
    match = timestamp_pattern.search(filename)
    return match.group(1) if match else ''

# Build mapping: {timestamp_str: file_path}
file_timestamp_pairs = []
for file_path in rdf_files:
    timestamp_str = extract_timestamp(file_path)
    if timestamp_str:
        file_timestamp_pairs.append((timestamp_str, file_path))

# Sort by timestamp (oldest first)
file_timestamp_pairs.sort(key=lambda x: convert_timestamp_str_to_iso(x[0]))

# Iterate over all files, starting from the oldest
for timestamp_str, rdf_file in file_timestamp_pairs[1:]:
    version_timestamp = convert_timestamp_str_to_iso(timestamp_str)
    versioning_service.run_versioning(version_timestamp=version_timestamp)
    logging.info(f"Deleting RDF file: {rdf_file}")
    os.remove(rdf_file)

logging.info("Retro versioning completed successfully.")