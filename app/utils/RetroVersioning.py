from datetime import datetime
import os
import glob
import sys
import re
from uuid import uuid4
from app.services.VersioningService import StarVersService
from app.models.TrackingTaskModel import TrackingTaskDto
from app.utils.graphdb.GraphDatabaseUtils import create_repository
from app.enums.DeltaTypeEnum import DeltaType
from app.LoggingConfig import get_logger

# Logging
repo_name = sys.argv[1]
logger = get_logger(__name__, f"tracking_{repo_name}.log")

try:
    delta_type = DeltaType[sys.argv[2].upper()]
except KeyError:
    logger.info("Invalid delta type. Use 'SPARQL' or 'ITERATIVE'.")
    sys.exit(1)

logger.info("Starting with building an RDF-star dataset from individual snapshots...")
logger.info(f"Repository name: {repo_name}, Delta type: {delta_type}")

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

# Extract all RDF file names and their timestamps, create a mapping, and sort by timestamp
files = glob.glob(os.path.join(evaluation_dir, f"{tracking_task.name}_*.raw.nt"))

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
file_timestamp_pairs.sort(key=lambda x: convert_timestamp_str_to_iso(x[0]))

init_version_timestmap, first_file = file_timestamp_pairs[0]
logger.info(f"First file: {first_file}, Timestamp: {init_version_timestmap}")
init_version_timestmap_iso = convert_timestamp_str_to_iso(init_version_timestmap)
logger.info(f"Initial version timestamp: {init_version_timestmap_iso}")

# Create repository
create_repository(tracking_task.name)

# Init versioning service
versioning_service = StarVersService(tracking_task)
versioning_service.local_file = True 

# Run initial versioning for the tracking task
versioning_service.run_initial_versioning(version_timestamp=init_version_timestmap_iso)

# Remove first RDF file after initial versioning
os.remove(first_file)

# Sort by timestamp (oldest first)
file_timestamp_pairs.sort(key=lambda x: convert_timestamp_str_to_iso(x[0]))

# Iterate over all files, starting from the oldest
for timestamp_str, file in file_timestamp_pairs[1:]:
    version_timestamp = convert_timestamp_str_to_iso(timestamp_str)
    rdf_file = file
    versioning_service.run_versioning(version_timestamp=version_timestamp)
    
    logger.info(f"Deleting RDF file: {rdf_file}")
    os.remove(rdf_file)

logger.info("Retro versioning completed successfully.")