from datetime import datetime
from datetime import timezone
import os
import sys, subprocess
import logging
import tomli
import csv
import re


def parse_ingestion_log_to_csv(log_file: str, output_path: str):
    header = ["version", "added", "durationms", "rate", "accsize"]
    data_re = re.compile(r'^\s*(\d+),(\d+),(\d+),(\d+),(\d+)\s*$')
    rows_by_version = {}

    with open(log_file, "r") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.lower().startswith("version,added,durationms,rate,accsize"):
                continue
            m = data_re.match(line)
            if not m:
                continue
            v, a, d, r, s = (int(m.group(i)) for i in range (1, 6))
            rows_by_version[v] = (v, a, d, r, s)

    with open(output_path, "w", newline="") as fout:
        w = csv.writer(fout, delimiter=";", lineterminator="\n")
        w.writerow(header)
        for row in sorted(rows_by_version.values(), key=lambda t: t[0]):
            w.writerow(row)

def sh(cmd, **kwargs):
    print("+", " ".join(map(str, cmd)), flush=True)
    subprocess.run(cmd, check=True, **kwargs)
    return None

def ostrich_ingestion(end_idx: int, log_path: str, store_dir, dataset_dir, empty_query_path):
    cmd = [
            "bash", "-lc",
            (
                f"ulimit -n 1048576 && cd {store_dir} && "
                f"/ostrich_eval/ostrich/build/ostrich-evaluate ingest never 0 {dataset_dir} 1 {end_idx} "
                f"| tee -a {log_path}"
            )
    ]
    sh(cmd)

def ingest(dataset: str, total_versions: int):
    logging.info(f"Starting ingestion...")

    store_dir = f"/ostrich_eval/stores/{dataset}"
    if os.path.exists(store_dir):
        logging.info(f"Aborting ingestion. Store already built!")
        return

    dataset_dir = f"/ostrich_eval/datasets/{dataset}"
    ingestion_measurements_dir = f"/ostrich_eval/output/measurements/{dataset}"

    if not os.path.exists(store_dir):
        logging.info(f"Create directory: {store_dir}")
        os.makedirs(store_dir)
    
    empty_query_path = f"/ostrich_eval/empty_query.txt"
    with open(empty_query_path, "w") as empty_query_file:
        empty_query_file.write("")

    if not os.path.exists(ingestion_measurements_dir):
        logging.info(f"Create directory: {ingestion_measurements_dir}")
        os.makedirs(ingestion_measurements_dir)

    log_path = f"{ingestion_measurements_dir}/ingestions_log.txt"
    with open(log_path, "w") as log_file:
        log_file.write("")

    ostrich_ingestion(total_versions, log_path, store_dir, dataset_dir, empty_query_path)

    log_path_csv = f"{ingestion_measurements_dir}/ingestion.csv"
    parse_ingestion_log_to_csv(log_path, log_path_csv)



############################################# Logging #############################################
if not os.path.exists('/ostrich_eval/output/logs/ingest'):
    os.makedirs('/ostrich_eval/output/logs/ingest')
with open('/ostrich_eval/output/logs/ingest/ingest.txt', "w") as log_file:
    log_file.write("")
logging.basicConfig(handlers=[logging.FileHandler(filename="/ostrich_eval/output/logs/ingest/ingest.txt", 
                                                  encoding='utf-8', mode='a+')],
                    format="%(asctime)s %(name)s:%(levelname)s:%(message)s", 
                    datefmt="%F %A %T", 
                    level=logging.INFO)

############################################# Parameters #############################################
datasets = sys.argv[1].split(" ")

in_frm = "nt"
LOCAL_TIMEZONE = datetime.now(timezone.utc).astimezone().tzinfo
with open("/ostrich_eval/configs", mode="rb") as config_file:
    eval_setup = tomli.load(config_file)
dataset_versions = {dataset: infos['snapshot_versions'] for dataset, infos in eval_setup['datasets'].items()}
allowed_datasets = list(dataset_versions.keys())

############################################# Start procedure #############################################
for dataset in datasets:
    if dataset not in allowed_datasets:
        print("Dataset must be one of: ", allowed_datasets, "but is: {0}".format(dataset))
        break

    total_versions = dataset_versions[dataset]
    print("Ingesting dataset for {0}".format(dataset))
    ingest(dataset, total_versions)


 
