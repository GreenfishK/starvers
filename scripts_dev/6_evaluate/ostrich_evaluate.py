from datetime import datetime
from datetime import timezone
import os
import sys, subprocess
import logging
import tomli

from ostrich_parse_eval_output import parse_eval_stdout_to_csv

def sh(cmd, capture=False, **kwargs):
    print("+", " ".join(map(str, cmd)), flush=True)
    if capture:
        result = subprocess.run(
            cmd,
            check=True,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            **kwargs
        )
        return result.stdout.strip()
    else:
        subprocess.run(cmd, check=True, **kwargs)
        return None

def ostrich_evaluation(end_idx: int, log_path: str, store_dir, dataset_dir, query_type):
    query_file = f"/ostrich_eval/computed_queries/basic/lookup_queries_{query_type}.txt"
    log_dir = f"{log_path}/basic"
    if not os.path.exists(log_dir):
        logging.info("Create directory: " + log_dir)
        os.makedirs(log_dir)

    log_file = f"{log_dir}/log.txt"

    cmd = [
            "bash", "-lc",
            (
                f"ulimit -n 1048576 && cd {store_dir} && "
                f"/ostrich_eval/ostrich/build/ostrich-evaluate query {query_file} 5 "
                f"| tee -a {log_file}"
            )
    ]
    sh(cmd)

    vm_queries_path = f"{log_dir}/lookup_queries_{query_type}_vm.csv"
    dm_queries_path = f"{log_dir}/lookup_queries_{query_type}_dm.csv"
    vq_queries_path = f"{log_dir}/lookup_queries_{query_type}_vq.csv"

    parse_eval_stdout_to_csv(log_file, vm_queries_path, dm_queries_path, vq_queries_path)


def evaluate_basic(dataset: str, total_versions: int):
    logging.info(f"Starting evaluation...")

    store_dir = f"/ostrich_eval/stores/{dataset}"
    dataset_dir = f"/ostrich_eval/datasets/{dataset}"
    measurements_dir = f"/ostrich_eval/output/measurements/{dataset}"
         
    if not os.path.exists(store_dir):
            logging.info(f"No store found for {dataset}")
            return

    db_size_cmd = sh(["du", "-sh", store_dir], capture=True)
    db_size_only = db_size_cmd.split()[0]
    with open(f"{measurements_dir}/db_file_size.txt", "w") as f:
        f.write(db_size_only + "\n")

    raw_size_cmd = sh(["du", "-sh", dataset_dir], capture=True)
    raw_size_only = raw_size_cmd.split()[0]
    with open(f"{measurements_dir}/raw_file_size.txt", "w") as f:
        f.write(raw_size_only + "\n")

    if dataset == "bearb_hour" or dataset == "bearb_day":
        pass
        ostrich_evaluation(total_versions, measurements_dir, store_dir, dataset_dir, "p")
        ostrich_evaluation(total_versions, measurements_dir, store_dir, dataset_dir, "po")
    else:
        logging.info("Basic evaluation only possible for bearb_hour and bearb_day!")



############################################# Logging #############################################
if not os.path.exists('/ostrich_eval/output/logs/evaluate'):
    os.makedirs('/ostrich_eval/output/logs/evaluate')
with open('/ostrich_eval/output/logs/evaluate/evaluate.txt', "w") as log_file:
    log_file.write("")
logging.basicConfig(handlers=[logging.FileHandler(filename="/ostrich_eval/output/logs/evaluate/evaluate.txt", 
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
    print("Evaluating dataset with basic queries for {0}".format(dataset))
    evaluate_basic(dataset, total_versions)


 
