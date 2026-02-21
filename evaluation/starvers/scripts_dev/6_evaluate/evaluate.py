import os
import sys
import time
import csv
import socket
import shlex
import logging
import subprocess
from pathlib import Path
from datetime import datetime, timedelta, timezone
from itertools import product
import tomli
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON, POST
from SPARQLWrapper.SPARQLExceptions import EndPointInternalError

##########################################################
# Logging
##########################################################
LOG_FILE = "/starvers_eval/output/logs/evaluate/evaluate.txt"
logging.basicConfig(
    handlers=[logging.FileHandler(filename=LOG_FILE, encoding='utf-8', mode='w+')],
    format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
    datefmt="%F %A %T",
    level=logging.INFO
)

##########################################################
# Paths & Config
##########################################################
CONFIG_PATH = "/starvers_eval/configs/eval_setup.toml"
RESULT_DIR = "/starvers_eval/output/result_sets"
TIME_FILE = "/starvers_eval/output/measurements/time.csv"
MEM_FILE = "/starvers_eval/output/measurements/memory_consumption.csv"

##########################################################
# Helpers
##########################################################
def eval_combi_exists(config, triplestore, dataset, policy):
    return policy in config.get("evaluations", {}).get(triplestore, {}).get(dataset, [])


def start_mem_tracker(pid, label, outfile, interval=1):
    def _track():
        with open(outfile, "a") as f:
            while True:
                try:
                    rss_kb = int(subprocess.check_output(
                        ["ps", "-o", "rss=", "-p", str(pid)]
                    ).decode().strip())
                    ts = datetime.now().isoformat()
                    f.write(f"{ts};{label};{pid};{rss_kb/1024/1024}\n")
                    f.flush()
                    time.sleep(interval)
                except Exception:
                    break

    import threading
    t = threading.Thread(target=_track, daemon=True)
    t.start()
    return t


def set_endpoints(config, triple_store, dataset, policy, engine):
    engine.endpoint = config["rdf_stores"][triple_store]["get"].format(repo=f"{policy}_{dataset}")
    engine.updateEndpoint = config["rdf_stores"][triple_store]["post"].format(repo=f"{policy}_{dataset}")


##########################################################
# QUERY EXECUTION (former query.py)
##########################################################
def run_queries(config, header, triple_store, policy, dataset):
    engine = SPARQLWrapper("dummy")
    engine.timeout = 30
    engine.setReturnFormat(JSON)
    engine.setMethod(POST)

    set_endpoints(config, triple_store, dataset, policy, engine)

    LOCAL_TIMEZONE = datetime.now(timezone.utc).astimezone().tzinfo
    init_ts = datetime(2022, 10, 1, 12, 0, 0, tzinfo=LOCAL_TIMEZONE)

    dataset_cfg = config['datasets'][dataset]
    query_sets = [
        f"{policy}/{dataset}/{qs}"
        for qs in dataset_cfg['query_sets'].keys()
    ]

    first_qs = next(iter(dataset_cfg['query_sets']))
    versions = dataset_cfg['query_sets'][first_qs]['policies'][policy]['versions']


    for query_set in query_sets:
        rows = []
        
        # Startup database
        mgmt_script = config["rdf_stores"][triple_store]["mgmt_script"]
        db_dir = f"/starvers_eval/databases/{triple_store}/{policy}_{dataset}"

        logging.info(f"Startup {triple_store} {policy} {dataset} for query set evaluation: {query_set}")
        subprocess.run([mgmt_script, "startup", db_dir, policy, dataset], check=True)

        # Wait for PID
        pid_file = f"/tmp/{triple_store}_{policy}_{dataset}.pid"
        for _ in range(3):
            time.sleep(3)
            if os.path.exists(pid_file):
                break

        if not os.path.exists(pid_file):
            raise RuntimeError("PID not found")

        with open(pid_file) as f:
            pid = int(f.read().strip())
        
        logging.info("Starting memory tracker")
        tracker = start_mem_tracker(pid, f"{policy}_{dataset}", MEM_FILE, 0.5)

        # Dry run
        logging.info("Starting dry run.")
        dry_query = config["rdf_stores"][triple_store]["dry_run_query"]
        engine.setQuery(dry_query)
        engine.query()

        logging.info("Running queries")
        for version in range(versions):
            base = f"/starvers_eval/queries/final_queries/{query_set}/{version}"
            snapshot_ts = init_ts + timedelta(seconds=version)

            for file_name in os.listdir(base):
                path = os.path.join(base, file_name)

                with open(path) as f:
                    query = f.read()

                engine.setQuery(query)

                exec_time = -1
                yn_timeout = 0

                try:
                    start = time.time()
                    response = engine.query()
                    exec_time = time.time() - start

                except (TimeoutError, socket.timeout) as e:
                    yn_timeout = 1
                    logging.error(e)

                except EndPointInternalError as e:
                    yn_timeout = 0
                    logging.error(e)

                    # After Jena crashes, just save what you have and continue
                    df = pd.DataFrame(rows, columns=header)
                    df.to_csv(TIME_FILE, sep=";", index=False, mode='a', header=False)
                    del df

                    continue

                except Exception as e:
                    yn_timeout = 0
                    logging.error(e)

                rows.append([
                    triple_store, dataset, policy,
                    query_set.split('/')[2],
                    version, snapshot_ts,
                    file_name, exec_time, 0, yn_timeout
                ])

        
        df = pd.DataFrame(rows, columns=header)
        df.to_csv(TIME_FILE, sep=";", index=False, mode='a', header=False)
        del df

        logging.info("Shutdown")
        subprocess.run([mgmt_script, "shutdown"], check=True)


##########################################################
# MAIN PIPELINE (former evaluation.sh)
##########################################################
def main():
    with open(CONFIG_PATH, "rb") as f:
        config = tomli.load(f)
    
    triple_stores = sys.argv[1].split(" ")
    policies = sys.argv[2].split(" ")
    datasets = sys.argv[3].split(" ")

    header = [
        'triplestore', 'dataset', 'policy', 'query_set',
        'snapshot', 'snapshot_ts', 'query',
        'execution_time', 'snapshot_creation_time', 'yn_timeout'
    ]

    # Write header once
    pd.DataFrame(columns=header).to_csv(
        TIME_FILE, sep=";", index=False, mode='w', header=True
    )
    combinations = product(triple_stores, policies, datasets)

    for triple_store, policy, dataset in combinations:

        if not eval_combi_exists(config, triple_store, dataset, policy):
            logging.info(f"The combination {triple_store}, {dataset}, and {policy} is not supported and will be skipped") 
            continue

        # Run evaluation
        run_queries(config, header, triple_store, policy, dataset)

if __name__ == "__main__":
    main()

