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
from SPARQLWrapper import Wrapper, SPARQLWrapper, JSON, POST
from SPARQLWrapper.SPARQLExceptions import EndPointInternalError
from itertools import product, takewhile
from functools import partial
from urllib.error import HTTPError
from enum import Enum
from starvers.starvers import TripleStoreEngine

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
CONFIG_TMPL_DIR="/starvers_eval/scripts/3_construct_datasets/configs"
CONFIG_DIR="/starvers_eval/configs/construct_datasets"
CONFIG_PATH = "/starvers_eval/configs/eval_setup.toml"
RESULT_DIR = "/starvers_eval/output/result_sets"
TIME_FILE = "/starvers_eval/output/measurements/time.csv"
MEM_FILE = "/starvers_eval/output/measurements/memory_consumption.csv"
databases_dir = "/starvers_eval/databases"


# For update evaluation
in_frm = "nt"
LOCAL_TIMEZONE = datetime.now(timezone.utc).astimezone().tzinfo
init_version_timestamp = datetime(2022,10,1,12,0,0,0,LOCAL_TIMEZONE)

with open(CONFIG_PATH, "rb") as f:
    config = tomli.load(f)

dataset_versions = {dataset: infos['snapshot_versions'] for dataset, infos in config['datasets'].items()}
ic_basename_lengths = {dataset: infos['ic_basename_length'] for dataset, infos in config['datasets'].items()}
snapshot_dir = config['general']['snapshot_dir']
change_sets_dir = config['general']['change_sets_dir']
##########################################################
# Helpers
##########################################################
class TripleStore(Enum):
    GRAPHDB = 1
    JENATDB2 = 2
    OSTRICH = 3

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


def config_engine(config, triple_store, dataset, policy):
    engine = SPARQLWrapper("dummy")
    engine.timeout = 30
    engine.setReturnFormat(JSON)
    engine.setMethod(POST)
    engine.addCustomHttpHeader("Connection", "close")
    engine.addCustomHttpHeader("Accept", "application/sparql-results+json")

    engine.endpoint = config["rdf_stores"][triple_store]["get"].format(repo=f"{policy}_{dataset}")
    engine.updateEndpoint = config["rdf_stores"][triple_store]["post"].format(repo=f"{policy}_{dataset}")

    return engine


def parse_results(result) -> list:
    """

    :param result:
    :return: Dataframe
    """

    if result is None:
        return [["None"]]

    results = result

    def format_value(res_value):
        value = res_value["value"]
        lang = res_value.get("xml:lang", None)
        datatype = res_value.get("datatype", None)
        if lang is not None:
            value += "@" + lang
        if datatype is not None:
            value += " [" + datatype + "]"
        return value

    header = []
    values = []

    if not "head" in results or not "vars" in results["head"]:
        return header

    if not "results" in results or not "bindings" in results["results"]:
        return values

    for var in results["head"]["vars"]:
        header.append(var)

    for r in results["results"]["bindings"]:
        row = []
        for col in results["head"]["vars"]:
            if col in r:
                result_value = format_value(r[col])
            else:
                result_value = None
            row.append(result_value)
        values.append(row)

    return [header] + values


##########################################################
# Evaluation functions
##########################################################
def run_queries(config, header, triple_store, policy, dataset):
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
        db_dir = f"{databases_dir}/{triple_store}/{policy}_{dataset}"

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
        engine = config_engine(config, triple_store, dataset, policy)        
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
                
                engine = config_engine(config, triple_store, dataset, policy)
                engine.setQuery(query)

                exec_time = -1
                yn_timeout = 0

                try:
                    start = time.time()
                    response = engine.query().convert()
                    exec_time = time.time() - start

                except (TimeoutError, socket.timeout) as e:
                    yn_timeout = 1
                    response = None
                    logging.error(e)

                except EndPointInternalError as e:
                    yn_timeout = 0
                    response = None
                    logging.error(f"The triple store crashed. Restarting triple store ... ")
                    logging.error(e)

                    logging.info("Shutdown")
                    subprocess.run([mgmt_script, "shutdown"], check=True)

                    logging.info(f"Startup {triple_store} {policy} {dataset} for query set evaluation: {query_set}")
                    subprocess.run([mgmt_script, "startup", db_dir, policy, dataset], check=True)
                except Exception as e:
                    yn_timeout = 0
                    response = None
                    logging.error(e)

                rows.append([
                    triple_store, dataset, policy,
                    query_set.split('/')[2],
                    version, snapshot_ts,
                    file_name, exec_time, 0, yn_timeout
                ])

                # Serialize
                result_set_dir = RESULT_DIR + "/" + triple_store + "/" + policy + "_" + dataset + "/" + query_set.split('/')[2] + "/" + str(version)
                Path(result_set_dir).mkdir(parents=True, exist_ok=True)
                with open(result_set_dir + "/" + file_name.split('.')[0] + ".csv", 'w') as file:
                    write = csv.writer(file, delimiter=";")
                    write.writerows(parse_results(response))

        
        logging.info(f"Writing results to {TIME_FILE}")
        df = pd.DataFrame(rows, columns=header)
        df.to_csv(TIME_FILE, sep=";", index=False, mode='a', header=False)
        del df

        logging.info("Shutdown")
        subprocess.run([mgmt_script, "shutdown"], check=True)


def measure_updates(dataset:str, source_ic0: str, source_cs: str, last_version: int, init_timestamp: datetime):
    # HTTPError
    triple_stores = [TripleStore.GRAPHDB]
    chunk_sizes = range(1000, 10000, 1000)
    measure_ts_with_varying_chunk_sizes = partial(insert_ic0_and_cbs, 
                                                dataset=dataset,
                                                source_ic0=source_ic0, 
                                                source_cs=source_cs, 
                                                last_version=last_version, 
                                                init_timestamp=init_timestamp)

    measurements: list[pd.DataFrame] = []
    for ts, chunk_size in product(triple_stores, chunk_sizes):
        result = measure_ts_with_varying_chunk_sizes(ts, chunk_size)
        if result is False:
            # Stop iteration if HTTPError occurred
            break
        measurements.append(result)

    combined_measurements = pd.concat(measurements, join="inner")
    
    logging.info("Writing performance measurements to disk ...")            
    combined_measurements.to_csv(f"/starvers_eval/output/measurements/time_update_{dataset}.csv", sep=";", index=False, mode='w', header=True)

    # Remove temporary output files
    dir_path = "/starvers_eval/output/measurements/"
    files_to_remove = [os.path.join(dir_path, f) for f in os.listdir(dir_path) if f.startswith(f"time_update_{dataset}_") and f.endswith(".csv")]
    for file in files_to_remove:
        os.remove(file)


def insert_ic0_and_cbs(triple_store: TripleStore, chunk_size: int, dataset: str,
                        source_ic0: str, source_cs: str, last_version: int, init_timestamp: datetime):
    triple_store_name = triple_store.name.lower()
    logging.info(f"Constructing timestamped RDF-star dataset from ICs and changesets triple store {triple_store} and chunk size {chunk_size}.")
    policy = "tb_rs_sr"

    repository = policy + "_" + dataset
    database_dir = f"{databases_dir}/{triple_store_name}"
    mgmt_script = eval_setup["rdf_stores"][triple_store_name]["mgmt_script"]

    logging.info("Create GraphDB directories and environment")
    logging.info(f"\nDatabase directory {database_dir}\nConfig dirctory:{CONFIG_DIR}\nConfig Template directory:{CONFIG_TMPL_DIR}")
    subprocess.call(shlex.split(f"{mgmt_script} --log-file {LOG_FILE} create_env {policy} {dataset} {database_dir} {CONFIG_TMPL_DIR} {CONFIG_DIR}"))

    logging.info(f"Ingest empty file into {repository} repository and start {triple_store_name}.")
    subprocess.call(shlex.split(f"{mgmt_script} --log-file {LOG_FILE} ingest_empty {database_dir} {policy} {dataset} {CONFIG_DIR}"))

    logging.info("Startup GraphDB engine")
    subprocess.call(shlex.split(f"{mgmt_script} --log-file {LOG_FILE} startup {database_dir} {policy} {dataset}"))

    logging.info("Read initial snapshot {0} into memory.".format(source_ic0))
    added_triples_raw = open(source_ic0, "r").read().splitlines()
    added_triples_raw = list(filter(None, added_triples_raw))
    added_triples_raw = list(filter(lambda x: not x.startswith("# "), added_triples_raw))

    logging.info("Add triples from initial snapshot {0} as nested triples into the RDF-star dataset.".format(source_ic0))
    
    query_endpoint = eval_setup["rdf_stores"][triple_store_name]["get"].format(repo=f"{policy}_{dataset}")
    update_endpoint = eval_setup["rdf_stores"][triple_store_name]["post"].format(repo=f"{policy}_{dataset}")
    rdf_star_engine = TripleStoreEngine(query_endpoint, update_endpoint)
    try:
        start = time.time()
        rdf_star_engine.insert(triples=added_triples_raw, timestamp=init_timestamp, chunk_size=chunk_size)
        end = time.time()
    except HTTPError:
        logging.info("Too many triples transfered over HTTP. No measures for this chunk size setting will be recorded")
        return False
    execution_time_insert = end - start
    
    df = pd.DataFrame(columns=['triplestore', 'dataset', 'batch', 'cnt_batch_trpls', 'chunk_size', 'execution_time'],
                        data=[[triple_store.name, dataset, 'snapshot_0', len(added_triples_raw), chunk_size, execution_time_insert]])

    # Map versions to files in chronological orders
    change_sets = {}
    for filename in sorted(os.listdir(source_cs)):
        if not (filename.startswith("data-added") or filename.startswith("data-deleted")):
            continue 
        version = int(filename.split('-')[2].split('.')[0].zfill(len(str(last_version)))) - 1
        change_sets[filename] = version

    # Apply changesets to RDF-star dataset
    for filename, version in sorted(change_sets.items(), key=lambda item: item[1]):
        vers_ts = init_timestamp + timedelta(seconds=version)

        mem_in_usage = psutil.virtual_memory().percent
        logging.info(f"Memory in usage: {mem_in_usage}%")
        if mem_in_usage > 85:
            # Reboot to free up main memory
            subprocess.call(shlex.split(f"{mgmt_script} --log-file {LOG_FILE} shutdown"))
            subprocess.call(shlex.split(f"{mgmt_script} --log-file {LOG_FILE} startup {database_dir} {policy} {dataset}"))
        
        if filename.startswith("data-added"):
            logging.info("Read positive changeset {0} into memory.".format(filename))
            added_triples_raw = open(source_cs + "/" + filename, "r").read().splitlines()
            added_triples_raw = list(filter(None, added_triples_raw))
            cnt_trpls = len(added_triples_raw)

            logging.info(f"Add {cnt_trpls} triples from changeset {filename} as nested triples into the RDF-star dataset.")
            start = time.time()
            rdf_star_engine.insert(triples=added_triples_raw, timestamp=vers_ts, chunk_size=chunk_size)
            end = time.time()
            execution_time_insert = end - start
            new_row = pd.DataFrame([[triple_store.name, dataset, 'positive_change_set_' + str(version), len(added_triples_raw), chunk_size, execution_time_insert]], columns=df.columns)
            df = pd.concat([df, new_row], ignore_index=True)

        if filename.startswith("data-deleted"):
            logging.info("Read negative changeset {0} into memory.".format(filename))
            deleted_triples_raw = open(source_cs + "/" + filename, "r").read().splitlines()
            deleted_triples_raw = list(filter(None, deleted_triples_raw))
            cnt_trpls = len(deleted_triples_raw)

            logging.info(f"Oudate {cnt_trpls} triples in the RDF-star dataset which match the triples in {filename}.")                
            start = time.time()
            rdf_star_engine.outdate(triples=deleted_triples_raw, timestamp=vers_ts, chunk_size=chunk_size)
            end = time.time()
            execution_time_outdate = end - start
            new_row = pd.DataFrame([[triple_store.name, dataset, 'negative_change_set_' + str(version), len(deleted_triples_raw), chunk_size, execution_time_outdate]], columns=df.columns)
            df = pd.concat([df, new_row], ignore_index=True)
    
        df.to_csv(f"/starvers_eval/output/measurements/time_update_{dataset}_{str(chunk_size)}.csv", sep=";", index=False, mode='w', header=True)

    # Shutdown engine
    subprocess.call(shlex.split(f"{mgmt_script} --log-file {LOG_FILE} shutdown"))
    subprocess.call(shlex.split(f"{mgmt_script} --log-file {LOG_FILE} shutdown"))

    return df


##########################################################
# MAIN PIPELINE (former evaluation.sh)
##########################################################
def main():
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

    # Query evaluation
    for triple_store, policy, dataset in combinations:

        if not eval_combi_exists(config, triple_store, dataset, policy):
            logging.info(f"The combination {triple_store}, {dataset}, and {policy} is not supported and will be skipped") 
            continue

        run_queries(config, header, triple_store, policy, dataset)

    # Update evaluation
    for dataset in datasets:
        data_dir = f"/starvers_eval/rawdata/{dataset}"
        total_versions = dataset_versions[dataset]

        #measure_updates(dataset=dataset, 
        #        source_ic0=f"{data_dir}/{snapshot_dir}/" + "1".zfill(ic_basename_lengths[dataset])  + ".nt",
        #        source_cs=f"{data_dir}/{change_sets_dir}.{in_frm}", 
        #        last_version=total_versions, 
        #        init_timestamp=init_version_timestamp)

if __name__ == "__main__":
    main()

