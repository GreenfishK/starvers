import os
import sys
import time
import csv
import socket
import logging
import subprocess
from pathlib import Path
from datetime import datetime, timedelta, timezone
from itertools import product
import tomli
import pandas as pd
from SPARQLWrapper import SPARQLWrapper, JSON, POST
from SPARQLWrapper.SPARQLExceptions import EndPointInternalError, QueryBadFormed
from enum import Enum
import psutil
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
import socket

from scripts.logging import setup_logging

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
BASE_LOG_DIR, LOG = setup_logging("evaluate_queries")

# ---------------------------------------------------------------------------
# Static eval parameters
# ---------------------------------------------------------------------------
CONFIG_PATH = Path("/starvers_eval/configs/eval_setup.toml")
def _load_config() -> dict:
    with open(CONFIG_PATH, "rb") as f:
        return tomli.load(f)
static_eval_params = _load_config()

# ---------------------------------------------------------------------------
# Environment / path constants
# ---------------------------------------------------------------------------
CONFIG_TMPL_DIR = "/starvers_eval/scripts/4_ingest/configs"
CONFIG_DIR: str = f"{os.environ['RUN_DIR']}/configs/ingest"
RESULT_DIR = f"{os.environ['RUN_DIR']}/output/result_sets"
TIME_FILE = f"{os.environ['RUN_DIR']}/output/measurements/queries_time.csv"
MEM_FILE = f"{os.environ['RUN_DIR']}/output/measurements/memory_consumption.csv"
databases_dir = f"{os.environ['RUN_DIR']}/databases"

triple_stores = os.environ.get("triple_stores").split(" ")
policies = os.environ.get("policies").split(" ")
datasets = os.environ.get("datasets").split(" ")

# ---------------------------------------------------------------------------
# Classes and functions
# ---------------------------------------------------------------------------
class TripleStore(Enum):
    GRAPHDB = 1
    JENATDB2 = 2
    OSTRICH = 3
    OSTRICH_AGGCHANGE = 4

def eval_combi_exists(triplestore, dataset, policy):
    return policy in static_eval_params.get("evaluations", {}).get(triplestore, {}).get(dataset, [])


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


def config_engine(triple_store, dataset, policy):
    engine = SPARQLWrapper("dummy")
    engine.timeout = 0
    engine.setReturnFormat(JSON)
    engine.setMethod(POST)
    engine.addCustomHttpHeader("Connection", "close")
    engine.addCustomHttpHeader("Accept", "application/sparql-results+json")

    engine.endpoint = static_eval_params["rdf_stores"][triple_store]["get"].format(repo=f"{policy}_{dataset}")
    engine.updateEndpoint = static_eval_params["rdf_stores"][triple_store]["post"].format(repo=f"{policy}_{dataset}")

    return engine


def parse_results(result) -> list:
    """
    :param result:
    :return: list of rows (header + values)
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

def print_mem_file_tail(mem_file, lines=20):
    with open(mem_file, "r") as f:
        all_lines = f.readlines()
        LOG.info(f"--------------- Memory tail start --------------- ")
        LOG.info(f"{all_lines[0]}")
        for line in all_lines[-lines:]:
            LOG.info({line})
        LOG.info(f"--------------- Memory tail end --------------- ")


def run_queries(triple_store, policy, dataset) -> list:
    LOCAL_TIMEZONE = datetime.now(timezone.utc).astimezone().tzinfo
    init_ts = datetime(2022, 10, 1, 12, 0, 0, tzinfo=LOCAL_TIMEZONE)

    dataset_cfg = static_eval_params['datasets'][dataset]
    query_sets = [
        f"{policy}/{dataset}/{qs}"
        for qs in dataset_cfg['query_sets'].keys()
    ]

    first_qs = next(iter(dataset_cfg['query_sets']))
    versions = dataset_cfg['query_sets'][first_qs]['policies'][policy]['versions']

    all_rows = []

    for query_set in query_sets:
        LOG.info(f"Evaluating {triple_store} {policy} {dataset} for query set: {query_set}")
        rows = []

        # Startup database
        mgmt_script = static_eval_params["rdf_stores"][triple_store]["mgmt_script"]
        db_dir = f"{databases_dir}/{triple_store}/{policy}_{dataset}"

        LOG.info(f"Startup {triple_store} for {policy}, {dataset}")
        subprocess.run([mgmt_script, "startup", db_dir, policy, dataset, CONFIG_DIR], check=True)

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

        LOG.info("Starting memory tracker")
        tracker = start_mem_tracker(pid, f"{policy}_{dataset}", MEM_FILE, 0.5)

        # Dry run
        LOG.info("Starting dry run.")
        engine = config_engine(triple_store, dataset, policy)

        dry_query = static_eval_params["rdf_stores"][triple_store]["dry_run_query"]
        engine.setQuery(dry_query)
        LOG.info(f"Dry run query:\n{dry_query}")

        # Try 5 times to mitigate Database Index buildup
        for try_counter in range(5):
            try:
                result = engine.query().convert()
                break
            except Exception as e:
                logging.error(f"Dry run failed with error: {e}")
                LOG.info("Retrying dry run after waiting for 5 seconds...")
                time.sleep(5)
        LOG.info("Dry run query result:\n " + str(result))

        LOG.info("Running queries")
        socket.setdefaulttimeout(30)
        for version in range(versions):
            base = f"{os.environ['RUN_DIR']}/queries/final_queries/{query_set}/{version}"
            snapshot_ts = init_ts + timedelta(seconds=version)

            for file_name in os.listdir(base):
                path = os.path.join(base, file_name)

                with open(path) as f:
                    query = f.read()

                engine = config_engine(triple_store, dataset, policy)
                engine.setQuery(query)

                exec_time = -1
                yn_timeout = 0

                executor = ThreadPoolExecutor(max_workers=1)

                result = {}
                def run(eng=engine):
                    result['start'] = time.time()
                    result['response'] = eng.query().convert()
                    result['end'] = time.time()

                try:
                    future = executor.submit(run)
                    future.result(timeout=30)
                    response = result['response']
                    exec_time = result['end'] - result['start']

                except FuturesTimeout as e:
                    yn_timeout = 1
                    response = None
                    logging.error(f"Timeout error for version {version} and query {file_name}: {e}")
                    # Check whats written in MEM_FILE
                    print_mem_file_tail(MEM_FILE, lines=20)

                except EndPointInternalError as e:
                    yn_timeout = 0
                    response = None
                    logging.error(f"The triple store crashed for version {version} and query {file_name}. \n {e}")

                except ConnectionResetError as e:
                    yn_timeout = 0
                    response = None
                    logging.error(f"Connection reset, probably due to memory overflow: {e}")
                    # Check whats written in MEM_FILE
                    print_mem_file_tail(MEM_FILE, lines=20)

                except QueryBadFormed as e:
                    yn_timeout = 0
                    response = "badly formed query"
                    logging.error(f"Query badly formed for version {version} and query {file_name}: {e}")

                except Exception as e:
                    yn_timeout = 0
                    response = None
                    logging.error(f"Other error for version {version} and query {file_name}: {e}")
                    LOG.info(f"Error instance type: {type(e)}")

                finally:
                    executor.shutdown(wait=False, cancel_futures=True)

                    if (response is None and yn_timeout == 0) \
                        or psutil.virtual_memory().percent >= 85:

                        LOG.info("Restart database. Virtual memory in usage: " + str(psutil.virtual_memory().percent) + "%")

                        LOG.info("Shutdown")
                        subprocess.run([mgmt_script, "shutdown"], check=True)

                        LOG.info(f"Startup {triple_store} {policy} {dataset} for query set evaluation: {query_set}")
                        subprocess.run([mgmt_script, "startup", db_dir, policy, dataset, CONFIG_DIR], check=True)

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

        LOG.info(f"Add all measurements for {triple_store}, {policy}, {dataset}, {query_set} to a list.")
        all_rows.extend(rows)

        LOG.info("Shutdown")
        subprocess.run([mgmt_script, "shutdown"], check=True)

        LOG.info("Stopping memory tracker")
        tracker.join(timeout=1)

    return all_rows


def load_or_init_time_df(header):
    if os.path.exists(TIME_FILE) and os.path.getsize(TIME_FILE) > 0:
        LOG.info(f"Loading existing measurement file {TIME_FILE}")
        df = pd.read_csv(TIME_FILE, sep=";")
        # Ensure all expected columns exist (e.g. if header changed between runs)
        for col in header:
            if col not in df.columns:
                df[col] = None
        df = df[header]
    else:
        LOG.info(f"No existing measurement file found, creating new one at {TIME_FILE}")
        df = pd.DataFrame(columns=header)
    return df


def upsert_rows(df, new_rows, header, key_cols=('triplestore', 'dataset', 'policy')):
    """
    Insert/update rows in df based on key_cols.
    Rows whose key_cols combination already exists are replaced;
    otherwise they are appended.
    """
    if not new_rows:
        return df

    new_df = pd.DataFrame(new_rows, columns=header)

    if df.empty:
        return new_df

    # Build a mask of existing rows whose key matches any new row's key
    key_tuples_new = set(
        tuple(row[col] for col in key_cols) for _, row in new_df.iterrows()
    )

    def row_key(row):
        return tuple(row[col] for col in key_cols)

    mask_to_drop = df.apply(lambda row: row_key(row) in key_tuples_new, axis=1)
    df = df[~mask_to_drop]

    df = pd.concat([df, new_df], ignore_index=True)
    return df


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------
def main():
    header = [
        'triplestore', 'dataset', 'policy', 'query_set',
        'snapshot', 'snapshot_ts', 'query',
        'execution_time', 'snapshot_creation_time', 'yn_timeout'
    ]

    # Load existing measurements (if any) instead of overwriting
    time_df = load_or_init_time_df(header)

    combinations = product(triple_stores, policies, datasets)

    Path(os.path.dirname(MEM_FILE)).mkdir(parents=True, exist_ok=True)
    with open(MEM_FILE, "w") as f:
        f.write("timestamp;label;pid;memory_gb\n")

    for triple_store, policy, dataset in combinations:
        if not eval_combi_exists(triple_store, dataset, policy):
            LOG.info(f"The combination {triple_store}, {dataset}, and {policy} is not supported and will be skipped")
            continue

        new_rows = run_queries(triple_store, policy, dataset)
        time_df = upsert_rows(time_df, new_rows, header)

        # Write back after each combination so progress isn't lost on crash
        LOG.info(f"Writing results to {TIME_FILE}")
        time_df.to_csv(TIME_FILE, sep=";", index=False, mode='w', header=True)


if __name__ == "__main__":
    main()
