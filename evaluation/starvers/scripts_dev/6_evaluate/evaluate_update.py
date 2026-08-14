import os
import sys
import time
import shlex
import logging
import subprocess
from pathlib import Path
from datetime import datetime, timedelta, timezone
import tomli
import pandas as pd
from urllib.error import HTTPError
import psutil

from starvers.starvers import TripleStoreEngine

from scripts.logging import setup_logging

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
BASE_LOG_DIR, LOG = setup_logging("evaluate_update")

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
LOG_FILE = f"{BASE_LOG_DIR}/evaluate_update/evaluate_update.log"

# Update evaluation writes its own repositories/database files under a
# dedicated sub-directory so it never overwrites the repositories created
# during the ingest step.
databases_dir = f"{os.environ['RUN_DIR']}/databases/updates"

# CSV header for the (single) update measurements file. The order matters and
# matches how rows are built in insert_ic0_and_cbs / measure_updates.
update_time_path = f"{os.environ['RUN_DIR']}/output/measurements/update_time.csv"
update_time_header = [
    'runs', 'triplestore', 'dataset', 'policy',
    'batch', 'cnt_batch_trpls', 'chunk_size', 'execution_time',
]

triple_stores = os.environ.get("triple_stores").split(" ")
policies = os.environ.get("policies").split(" ")
datasets = os.environ.get("datasets").split(" ")

# For update evaluation
in_frm = "nt"
LOCAL_TIMEZONE = datetime.now(timezone.utc).astimezone().tzinfo
init_version_timestamp = datetime(2022, 10, 1, 12, 0, 0, 0, LOCAL_TIMEZONE)

dataset_versions = {dataset: infos['snapshot_versions'] for dataset, infos in static_eval_params['datasets'].items()}
ic_basename_lengths = {dataset: infos['ic_basename_length'] for dataset, infos in static_eval_params['datasets'].items()}
snapshot_dir = static_eval_params['general']['snapshot_dir']
change_sets_dir = static_eval_params['general']['change_sets_dir']


# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------
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


def print_mem_file_tail(mem_file, lines=20):
    with open(mem_file, "r") as f:
        all_lines = f.readlines()
        LOG.info(f"--------------- Memory tail start --------------- ")
        LOG.info(f"{all_lines[0]}")
        for line in all_lines[-lines:]:
            LOG.info({line})
        LOG.info(f"--------------- Memory tail end --------------- ")


def load_or_init_update_df() -> pd.DataFrame:
    """Load the existing update_time.csv (or create an empty frame with the header)."""
    if os.path.exists(update_time_path) and os.path.getsize(update_time_path) > 0:
        LOG.info(f"Loading existing update measurements file {update_time_path}")
        df = pd.read_csv(update_time_path, sep=";")
        # Ensure all expected columns exist (e.g. if header changed between runs)
        for col in update_time_header:
            if col not in df.columns:
                df[col] = None
        df = df[update_time_header]
    else:
        LOG.info(f"No existing update measurements file found, creating new one at {update_time_path}")
        df = pd.DataFrame(columns=update_time_header)
    return df


def upsert_rows(df, new_rows, key_cols=('runs', 'triplestore', 'dataset', 'policy', 'batch', 'chunk_size')):
    """
    Insert/update rows in df based on key_cols (a primary key).
    Rows whose key_cols combination already exists are replaced;
    otherwise they are appended.
    """
    if new_rows is None or len(new_rows) == 0:
        return df

    new_df = new_rows[update_time_header].copy()

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


def measure_updates(triple_store: str, dataset: str, policy: str, chunk_size: int, runs: int, source_ic0: str, source_cs: str, last_version: int, init_timestamp: datetime):
    LOG.info(f"Measuring update times for {triple_store}/{policy}/{dataset} with chunk size {chunk_size} over {runs} runs.")

    run_measurements: list[pd.DataFrame] = []
    for run_idx in range(runs):
        LOG.info(f"Run {run_idx + 1}/{runs} ...")
        result = insert_ic0_and_cbs(triple_store, chunk_size, dataset=dataset, policy=policy,
                                    source_ic0=source_ic0, source_cs=source_cs,
                                    last_version=last_version, init_timestamp=init_timestamp)
        if result is False:
            # Stop iteration if HTTPError occurred
            LOG.info("HTTPError occurred, stopping update evaluation for this combination.")
            break
        run_measurements.append(result)

    if not run_measurements:
        return

    combined_measurements = pd.concat(run_measurements, join="inner")

    # Average the execution time per batch across runs
    group_cols = [c for c in combined_measurements.columns if c != 'execution_time']
    averaged_measurements = combined_measurements.groupby(group_cols, as_index=False)['execution_time'].mean()

    # Add the number of runs that were averaged
    averaged_measurements.insert(0, 'runs', runs)

    # Upsert this combination's rows into the single update measurements file,
    # replacing any rows with the same primary key
    # (runs, triplestore, dataset, policy, batch, chunk_size).
    update_df = load_or_init_update_df()
    update_df = upsert_rows(update_df, averaged_measurements)

    LOG.info("Writing update performance measurements to disk ...")
    Path(os.path.dirname(update_time_path)).mkdir(parents=True, exist_ok=True)
    update_df.to_csv(update_time_path, sep=";", index=False, mode='w', header=True)


def insert_ic0_and_cbs(triple_store: str, chunk_size: int, dataset: str, policy: str,
                        source_ic0: str, source_cs: str, last_version: int, init_timestamp: datetime):
    triple_store_name = triple_store.lower()
    LOG.info(f"Constructing timestamped RDF-star dataset from ICs and changesets triple store {triple_store} and chunk size {chunk_size}.")

    repository = policy + "_" + dataset
    database_dir = f"{databases_dir}/{triple_store_name}/{repository}"
    mgmt_script = static_eval_params["rdf_stores"][triple_store_name]["mgmt_script"]

    LOG.info(f"Create {triple_store} directories and environment")
    LOG.info(f"\nDatabase directory {database_dir}\nConfig dirctory:{CONFIG_DIR}\nConfig Template directory:{CONFIG_TMPL_DIR}")
    subprocess.call(shlex.split(f"{mgmt_script} --log-file {LOG_FILE} create_env {policy} {dataset} {database_dir} {CONFIG_TMPL_DIR} {CONFIG_DIR}"))

    LOG.info(f"Ingest empty file into {repository} repository and start {triple_store_name}.")
    subprocess.call(shlex.split(f"{mgmt_script} --log-file {LOG_FILE} ingest_empty {database_dir} {policy} {dataset} {CONFIG_DIR}"))

    LOG.info(f"Startup {triple_store} engine")
    subprocess.call(shlex.split(f"{mgmt_script} --log-file {LOG_FILE} startup {database_dir} {policy} {dataset} {CONFIG_DIR}"))

    LOG.info("Read initial snapshot {0} into memory.".format(source_ic0))
    added_triples_raw = open(source_ic0, "r").read().splitlines()
    added_triples_raw = list(filter(None, added_triples_raw))
    added_triples_raw = list(filter(lambda x: not x.startswith("# "), added_triples_raw))

    LOG.info("Add triples from initial snapshot {0} as nested triples into the RDF-star dataset.".format(source_ic0))

    query_endpoint = static_eval_params["rdf_stores"][triple_store_name]["get"].format(repo=f"{policy}_{dataset}")
    update_endpoint = static_eval_params["rdf_stores"][triple_store_name]["post"].format(repo=f"{policy}_{dataset}")
    rdf_star_engine = TripleStoreEngine(query_endpoint, update_endpoint)
    try:
        start = time.time()
        rdf_star_engine.insert(triples=added_triples_raw, timestamp=init_timestamp, chunk_size=chunk_size)
        end = time.time()
    except HTTPError:
        LOG.info("Too many triples transfered over HTTP. No measures for this chunk size setting will be recorded")
        return False
    execution_time_insert = end - start

    df = pd.DataFrame(columns=['triplestore', 'dataset', 'policy', 'batch', 'cnt_batch_trpls', 'chunk_size', 'execution_time'],
                        data=[[triple_store, dataset, policy, 'snapshot_0', len(added_triples_raw), chunk_size, execution_time_insert]])

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
        LOG.info(f"Memory in usage: {mem_in_usage}%")
        if mem_in_usage > 85:
            # Reboot to free up main memory
            subprocess.call(shlex.split(f"{mgmt_script} --log-file {LOG_FILE} shutdown"))
            subprocess.call(shlex.split(f"{mgmt_script} --log-file {LOG_FILE} startup {database_dir} {policy} {dataset} {CONFIG_DIR}"))

        if filename.startswith("data-added"):
            LOG.info("Read positive changeset {0} into memory.".format(filename))
            added_triples_raw = open(source_cs + "/" + filename, "r").read().splitlines()
            added_triples_raw = list(filter(None, added_triples_raw))
            cnt_trpls = len(added_triples_raw)

            LOG.info(f"Add {cnt_trpls} triples from changeset {filename} as nested triples into the RDF-star dataset.")
            start = time.time()
            rdf_star_engine.insert(triples=added_triples_raw, timestamp=vers_ts, chunk_size=chunk_size)
            end = time.time()
            execution_time_insert = end - start
            new_row = pd.DataFrame([[triple_store, dataset, policy, 'positive_change_set_' + str(version), len(added_triples_raw), chunk_size, execution_time_insert]], columns=df.columns)
            df = pd.concat([df, new_row], ignore_index=True)

        if filename.startswith("data-deleted"):
            LOG.info("Read negative changeset {0} into memory.".format(filename))
            deleted_triples_raw = open(source_cs + "/" + filename, "r").read().splitlines()
            deleted_triples_raw = list(filter(None, deleted_triples_raw))
            cnt_trpls = len(deleted_triples_raw)

            LOG.info(f"Oudate {cnt_trpls} triples in the RDF-star dataset which match the triples in {filename}.")
            start = time.time()
            rdf_star_engine.outdate(triples=deleted_triples_raw, timestamp=vers_ts, chunk_size=chunk_size)
            end = time.time()
            execution_time_outdate = end - start
            new_row = pd.DataFrame([[triple_store, dataset, policy, 'negative_change_set_' + str(version), len(deleted_triples_raw), chunk_size, execution_time_outdate]], columns=df.columns)
            df = pd.concat([df, new_row], ignore_index=True)

    # Shutdown engine
    subprocess.call(shlex.split(f"{mgmt_script} --log-file {LOG_FILE} shutdown"))

    return df


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------
def main():
    # Dedicated run directory for update repositories/database files so that
    # running the update evaluation never overwrites ingest-created repositories.
    os.makedirs(databases_dir, exist_ok=True)

    chunk_size = 7000
    runs = 1
    for dataset in datasets:
        for policy in policies:
            for triple_store in triple_stores:
                data_dir = f"{os.environ['RUN_DIR']}/rawdata/{dataset}"
                total_versions = dataset_versions[dataset]

                measure_updates(triple_store=triple_store,
                        dataset=dataset,
                        policy=policy,
                        chunk_size=chunk_size,
                        runs=runs,
                        source_ic0=f"{data_dir}/{snapshot_dir}/" + "1".zfill(ic_basename_lengths[dataset])  + ".nt",
                        source_cs=f"{data_dir}/{change_sets_dir}.{in_frm}",
                        last_version=total_versions,
                        init_timestamp=init_version_timestamp)


if __name__ == "__main__":
    main()
