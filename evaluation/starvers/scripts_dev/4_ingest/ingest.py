from dataclasses import dataclass
from typing import Tuple
import threading
import queue
import os
import subprocess
import time
from pathlib import Path
import tomli
from SPARQLWrapper import SPARQLWrapper, JSON, GET
import logging
import sys

from scripts.logging import setup_logging

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
_loggers: dict[str, logging.Logger] = {}

def get_ts_logger(triplestore: str) -> logging.Logger:
    """Return a logger that writes exclusively to the triplestore's log file."""
    
    if triplestore in _loggers:
        return _loggers[triplestore]
    _, logger = setup_logging(f"ingestion_{triplestore}", sub_dir="ingest")
    _loggers[triplestore] = logger

    return logger

def log(triplestore: str, message: str):
    get_ts_logger(triplestore).info(message)

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
MEASUREMENTS_FILE = f"{os.environ['RUN_DIR']}/output/measurements/storage_and_ingestion.csv"
CNT_QUERIES_PATH = "/starvers_eval/scripts/4_ingest/cnt_queries"
CONFIG_TMPL_DIR = "/starvers_eval/scripts/4_ingest/configs"
CONFIG_DIR = f"{os.environ['RUN_DIR']}/configs/ingest"

DATASETS = os.environ.get("datasets").split(" ")
POLICIES = os.environ.get("policies").split(" ")
TRIPLE_STORES = os.environ.get("triple_stores").split(" ")

LOCK_DIR = Path("/starvers_eval/locks")
LOCK_DIR.mkdir(parents=True, exist_ok=True)

DATASET_DIR_OR_FILE_MAP = {
    "ostrich": "alldata_vdir",
    "ostrich_aggchange": "alldata_vdir",
    "ic_sr_ng": "alldata.ICNG.trig",
    "cb_sr_ng": "alldata.CBNG.trig",
    "tb_sr_ng": "alldata.TB_computed.nq",
    "tb_sr_rs": "alldata.TB_star_hierarchical.ttl",
    "tb_sr_re": "alldata.TB_star_reif.ttl"
}

RUNS = 1

# ---------------------------------------------------------------------------
# Classes
# ---------------------------------------------------------------------------
# Job definition
@dataclass(frozen=True)
class Job:
    triplestore: str
    dataset: str
    policy: str

    @property
    def lock_key(self) -> Tuple[str, str, str]:
        return (self.dataset, self.policy, self.triplestore)


# Lock manager for dataset-policy combinations
class DatasetPolicyLock:
    def __init__(self):
        self._locks = {}
        self._global = threading.Lock()

    def acquire(self, key):
        with self._global:
            lock = self._locks.setdefault(key, threading.Lock())
        lock.acquire()

    def release(self, key):
        self._locks[key].release()

# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------
# Helpers


def eval_combi_exists(triplestore: str, dataset: str, policy: str) -> bool:
    try:
        return policy in static_eval_params["evaluations"][triplestore][dataset]
    except KeyError:
        return False
    

def du_mib(path: Path) -> int:
    """
    Return apparent size in MiB (like du --apparent-size -BM).
    """
    result = subprocess.run(
        ["du", "-s", "-L", "--block-size=1M", "--apparent-size", str(path)],
        capture_output=True,
        text=True,
        check=True,
    )
    return int(result.stdout.split()[0])


def count_triples(job: Job):
    query_endpoint = static_eval_params["rdf_stores"][job.triplestore]["get"].format(repo=f"{job.policy}_{job.dataset}")
    log(job.triplestore, f"Setting endpoint for counting triples: {query_endpoint}")

    engine = SPARQLWrapper(endpoint=query_endpoint)
    engine.setReturnFormat(JSON)
    engine.setOnlyConneg(True)
    engine.setMethod(GET)
    engine.addCustomHttpHeader("Accept", "application/sparql-results+json")

    if job.policy in ["ic_sr_ng", "ostrich", "ostrich_aggchange"]:
        with open(f"{CNT_QUERIES_PATH}/ic_sr_ng.sparql", "r") as cnt_query_file:
            query_string = cnt_query_file.read()
            engine.setQuery(query_string)
            try:
                result = engine.query().convert()
                count = int(result["results"]["bindings"][0]["count"]["value"])
                log(job.triplestore, f"Number of triples: {count}")
            except Exception as e:
                log(job.triplestore, f"ERROR: The following exeception occured while counting triples: {e}")
    else:
        log(job.triplestore, "Supported policies for counting triples are: ic_sr_ng, ostrich, and ostrich_aggchange.")

def ensure_empty_dir(path: Path): 
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)




# ---------------------------------------------------------------------------
# Job scheduling
# ---------------------------------------------------------------------------
# Job queue
job_queue = queue.Queue()
lock_manager = DatasetPolicyLock()
results_lock = threading.Lock()

# Enqueue jobs
def enqueue_jobs():
    count = 0
    for triplestore in TRIPLE_STORES:
        for dataset in DATASETS:
            for policy in POLICIES:
                if not eval_combi_exists(triplestore, dataset, policy):
                    log(triplestore, f"SKIP: {triplestore}/{dataset}/{policy} not in evaluations config")
                    continue
                job_queue.put(Job(triplestore, dataset, policy))
                count += 1
    log(triplestore, f"Enqueued {count} jobs")
    if count == 0:
        print("[ingest] ERROR: No jobs enqueued — check triple_stores/policies/datasets env vars against eval_setup.toml", file=sys.stderr)
        sys.exit(1)

HEADER = "triplestore;policy;dataset;run;ingestion_time;raw_file_size_MiB;db_files_disk_usage_MiB"
KEY_COLS = ("triplestore", "policy", "dataset", "run")


def load_or_init_measurements() -> list[dict]:
    """
    Return the existing rows as a list of dicts, or an empty list if the file
    does not exist / is empty.
    """
    path = Path(MEASUREMENTS_FILE)
    if not path.exists() or path.stat().st_size == 0:
        return []
    rows = []
    with open(path, newline="") as f:
        col_names = HEADER.split(";")
        for line in f:
            line = line.rstrip("\n")
            if not line or line == HEADER:
                continue
            values = line.split(";")
            rows.append(dict(zip(col_names, values)))
    return rows


def save_measurements(rows: list[dict]):
    """Write the full list of rows back to MEASUREMENTS_FILE."""
    with open(MEASUREMENTS_FILE, "w") as f:
        f.write(HEADER + "\n")
        col_names = HEADER.split(";")
        for row in rows:
            f.write(";".join(str(row.get(c, "")) for c in col_names) + "\n")


def upsert_result(existing: list[dict], new_row: tuple) -> list[dict]:
    """
    Insert or replace a row in *existing* using
    (triplestore, policy, dataset, run) as the primary key.  A re-run for the
    same store/policy/dataset/run replaces the earlier measurement.  Returns
    the updated list.
    """
    col_names = HEADER.split(";")
    new_dict  = dict(zip(col_names, map(str, new_row)))
    new_key   = tuple(new_dict[k] for k in KEY_COLS)

    # Drop any existing row(s) with the same key
    updated = [r for r in existing if tuple(r[k] for k in KEY_COLS) != new_key]
    updated.append(new_dict)
    return updated


# Module-level in-memory store — workers append here; main() flushes to disk
_results: list[dict] = []


def write_result(row: tuple):
    """Thread-safe upsert + immediate flush to disk."""
    global _results
    with results_lock:
        _results = upsert_result(_results, row)
        save_measurements(_results)


_worker_exception = None
# Worker
def worker(worker_id: int, job_queue: queue.Queue):
    global _worker_exception
    try:
        while True:
            try:
                job = job_queue.get(timeout=2)
            except queue.Empty:
                return
            for run in range(1, RUNS + 1):
                lock_manager.acquire(job.lock_key)
                try:
                    result = run_ingestion(job, run)
                finally:
                    lock_manager.release(job.lock_key)
                write_result(result)
            job_queue.task_done()
    except Exception as e:
        _worker_exception = e
        raise  # still prints traceback to stderr


# Ingest dispatch
def run_ingestion(job: Job, run: int):
    dataset = job.dataset
    policy = job.policy
    repository_id = f"{policy}_{dataset}"

    log(job.triplestore, f"Run {run}: Starting {job.triplestore} for dataset={dataset}, policy={policy}")
    
    # Setup environment
    db_root = Path(f"{os.environ['RUN_DIR']}/databases/{job.triplestore}")
    database_dir = db_root / repository_id

    mgmt_script = static_eval_params["rdf_stores"][job.triplestore]["mgmt_script"]
    subprocess.run([f"{mgmt_script}", "create_env", policy, dataset, database_dir, CONFIG_TMPL_DIR, CONFIG_DIR], check=True)
    
    dataset_dir = Path(f"{os.environ['RUN_DIR']}/rawdata/{dataset}/{DATASET_DIR_OR_FILE_MAP[policy]}")

    # Ingest and measure time
    start = time.time()
    subprocess.run([f"{mgmt_script}", "ingest", database_dir, dataset_dir, policy, dataset, CONFIG_DIR], check=True)
    ingestion_time = round(time.time() - start, 3)

    # Metrics
    raw_size = du_mib(dataset_dir)
    db_size = du_mib(database_dir)

    # Start database
    subprocess.run([f"{mgmt_script}", "startup", str(database_dir), policy, dataset, CONFIG_DIR], check=True)

    count_triples(job)

    # Shutdown database
    subprocess.run([f"{mgmt_script}", "shutdown"], check=True)

    # Cleanup
    if run != RUNS:
        log(job.triplestore, f"Run {run}: Cleaning up {job.triplestore} for dataset={dataset}, policy={policy}")
        ensure_empty_dir(database_dir)

    log(job.triplestore, f"Run {run}: Completed ingestion for dataset={dataset}, policy={policy} in {ingestion_time} seconds.")

    return (job.triplestore, policy, dataset, run, ingestion_time, raw_size, db_size)


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------

# Start
def main():
    global _results

    # Load whatever is already on disk (may be empty on first run)
    _results = load_or_init_measurements()

    enqueue_jobs()

    num_workers = 1
    threads = []

    for i in range(num_workers):
        t = threading.Thread(target=worker, args=(i, job_queue), daemon=True)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    if _worker_exception is not None:
        print(f"[ingest] Worker failed: {_worker_exception}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()