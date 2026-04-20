from dataclasses import dataclass
from fileinput import filename
from typing import Tuple
import threading
import queue
import os
import subprocess
import shlex
import time
from pathlib import Path
import datetime
import shutil
import tomli
from SPARQLWrapper import SPARQLWrapper, JSON, GET


#####################################################################
# Configuration
#####################################################################
LOG_DIR = Path(f"{os.environ['RUN_DIR']}/output/logs/ingest")

if not LOG_DIR.exists():
    LOG_DIR.mkdir(parents=True, exist_ok=True)

MEASUREMENTS_FILE = f"{os.environ['RUN_DIR']}/output/measurements/ingestion.csv"
CONFIG_PATH = "/starvers_eval/configs/eval_setup.toml"
CNT_QUERIES_PATH = "/starvers_eval/scripts/4_ingest/cnt_queries"
CONFIG_TMPL_DIR = "/starvers_eval/scripts/4_ingest/configs"
CONFIG_DIR = "/starvers_eval/configs/ingest"

LOG_FILES = {
    "ostrich": LOG_DIR / "ingestion_ostrich.txt",
    "graphdb": LOG_DIR / "ingestion_graphdb.txt",
    "jenatdb2": LOG_DIR / "ingestion_jena.txt",
}

DATASETS = os.environ.get("datasets").split(" ")
POLICIES = os.environ.get("policies").split(" ")
TRIPLE_STORES = os.environ.get("triple_stores").split(" ")

LOCK_DIR = Path("/starvers_eval/locks")
LOCK_DIR.mkdir(parents=True, exist_ok=True)

DATASET_DIR_OR_FILE_MAP = {
    "ostrich": "alldata_vdir",
    "ic_sr_ng": "alldata.ICNG.trig",
    "cb_sr_ng": "alldata.CBNG.trig",
    "tb_sr_ng": "alldata.TB_computed.nq",
    "tb_sr_rs": "alldata.TB_star_hierarchical.ttl",
}

RUNS = 10

#####################################################################
# Classes
#####################################################################
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

#####################################################################
# Functions
#####################################################################
# Helpers
def log(triplestore: str, message: str):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %A %H:%M:%S")
    log_file = LOG_FILES[triplestore]
    with open(log_file, "a") as f:
        f.write(f"{ts} root:INFO: {message}\n")


def eval_combi_exists(triplestore: str, dataset: str, policy: str) -> bool:
    try:
        with open(CONFIG_PATH, "rb") as f:
            CONFIG = tomli.load(f)
        return policy in CONFIG["evaluations"][triplestore][dataset]
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
    with open(CONFIG_PATH, "rb") as f:
        CONFIG = tomli.load(f)

    query_endpoint = CONFIG["rdf_stores"][job.triplestore]["get"].format(repo=f"{job.policy}_{job.dataset}")
    log(job.triplestore, f"Setting endpoint for counting triples: {query_endpoint}")

    engine = SPARQLWrapper(endpoint=query_endpoint)
    engine.setReturnFormat(JSON)
    engine.setOnlyConneg(True)
    engine.setMethod(GET)

    if job.policy in ["ic_sr_ng", "cb_sr_ng", "ostrich"]:
        with open(f"{CNT_QUERIES_PATH}/ic_sr_ng.sparql", "r") as cnt_query_file:
            query_string = cnt_query_file.read()
            engine.setQuery(query_string)
            try:
                result = engine.query()
                log(job.triplestore, f"Number of triples: {result}")

            except Exception as e:
                log(job.triplestore, f"ERROR: The following exeception occured while counting triples: {e}")
    else:
        log(job.triplestore, "Supported policies for counting triples are: ic_sr_ng")

def ensure_empty_dir(path: Path): 
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)




#####################################################################
# Job scheduling
#####################################################################
# Job queue
job_queue = queue.Queue()
lock_manager = DatasetPolicyLock()
results_lock = threading.Lock()

# Enqueue jobs
def enqueue_jobs():
    """
    Queue one job per triplestore, dataset, policy combination.
    Each job will internally run `RUNS` times.
    """
    for triplestore in TRIPLE_STORES:
        for dataset in DATASETS:
            for policy in POLICIES:
                if not eval_combi_exists(triplestore, dataset, policy):
                    continue
                job_queue.put(Job(triplestore, dataset, policy))


# Result writer
def write_result(row):
    with results_lock:
        with open(MEASUREMENTS_FILE, "a") as f:
            f.write(";".join(map(str, row)) + "\n")


# Worker
def worker(worker_id: int, job_queue: queue.Queue):
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


# Ingest dispatch
def run_ingestion(job: Job, run: int):
    dataset = job.dataset
    policy = job.policy
    repository_id = f"{policy}_{dataset}"

    log(job.triplestore, f"Run {run}: Starting {job.triplestore} for dataset={dataset}, policy={policy}")
    
    # Setup environment
    db_root = Path(f"{os.environ['RUN_DIR']}/databases/{job.triplestore}")
    database_dir = db_root / repository_id

    with open(CONFIG_PATH, "rb") as f:
        CONFIG = tomli.load(f)

    mgmt_script = CONFIG["rdf_stores"][job.triplestore]["mgmt_script"]
    subprocess.run([f"{mgmt_script}", "create_env", policy, dataset, database_dir, CONFIG_TMPL_DIR, CONFIG_DIR], check=True)
    
    dataset_dir = Path(f"{os.environ['RUN_DIR']}/rawdata/{dataset}/{DATASET_DIR_OR_FILE_MAP[policy]}")

    # Ingest and measure time
    start = time.time()
    subprocess.run([f"{mgmt_script}", "ingest", database_dir, dataset_dir, policy, dataset, CONFIG_DIR], check=True)
    ingestion_time = round(time.time() - start, 3)

    # Metrics
    raw_size = du_mib(dataset_dir)
    db_size = du_mib(database_dir)
    count_triples(job)

    # Cleanup
    if run != RUNS:
        log(job.triplestore, f"Run {run}: Cleaning up {job.triplestore} for dataset={dataset}, policy={policy}")
        ensure_empty_dir(database_dir)

    log(job.triplestore, f"Run {run}: Completed ingestion for dataset={dataset}, policy={policy} in {ingestion_time} seconds.")

    return (job.triplestore, policy, dataset, run, ingestion_time, raw_size, db_size)



# Start
def main():
    # Write header
    with open(MEASUREMENTS_FILE, "w") as f:
        f.write("triplestore;policy;dataset;run;ingestion_time;raw_file_size_MiB;db_files_disk_usage_MiB\n")

    enqueue_jobs()

    # Parallel execution changes the runtimes for the evaluated RDF stores unproportionally. 
    # While Jena needs the same amount of time to ingest data, GraphDB takes twice as much 
    # while running in parallel to Jena.
    # That is why we currently limit ourselves to a serial ingest.
    num_workers =  1
    threads = []

    # Run ingestion
    for i in range(num_workers):
        t = threading.Thread(target=worker, args=(i, job_queue), daemon=True)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()


if __name__ == "__main__":
    main()