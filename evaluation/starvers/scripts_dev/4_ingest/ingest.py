from dataclasses import dataclass
from fileinput import filename
from typing import Tuple
import threading
import queue
import os
import subprocess
import time
from pathlib import Path
import datetime
import shutil
import tomli


#####################################################################
# Configuration
#####################################################################
LOG_DIR = Path("/starvers_eval/output/logs/ingest")
MEASUREMENTS_FILE = "/starvers_eval/output/measurements/ingestion.csv"
CONFIG_PATH = "/starvers_eval/configs/eval_setup.toml"


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
    "ostrich": "/starvers_eval/rawdata",
    "ic_sr_ng": "alldata.ICNG.trig",
    "cb_sr_ng": "alldata.CBNG.trig",
    "tb_sr_ng": "alldata.TB.nq",
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


def policy_allowed(triplestore, policy):
    if triplestore == "ostrich":
        return policy == "ostrich"
    return policy in {"ic_sr_ng", "cb_sr_ng", "tb_sr_ng", "tb_sr_rs"}


def du_mib(path: Path) -> int:
    """
    Return apparent size in MiB (like du --apparent-size -BM).
    """
    result = subprocess.run(
        ["du", "-s", "--block-size=1M", "--apparent-size", str(path)],
        capture_output=True,
        text=True,
        check=True,
    )
    return int(result.stdout.split()[0])


def ensure_empty_dir(path: Path): 
    if path.exists():
        shutil.rmtree(path)
    path.mkdir(parents=True, exist_ok=True)


# Ingestion functions

# Ostrich
def run_ostrich(job: Job, run: int):
    """
    Ingest dataset-policy into Ostrich using virtual directory layout.
    """

    dataset = job.dataset
    policy = job.policy

    log(job.triplestore, f"Run {run}: Starting ingestion for dataset={dataset}, policy={policy}")

    raw_root = Path("/starvers_eval/rawdata")
    db_root = Path("/starvers_eval/databases/ostrich")

    with open(CONFIG_PATH, "rb") as f:
        CONFIG = tomli.load(f)

    assert policy == "ostrich"

    # pick the "first" query_set or the one you intend
    qs_name = list(CONFIG["datasets"][dataset]["query_sets"].keys())[0]

    versions = CONFIG["datasets"][dataset]["query_sets"][qs_name]["policies"][policy]["versions"]
    file_fmt_len = CONFIG["datasets"][dataset]["ic_basename_length"]
    filename = f"{1:0{file_fmt_len}d}.nt"   # zero-padded integer

    # Virtual directory
    vdir = Path(f"/ostrich/{policy}_{dataset}_run{run}")
    ensure_empty_dir(vdir)

    ic_dir = vdir / "alldata.IC.nt"
    ic_dir.mkdir(parents=True, exist_ok=True)

    cb_src = raw_root / dataset / "alldata.CB_computed.nt"
    ic_src = raw_root / dataset / "alldata.IC.nt" / filename

    (vdir / "alldata.CB.nt").symlink_to(cb_src)
    (ic_dir / filename).symlink_to(ic_src)

    # DB directory
    db_dir = db_root / f"{policy}_{dataset}"
    ensure_empty_dir(db_dir)

    start = time.time()
    subprocess.run(
        [
            "/opt/ostrich/ostrich-evaluate",
            "ingest",
            "never",
            "0",
            str(vdir),
            "1",
            str(versions),
        ],
        check=True,
        cwd=db_dir, # Working directory
    )
    ingestion_time = round(time.time() - start, 3)

    # Metrics
    raw_size = du_mib(cb_src) + du_mib(ic_src)
    db_size = du_mib(db_dir)

    # Cleanup
    if run != RUNS:
        log(job.triplestore, f"Run {run}: Cleaning up Ostrich DB and virtual directory for dataset={dataset}, policy={policy}")
        ensure_empty_dir(db_dir)
        ensure_empty_dir(vdir)

    log(job.triplestore, f"Run {run}: Completed ingestion for dataset={dataset}, policy={policy} in {ingestion_time} seconds.")

    return ("ostrich", policy, dataset, run, ingestion_time, raw_size, db_size)


# GraphDB
def run_graphdb(job: Job, run: int):
    dataset = job.dataset
    policy = job.policy
    repository_id = f"{policy}_{dataset}"

    log(job.triplestore, f"Run {run}: Starting GraphDB ingestion ({policy}, {dataset})")
    # Paths ---
    configs_dir = Path("/starvers_eval/configs/ingest/graphdb")
    db_root = Path("/starvers_eval/databases/graphdb")
    raw_file = Path("/starvers_eval/rawdata") / dataset / DATASET_DIR_OR_FILE_MAP[policy]

    configs_dir.mkdir(parents=True, exist_ok=True)
    db_root.mkdir(parents=True, exist_ok=True)

    # Prepare config ---
    config_file = configs_dir / f"{repository_id}.ttl"
    template_file = Path("/starvers_eval/scripts/4_ingest/configs/graphdb-config_template.ttl")
    text = template_file.read_text()
    text = text.replace("{{repositoryID}}", repository_id)
    config_file.write_text(text)

    #  Environment 
    env = os.environ.copy()
    env["JAVA_HOME"] = "/opt/java/java11/openjdk"
    env["PATH"] = "/opt/java/java11/openjdk/bin:" + env["PATH"]

    gdb_java_opts_base = env.get("GDB_JAVA_OPTS", "")
    env["GDB_JAVA_OPTS"] = (
        f"{gdb_java_opts_base} "
        f"-Dgraphdb.home.data={db_root}/{repository_id}"
    )

    # Ensure fresh DB dir 
    ensure_empty_dir(db_root / repository_id)

    # Run ingestion
    start = time.time()
    proc = subprocess.Popen(
        [
            "/opt/graphdb/dist/bin/importrdf",
            "preload",
            "--force",
            "-c", str(config_file),
            str(raw_file)
        ],
        env=env,
    )
    proc.wait()
    ingestion_time: float = round(time.time() - start, 3)

    # Terminate GraphDB
    subprocess.run(["pkill", "-P", str(proc.pid)], check=False)

    # Accept GraphDB's non-zero exit
    if proc.returncode not in (0, 1):
        raise RuntimeError(f"GraphDB failed (exit={proc.returncode}):\n{proc.stderr}")

    # Metrics 
    raw_size = du_mib(raw_file)
    disk_usage = du_mib(db_root / repository_id / "repositories")

    # Cleanup
    if run != RUNS:
        log(job.triplestore, f"Run {run}: Cleaning up GraphDB DB for dataset={dataset}, policy={policy}")
        ensure_empty_dir(db_root / repository_id)

    log(job.triplestore, f"Run {run}: Completed ingestion for dataset={dataset}, policy={policy} in {ingestion_time} seconds.")

    return ("graphdb", policy, dataset, run, ingestion_time, raw_size, disk_usage)



# Jena TDB2
def run_jena(job: Job, run: int):
    dataset = job.dataset
    policy = job.policy

    log(job.triplestore, f"Run {run}: Starting Jena TDB2 ingestion ({policy}, {dataset})")

    # Load template
    template_file = Path("/starvers_eval/scripts/4_ingest/configs/jenatdb2-config_template.ttl")
    configs_dir = Path("/starvers_eval/configs/ingest/jenatdb2")
    configs_dir.mkdir(parents=True, exist_ok=True)

    repositoryID = f"{policy}_{dataset}"
    config_file = configs_dir / f"{repositoryID}.ttl"
    shutil.copy(template_file, config_file)

    # Replace placeholders in config
    text = config_file.read_text()
    text = text.replace("{{repositoryID}}", repositoryID)
    text = text.replace("{{policy}}", policy)
    text = text.replace("{{dataset}}", dataset)
    config_file.write_text(text)

    # Set JAVA_HOME for Jena
    env = os.environ.copy()
    env["JAVA_HOME"] = "/opt/java/java17/openjdk"
    env["PATH"] = f"{env['JAVA_HOME']}/bin:" + env["PATH"]

    db_root = Path("/starvers_eval/databases/jenatdb2")
    data_dir = db_root / repositoryID
    ensure_empty_dir(data_dir)

    data_file = Path(f"/starvers_eval/rawdata/{dataset}/{DATASET_DIR_OR_FILE_MAP[policy]}")

    start = time.time()
    proc = subprocess.Popen(
        ["/jena-fuseki/tdbloader2", "--loc", str(data_dir), str(data_file)],

        env=env
    )
    proc.wait()
    ingestion_time = round(time.time() - start, 3)

    # Terminate Jena
    subprocess.run(["pkill", "-P", str(proc.pid)], check=False)

    # Metrics
    raw_size = du_mib(data_file)
    db_size = du_mib(data_dir)

    # Cleanup
    if run != RUNS:
        log(job.triplestore, f"Run {run}: Cleaning up Jena TDB2 DB for dataset={dataset}, policy={policy}")
        ensure_empty_dir(data_dir)

    log(job.triplestore, f"Run {run}: Completed ingestion for dataset={dataset}, policy={policy} in {ingestion_time} seconds.")

    return ("jenatdb2", policy, dataset, run, ingestion_time, raw_size, db_size)

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
                if not policy_allowed(triplestore, policy):
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




# Ingestion dispatch
def run_ingestion(job: Job, run: int):
    if job.triplestore == "ostrich":
        return run_ostrich(job, run)
    if job.triplestore == "graphdb":
        return run_graphdb(job, run)
    if job.triplestore == "jenatdb2":
        return run_jena(job, run)
    raise ValueError(job.triplestore)



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

    for i in range(num_workers):
        t = threading.Thread(target=worker, args=(i, job_queue), daemon=True)
        t.start()
        threads.append(t)

    for t in threads:
        t.join()


if __name__ == "__main__":
    main()