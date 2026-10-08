#!/usr/bin/env python3
"""
evaluate.py — run the retrieval query Q on GraphDB and Jena TDB2 (D1/D2/D3) and
measure query runtime.

Uses the shared triple_store_mgmt shell scripts for store lifecycle and the
endpoints/mgmt paths from eval_setup.toml ([rdf_stores][store]->get / mgmt_script),
so no ports, hosts, or Java paths are hardcoded here.

For each (store, dataset) it starts the store via its mgmt script, executes the
decorator retrieval query Q RETRIEVAL_RUNS times (default 10), records one row
per run, then shuts the store down:

    <base>/output/measurements/retrieval.csv
Logging: <base>/output/logs/evaluate/evaluate.log (plus mgmt scripts' own logs)
"""
import csv
import os
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout
from pathlib import Path

import tomli
from SPARQLWrapper import SPARQLWrapper, JSON, POST

from experiments.logging import setup_logging

BASE = Path(os.environ.get("SYNTH_PERF_EVAL_BASE", "/starvers_eval/data/synth_perf_eval"))
DB_ROOT = BASE / "databases"
CONFIG_DIR = BASE / "configs"
OUTPUT = BASE / "output"
MEAS_DIR = OUTPUT / "measurements"
MEAS_FILE = MEAS_DIR / "retrieval.csv"

CONFIG_PATH = Path("/starvers_eval/configs/eval_setup.toml")
QUERY_FILE = Path("/starvers_eval/experiments/synth_perf_eval/decorator_query.txt")

POLICY = "tb_sr_rs"
DATASETS = ["d1", "d2", "d3"]
STORES = ["graphdb", "jenatdb2"]
RUNS = int(os.environ.get("RETRIEVAL_RUNS", 10))
QUERY_TIMEOUT = 300

CSV_HEADER = ["triplestore", "dataset", "run", "query_time_s"]


def load_config() -> dict:
    with open(CONFIG_PATH, "rb") as f:
        return tomli.load(f)


def repo(dataset: str) -> str:
    return f"{dataset}_{POLICY}"


# ---------------------------------------------------------------------------
# Query timing
# ---------------------------------------------------------------------------
def measure_query(endpoint: str, query: str) -> float:
    """Run the query once and return elapsed time in seconds."""
    engine = SPARQLWrapper(endpoint)
    engine.setMethod(POST)
    engine.addCustomHttpHeader("Accept", "application/sparql-results+json")
    engine.setReturnFormat(JSON)
    engine.setQuery(query)
    result = {}

    def _run():
        result["start"] = time.time()
        result["resp"] = engine.query().convert()
        result["end"] = time.time()

    with ThreadPoolExecutor(max_workers=1) as ex:
        fut = ex.submit(_run)
        fut.result(timeout=QUERY_TIMEOUT)
    return result["end"] - result["start"]


def timed_runs(endpoint: str, query: str, log) -> list[float]:
    log.info("warmup query ...")
    try:
        measure_query(endpoint, query)
    except Exception as e:
        log.warning("warmup failed: %s", e)

    times = []
    for i in range(1, RUNS + 1):
        t0 = time.time()
        try:
            t = measure_query(endpoint, query)
        except FuturesTimeout:
            log.error("query timed out (>%ss)", QUERY_TIMEOUT)
            t = -1.0
        except Exception as e:
            log.error("query error: %s", e)
            t = -1.0
        times.append(t)
        log.info("run %d: %.4f s (wall %.2f s)", i, t, time.time() - t0)
    return times


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    os.environ["RUN_DIR"] = str(BASE)
    _, log = setup_logging("evaluate")
    config = load_config()
    query = QUERY_FILE.read_text()
    log.info("SYNTH_PERF_EVAL_BASE = %s; RUNS = %d", BASE, RUNS)
    log.info("Query:\n%s", query)

    MEAS_DIR.mkdir(parents=True, exist_ok=True)
    new_file = (not MEAS_FILE.exists()) or MEAS_FILE.stat().st_size == 0
    with open(MEAS_FILE, "a", newline="") as f:
        w = csv.writer(f, delimiter=";")
        if new_file:
            w.writerow(CSV_HEADER)

    rows = []
    for dataset in DATASETS:
        for store in STORES:
            rep = repo(dataset)
            conf = config["rdf_stores"][store]
            mgmt_script = conf["mgmt_script"]
            endpoint = conf["get"].format(repo=rep)
            db_dir = DB_ROOT / store / rep

            log.info("%s: starting repo=%s", store, rep)
            if store == "jenatdb2":
                subprocess.run([mgmt_script, "startup", str(db_dir),
                                dataset, POLICY, str(CONFIG_DIR)], check=True)
            else:
                subprocess.run([mgmt_script, "startup", str(db_dir),
                                dataset, POLICY], check=True)
            try:
                times = timed_runs(endpoint, query, log)
            finally:
                subprocess.run([mgmt_script, "shutdown"], check=True)
            for i, t in enumerate(times, 1):
                rows.append([store, dataset, i, t])
            log.info("%s: done repo=%s", store, rep)

    with open(MEAS_FILE, "a", newline="") as f:
        csv.writer(f, delimiter=";").writerows(rows)
    log.info("Wrote %d measurement rows to %s", len(rows), MEAS_FILE)


if __name__ == "__main__":
    main()
