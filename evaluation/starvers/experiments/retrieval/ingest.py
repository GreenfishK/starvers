#!/usr/bin/env python3
"""
ingest.py — ingest the three synthetic retrieval datasets (D1/D2/D3) into
GraphDB and Jena TDB2 for the Starvers Retrieval Evaluation.

Uses the shared triple_store_mgmt shell scripts (graphdb_mgmt.sh /
jenatdb2_mgmt.sh) and the endpoints/mgmt paths from eval_setup.toml, so no
ports/hosts/Java paths are hardcoded here.

Layout produced (relative to RETRIEVAL_BASE; each repository is
<dataset>_tb_sr_rs):
    <base>/databases/graphdb/<dataset>_tb_sr_rs/
    <base>/databases/jenatdb2/<dataset>_tb_sr_rs/
    <base>/configs/graphdb|jenatdb2/<dataset>_tb_sr_rs/<repo>.ttl
Logging: <base>/output/logs/ingest/ingest.log (plus mgmt scripts' own logs)
"""
import os
import subprocess
import sys
from pathlib import Path

import tomli

from experiments.logging import setup_logging

BASE = Path(os.environ.get("RETRIEVAL_BASE", "/starvers_eval/data/retrieval_exp"))
DATA_DIR = BASE / "data"
DB_ROOT = BASE / "databases"
CONFIG_DIR = BASE / "configs"
CONFIG_PATH = Path("/starvers_eval/configs/eval_setup.toml")
CONFIG_TMPL_DIR = "/starvers_eval/scripts/4_ingest/configs"

DATASETS = ["d1", "d2", "d3"]
POLICY = "tb_sr_rs"
STORES = ["graphdb", "jenatdb2"]


def load_config() -> dict:
    with open(CONFIG_PATH, "rb") as f:
        return tomli.load(f)


def repo(dataset: str) -> str:
    return f"{dataset}_{POLICY}"


def run_mgmt(args: list, log) -> None:
    log.info("  $ %s", " ".join(args))
    subprocess.run([str(a) for a in args], check=True)


def main() -> None:
    os.environ["RUN_DIR"] = str(BASE)
    _, log = setup_logging("ingest")
    config = load_config()
    log.info("RETRIEVAL_BASE = %s", BASE)
    DB_ROOT.mkdir(parents=True, exist_ok=True)

    for dataset in DATASETS:
        data_file = DATA_DIR / dataset / "dataset.ttl"
        if not data_file.exists():
            log.error("Missing dataset file: %s", data_file)
            sys.exit(1)

        for store in STORES:
            rep = repo(dataset)
            db_dir = DB_ROOT / store / rep
            mgmt_script = config["rdf_stores"][store]["mgmt_script"]
            log.info("Ingesting %s into %s (repo=%s, db=%s)", dataset, store, rep, db_dir)

            run_mgmt([mgmt_script, "create_env", dataset, POLICY,
                      str(db_dir), CONFIG_TMPL_DIR, str(CONFIG_DIR)], log)
            run_mgmt([mgmt_script, "ingest", str(db_dir), str(data_file),
                      dataset, POLICY, str(CONFIG_DIR)], log)
            log.info("Ingested %s -> %s", dataset, rep)

    log.info("Ingestion complete. Data under %s", DB_ROOT)


if __name__ == "__main__":
    main()
