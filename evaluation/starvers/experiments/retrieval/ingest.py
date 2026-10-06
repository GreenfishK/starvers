#!/usr/bin/env python3
"""
ingest.py — ingest the three synthetic retrieval datasets (D1/D2/D3) into
GraphDB and Jena TDB2 for the Starvers Retrieval Evaluation.

Layout produced (relative to RETRIEVAL_BASE):
    <base>/databases/graphdb/<d1|d2|d3>/
    <base>/databases/jenatdb2/<d1|d2|d3>/
    <base>/configs/graphdb|jenatdb2/<d1|d2|d3>/<dataset>.ttl
Logging:  <base>/logs/ingest.log

Runs inside the starvers_eval container: RETRIEVAL_BASE defaults to
/starvers_eval/data/retrieval_exp. Uses the same config templates and bulk
loaders as the benchmark ingest step, but keeps repository IDs equal to the
dataset names (d1/d2/d3).
"""
import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path

BASE = Path(os.environ.get("RETRIEVAL_BASE", "/starvers_eval/data/retrieval_exp"))
DATA_DIR = BASE / "data"
DB_ROOT = BASE / "databases"
CONFIG_DIR = BASE / "configs"
LOG_FILE = BASE / "logs" / "ingest.log"

DATASETS = ["d1", "d2", "d3"]

# Store binaries / templates (reuse the ones the benchmark pipeline ships)
GRAPHDB_TEMPLATE = Path("/starvers_eval/scripts/4_ingest/configs/graphdb-config_template.ttl")
JENA_TEMPLATE = Path("/starvers_eval/scripts/4_ingest/configs/jenatdb2-config_template.ttl")
IMPORTRDF = "/opt/graphdb/dist/bin/importrdf"
TD_BLOADER = "/jena-fuseki/tdbloader2"

JAVA11 = "/opt/java/java11/openjdk"
JAVA17 = "/opt/java/java17/openjdk"


def setup_log() -> logging.Logger:
    (LOG_FILE.parent).mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("ingest")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s %(name)s:%(levelname)s:%(message)s",
                            datefmt="%Y-%m-%d %A %H:%M:%S")
    fh = logging.FileHandler(LOG_FILE, encoding="utf-8", mode="a+")
    fh.setFormatter(fmt)
    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


def render(template: Path, dest: Path, subs: dict) -> None:
    text = template.read_text()
    for key, val in subs.items():
        text = text.replace(key, str(val))
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text(text)


def ingest_graphdb(log, dataset: str) -> None:
    repo = dataset
    db_dir = DB_ROOT / "graphdb" / repo
    cfg_file = CONFIG_DIR / "graphdb" / repo / f"{repo}.ttl"
    data_file = DATA_DIR / dataset / "dataset.ttl"

    log.info("GraphDB: setup repo=%s (db=%s)", repo, db_dir)
    shutil.rmtree(db_dir, ignore_errors=True)
    shutil.rmtree(cfg_file.parent, ignore_errors=True)
    (db_dir / "repositories" / repo).mkdir(parents=True, exist_ok=True)
    render(GRAPHDB_TEMPLATE, cfg_file, {"{{repositoryID}}": repo})

    env = dict(os.environ)
    env["JAVA_HOME"] = JAVA11
    env["PATH"] = f"{JAVA11}/bin:" + env.get("PATH", "")
    env["GDB_JAVA_OPTS"] = env.get("GDB_JAVA_OPTS", "") + f" -Dgraphdb.home.data={db_dir}"

    log.info("GraphDB: preloading %s -> %s", data_file, repo)
    cmd = [IMPORTRDF, "preload", "--force", "-c", str(cfg_file), str(data_file)]
    subprocess.run(cmd, cwd=str(db_dir), env=env, check=True)
    log.info("GraphDB: ingested %s", repo)


def ingest_jena(log, dataset: str) -> None:
    repo = dataset
    db_dir = DB_ROOT / "jenatdb2" / repo
    cfg_file = CONFIG_DIR / "jenatdb2" / repo / f"{repo}.ttl"
    data_file = DATA_DIR / dataset / "dataset.ttl"

    log.info("Jena: setup repo=%s (db=%s)", repo, db_dir)
    shutil.rmtree(db_dir, ignore_errors=True)
    shutil.rmtree(cfg_file.parent, ignore_errors=True)
    db_dir.mkdir(parents=True, exist_ok=True)
    render(JENA_TEMPLATE, cfg_file,
           {"{{repositoryID}}": repo, "{{RUN_DIR}}": str(BASE)})

    env = dict(os.environ)
    env["JAVA_HOME"] = JAVA17
    env["PATH"] = f"{JAVA17}/bin:" + env.get("PATH", "")

    log.info("Jena: tdb2 loading %s -> %s", data_file, repo)
    cmd = [TD_BLOADER, "--loc", str(db_dir), str(data_file)]
    subprocess.run(cmd, cwd=str(db_dir), env=env, check=True)
    log.info("Jena: ingested %s", repo)


def main() -> None:
    log = setup_log()
    log.info("RETRIEVAL_BASE = %s", BASE)
    log.info("Dataset files: %s", DATA_DIR)
    DB_ROOT.mkdir(parents=True, exist_ok=True)

    for d in DATASETS:
        if not (DATA_DIR / d / "dataset.ttl").exists():
            log.error("Missing dataset file for %s under %s", d, DATA_DIR / d)
            sys.exit(1)

        ingest_graphdb(log, d)
        ingest_jena(log, d)

    log.info("Ingestion complete. Data under %s", DB_ROOT)


if __name__ == "__main__":
    main()
