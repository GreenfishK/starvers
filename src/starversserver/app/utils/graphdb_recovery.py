#!/usr/bin/env python3
import subprocess
import logging
import time
from pathlib import Path

# ---------------- CONFIG ----------------
STORAGE_DIR = "/opt/graphdb/home/data/repositories/BI2025/storage"
OUTPUT_FILE = "/opt/graphdb/recovery-output/BI2025_export.ttl"
STORAGE_TOOL = "/opt/graphdb/dist/bin/storage-tool"
SRC_INDEX = "pso" 
LOG_FILE = "/opt/graphdb/recovery-output/recovery.log"
# ----------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

def file_size(path: str) -> int:
    return Path(path).stat().st_size if Path(path).exists() else 0

def main():
    logging.info("=== GraphDB storage-tool recovery export started ===")
    logging.info(f"Storage dir: {STORAGE_DIR}")
    logging.info(f"Source index: {SRC_INDEX}")
    logging.info(f"Output file: {OUTPUT_FILE}")

    if not Path(STORAGE_DIR).exists():
        logging.error("Storage directory does not exist!")
        return 1

    Path("/opt/graphdb/recovery-output").mkdir(parents=True, exist_ok=True)

    size_before = file_size(OUTPUT_FILE)
    logging.info(f"Output size before export: {size_before} bytes")

    cmd = [
        STORAGE_TOOL, "export",
        "-s", STORAGE_DIR,
        "-f", OUTPUT_FILE,
        "--scan-history"
    ]

    logging.info("Executing:")
    logging.info(" ".join(cmd))

    start = time.time()
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )

    while True:
        out = proc.stdout.readline()
        err = proc.stderr.readline()

        if out:
            logging.info(f"[storage-tool] {out.strip()}")
        if err:
            logging.error(f"[storage-tool] {err.strip()}")

        if out == "" and err == "" and proc.poll() is not None:
            break

    rc = proc.wait()
    elapsed = time.time() - start
    size_after = file_size(OUTPUT_FILE)

    logging.info(f"Return code: {rc}")
    logging.info(f"Elapsed: {elapsed:.2f}s")
    logging.info(f"Output size after export: {size_after} bytes")

    if size_after > size_before:
        logging.info("Export produced data")
    else:
        logging.warning("Export produced no data")

    logging.info("=== GraphDB recovery export finished ===")
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
