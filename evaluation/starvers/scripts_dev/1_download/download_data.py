"""
download.py

Rewrite of download.sh in Python.

Downloads:
  1. Per-dataset snapshot archives and named-graph datasets (BEAR/ORKG).
  2. Query sets for BEARA, BEARB, BEARC, and ORKG (SciQA).

Writes:
  - RUN_DIR/output/measurements/datasets_meta.csv
  - RUN_DIR/output/measurements/queries_meta.csv
"""

import csv
import gzip
import os
import shutil
import subprocess
import tarfile
import zipfile
from pathlib import Path
from urllib.parse import urlparse

import requests
import tomli

from scripts.logging import setup_logging

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
BASE_LOG_DIR, LOG = setup_logging("download")

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
RUN_DIR     = Path(os.environ["RUN_DIR"])

MEASUREMENTS_DIR = RUN_DIR / "output" / "measurements"
METADATA_CSV     = MEASUREMENTS_DIR / "datasets_meta.csv"
QUERIES_CSV      = MEASUREMENTS_DIR / "queries_meta.csv"

DOWNLOADED_QUERIES_DIR = RUN_DIR / "queries" / "downloaded_queries"

DATASETS     = os.environ.get("datasets", "").split()
WGET_RETRIES = 3


# ---------------------------------------------------------------------------
# Static eval parameters helpers
# ---------------------------------------------------------------------------

def _qs_links(config: dict, dataset: str, qs_name: str) -> list[str]:
    return (
        config.get("datasets", {})
        .get(dataset, {})
        .get("query_sets", {})
        .get(qs_name, {})
        .get("download_links", [])
    )


# ---------------------------------------------------------------------------
# Download helper (mirrors `wget -t 3 -c`)
# ---------------------------------------------------------------------------

def _download(url: str, dest_dir: Path, filename: str | None = None) -> Path:
    """Download a URL into dest_dir, retrying up to WGET_RETRIES times."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    if filename is None:
        filename = Path(urlparse(url).path).name
    dest_path = dest_dir / filename

    last_exc: Exception | None = None
    for attempt in range(1, WGET_RETRIES + 1):
        try:
            LOG.info(f"Downloading {url} -> {dest_path} (attempt {attempt}/{WGET_RETRIES})")
            with requests.get(url, stream=True, timeout=60) as resp:
                resp.raise_for_status()
                with open(dest_path, "wb") as f:
                    for chunk in resp.iter_content(chunk_size=8192):
                        f.write(chunk)
            return dest_path
        except Exception as exc:
            last_exc = exc
            LOG.warning(f"Download failed for {url} (attempt {attempt}/{WGET_RETRIES}): {exc}")

    LOG.error(f"Giving up on {url} after {WGET_RETRIES} attempts: {last_exc}")
    raise last_exc


# ---------------------------------------------------------------------------
# CSV writers
# ---------------------------------------------------------------------------

def _init_csvs():
    with open(METADATA_CSV, "w", newline="") as f:
        csv.writer(f).writerow(["dataset", "snapshot_dir", "size"])

    with open(QUERIES_CSV, "w", newline="") as f:
        csv.writer(f).writerow(["query_set", "for_dataset", "links"])


def _record_dataset_meta(dataset: str, snapshot_dir: str, size_mb: int):
    with open(METADATA_CSV, "a", newline="") as f:
        csv.writer(f).writerow([dataset, snapshot_dir, size_mb])


def _record_query_set(config: dict, dataset: str, qs_name: str, for_label: str):
    """Mirrors record_query_set(): 'filename; url | filename; url ...' per row."""
    parts = []
    for url in _qs_links(config, dataset, qs_name):
        fname = Path(urlparse(url).path).name
        parts.append(f"{fname}; {url}")
    links_str = " | ".join(parts)

    with open(QUERIES_CSV, "a", newline="") as f:
        csv.writer(f).writerow([qs_name, for_label, links_str])
    LOG.info(f"Recorded query set {qs_name} for {dataset}")


# ---------------------------------------------------------------------------
# Archive helpers
# ---------------------------------------------------------------------------

def _extract_tar(archive: Path, dest_dir: Path):
    dest_dir.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive, "r") as tar:
        tar.extractall(dest_dir)


def _gunzip(src: Path, dest: Path):
    with gzip.open(src, "rb") as f_in, open(dest, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)


def _dir_size_mb(path: Path) -> int:
    """Apparent size in MiB, using the same du invocation as ingest.py's du_mib so
    measurements are comparable."""
    result = subprocess.run(
        ["du", "-s", "-L", "--block-size=1M", "--apparent-size", str(path)],
        capture_output=True, text=True, check=True,
    )
    return int(result.stdout.split()[0])


# ---------------------------------------------------------------------------
# Per-dataset download
# ---------------------------------------------------------------------------

def download_datasets(config: dict):
    registered_datasets = set(config.get("datasets", {}).keys())
    LOG.info(f"Registered datasets are {sorted(registered_datasets)} ...")

    snapshot_dir_name = config.get("general", {}).get("snapshot_dir", "snapshots")
    change_sets_orig_dir_name = config.get("general", {}).get("change_sets_orig_dir", "alldata.CB.nt")

    for dataset in DATASETS:
        if dataset not in registered_datasets:
            LOG.info(
                f"Dataset {dataset} is not within the registered datasets: "
                f"{sorted(registered_datasets)} ..."
            )
            continue

        entry = config["datasets"][dataset]
        download_link_snapshots  = entry.get("download_link_snapshots", "")
        archive_name_snapshots   = entry.get("archive_name_snapshots", "")
        download_link_ng_dataset = entry.get("download_link_ng_dataset", "")
        archive_name_ng_dataset  = entry.get("archive_name_ng_dataset", "")
        download_link_changesets = entry.get("download_link_changesets", "")
        archive_name_changesets  = entry.get("archive_name_changesets", "")
        yn_nested_archives       = entry.get("yn_nested_archives", False)

        raw_ds_dir   = RUN_DIR / "rawdata" / dataset
        snapshot_dir = raw_ds_dir / snapshot_dir_name

        # tar
        LOG.info(f"Downloading {dataset} snapshots...")
        _download(download_link_snapshots, raw_ds_dir, archive_name_snapshots)

        LOG.info(f"Extracting {dataset} snapshots...")
        _extract_tar(raw_ds_dir / archive_name_snapshots, snapshot_dir)

        if yn_nested_archives:
            for gz_file in snapshot_dir.glob("*.gz"):
                _gunzip(gz_file, gz_file.with_suffix(""))
                gz_file.unlink()

        size = _dir_size_mb(snapshot_dir)
        _record_dataset_meta(dataset, snapshot_dir_name, size)

        # gz
        LOG.info(f"Downloading {dataset} named graphs dataset...")
        _download(download_link_ng_dataset, raw_ds_dir, archive_name_ng_dataset)

        LOG.info(f"Extracting {dataset} named graphs dataset...")
        _gunzip(raw_ds_dir / archive_name_ng_dataset, raw_ds_dir / "alldata.TB.nq")

        # for CB and CBNG policy: empty initial delete changeset;
        # also used to filter ORKG queries against an empty repository
        (raw_ds_dir / "empty.nt").touch()

        # Original BEAR changesets (alldata.CB.nt.tar.gz) -> rawdata/<dataset>/alldata.CB.nt/
        if download_link_changesets:
            change_set_dir = raw_ds_dir / change_sets_orig_dir_name
            LOG.info(f"Downloading {dataset} original changesets...")
            _download(download_link_changesets, raw_ds_dir, archive_name_changesets)
            LOG.info(f"Extracting {dataset} original changesets...")
            _extract_tar(raw_ds_dir / archive_name_changesets, change_set_dir)
            for gz_file in change_set_dir.glob("*.gz"):
                _gunzip(gz_file, gz_file.with_suffix(""))
                gz_file.unlink()
            size = _dir_size_mb(change_set_dir)
            LOG.info(f"Extracted {dataset} original changesets into {change_set_dir} ({size} MB).")

        LOG.info(f"Downloading and extracting {dataset} datasets finished.")


# ---------------------------------------------------------------------------
# Query set downloads
# ---------------------------------------------------------------------------

def _download_query_set(config: dict, dataset: str, qs_name: str, dest_dir: Path):
    for url in _qs_links(config, dataset, qs_name):
        _download(url, dest_dir)


def download_query_sets(config: dict):
    LOG.info("Downloading query sets for BEARA, BEARB, BEARC, and ORKG")

    for sub in [
        "beara/low", "beara/high",
        "bearb/lookup", "bearb/join",
        "bearc/complex", "orkg/complex",
    ]:
        (DOWNLOADED_QUERIES_DIR / sub).mkdir(parents=True, exist_ok=True)

    # BEARA low / high
    _download_query_set(config, "beara", "low", DOWNLOADED_QUERIES_DIR / "beara" / "low")
    _record_query_set(config, "beara", "low", "beara")

    _download_query_set(config, "beara", "high", DOWNLOADED_QUERIES_DIR / "beara" / "high")
    _record_query_set(config, "beara", "high", "beara")

    # BEARB lookup — links live under bearb_hour (same URLs as bearb_day)
    _download_query_set(config, "bearb_hour", "lookup", DOWNLOADED_QUERIES_DIR / "bearb" / "lookup")
    _record_query_set(config, "bearb_hour", "lookup", "bearb_hour")
    _record_query_set(config, "bearb_day", "lookup", "bearb_day")

    # BEARB join
    join_dir = DOWNLOADED_QUERIES_DIR / "bearb" / "join"
    _download_query_set(config, "bearb_hour", "join", join_dir)
    join_zip = join_dir / "joins.zip"
    if join_zip.exists():
        with zipfile.ZipFile(join_zip) as zf:
            zf.extractall(join_dir)
        join_zip.unlink()
    _record_query_set(config, "bearb_hour", "join", "bearb_hour")
    _record_query_set(config, "bearb_day", "join", "bearb_day")

    # BEARC complex
    _download_query_set(config, "bearc", "complex", DOWNLOADED_QUERIES_DIR / "bearc" / "complex")
    _record_query_set(config, "bearc", "complex", "bearc")

    # ORKG complex (SciQA)
    orkg_dir = DOWNLOADED_QUERIES_DIR / "orkg" / "complex"
    links = _qs_links(config, "orkg", "complex")
    if links:
        _download(links[0], orkg_dir, "SciQA-dataset.zip")
        with zipfile.ZipFile(orkg_dir / "SciQA-dataset.zip") as zf:
            zf.extractall(orkg_dir)
    _record_query_set(config, "orkg", "complex", "orkg")

    LOG.info(f"Finished downloading query sets and extracted them to {DOWNLOADED_QUERIES_DIR}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    _init_csvs()

    download_datasets(static_eval_params)
    download_query_sets(static_eval_params)