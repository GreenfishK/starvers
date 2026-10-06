"""
populate_queries_meta.py

Regenerate RUN_DIR/output/measurements/queries_meta.csv from eval_setup.toml
without re-downloading anything (queries are expected to already be on disk).

This is a backfill helper for runs that predate the recording of query sets
(see _record_query_set in download_data.py). Rows are written in the same
format the GUI (gui/api.py _detail_download) and write_query_counts expect:

    query_set,for_dataset,links
    low,beara,"p.txt; <url1> | q.txt; <url2> | ..."

Usage:
    RUN_DIR=/path/to/run_<ts> python3 -m scripts.1_download.populate_queries_meta
or
    RUN_DIR=/path/to/run_<ts> python3 ./1_download/populate_queries_meta.py
"""

import csv
import os
from pathlib import Path
from urllib.parse import urlparse

try:
    import tomli
except ModuleNotFoundError:  # Python 3.11+ stdlib replacement
    import tomllib as tomli


def _load_config():
    for candidate in (
        Path("/starvers_eval/configs/eval_setup.toml"),
        Path(__file__).resolve().parent.parent / "eval_setup.toml",
    ):
        if candidate.exists():
            with open(candidate, "rb") as f:
                return tomli.load(f)
    raise FileNotFoundError("eval_setup.toml not found")


def _links_str(download_links: list[str]) -> str:
    parts = []
    for url in download_links:
        fname = Path(urlparse(url).path).name
        if fname:
            parts.append(f"{fname}; {url}")
    return " | ".join(parts)


def main():
    run_dir = Path(os.environ["RUN_DIR"])
    out_csv = run_dir / "output" / "measurements" / "queries_meta.csv"
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    config = _load_config()
    rows = []
    qs_count = 0
    for ds_name, ds_meta in config.get("datasets", {}).items():
        for qs_name, qs_meta in ds_meta.get("query_sets", {}).items():
            links = _links_str(qs_meta.get("download_links", []))
            rows.append({"query_set": qs_name, "for_dataset": ds_name, "links": links})
            qs_count += 1

    rows.sort(key=lambda r: (str(r["for_dataset"]), str(r["query_set"])))
    with open(out_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["query_set", "for_dataset", "links"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Wrote {len(rows)} query sets to {out_csv}")


if __name__ == "__main__":
    main()
