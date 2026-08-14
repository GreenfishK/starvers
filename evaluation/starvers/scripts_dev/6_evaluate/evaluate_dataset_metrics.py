#!/usr/bin/env python3
"""
evaluate_dataset_metrics.py

Compute the BEAR dataset metrics (see the BEAR paper) for every dataset in the
rawdata directory (BEARB_day, BEARB_hour, BEARC, and ORKG).

This step only creates the CSV files for the dataset metrics (the pure metric
computation). The generation of the LaTeX table from these CSV files is done in
the visualize step.

Inputs (per dataset under {RUN_DIR}/rawdata/<dataset>/):
    alldata.IC.nt/*                 all snapshots
    alldata.CB_computed.nt/         our computed changesets (data-added_X-Y.nt / data-deleted_X-Y.nt)
    alldata_vdir/alldata.CB.nt/     the original changesets from the BEAR benchmark

The snapshot-based metrics (versions, triples in first/last version, mean growth,
static core, version-oblivious triples) are always derived from the
snapshots. The change metrics (mean change ratio, insertion ratio, deletion ratio)
are computed from the added/deleted deltas of the changesets:

    |Vi ∪ Vj| = |Vi| + |added|          (added = Vj \ Vi)
    change ratio  = (|added| + |deleted|) / |Vi ∪ Vj|
    insertion ratio = |added| / |Vi ∪ Vj|
    deletion ratio  = |deleted| / |Vi ∪ Vj|

The metrics are computed twice:
    1. using CHANGESET_DIR       (our computed changesets) -> dataset_metrics.csv
    2. using CHANGESET_ORIG_DIR  (original changesets)      -> dataset_metrics_orig.csv

Outputs:
    {RUN_DIR}/output/measurements/dataset_metrics.csv      (computed changesets)
    {RUN_DIR}/output/measurements/dataset_metrics_orig.csv (original changesets)
"""

import csv
import os
import re
import statistics
import sys
from pathlib import Path

SNAPSHOT_DIR = "alldata.IC.nt"
CHANGESET_DIR = "alldata.CB_computed.nt"
CHANGESET_ORIG_DIR = "alldata.CB.nt"

OUTPUT_CSV = "dataset_metrics.csv"
OUTPUT_CSV_ORIG = "dataset_metrics_orig.csv"

CSV_FIELDS = [
    "dataset",
    "versions",
    "cnt_triples_first_version",
    "cnt_triples_last_version",
    "mean_growth",
    "mean_change_ratio",
    "mean_insertion_ratio",
    "mean_deletion_ratio",
    "cnt_triples_static_core",
    "cnt_triples_version_oblivious",
]


def read_triples(path: Path) -> set:
    """Read an NTriples file and return its triples as a set of raw lines."""
    triples: set = set()
    with open(path, encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#"):
                triples.add(line)
    return triples


def version_key(path: Path) -> int:
    """Extract the numeric version id from a snapshot file name."""
    m = re.match(r"(\d+)", path.stem)
    return int(m.group(1)) if m else -1


def load_snapshots(snapshot_dir: Path) -> list[tuple[int, set]]:
    """Load all snapshot files as (version_key, triples), ordered by version."""
    files = sorted((p for p in snapshot_dir.glob("*.nt") if version_key(p) >= 0),
                   key=version_key)
    return [(version_key(p), read_triples(p)) for p in files]


def load_changesets(changeset_dir: Path) -> dict:
    """Load changesets as {(from_version, to_version): {'added': set, 'deleted': set}}."""
    deltas: dict = {}
    if not changeset_dir.is_dir():
        return deltas
    for p in changeset_dir.glob("*.nt"):
        m = re.match(r"data-(added|deleted)_(\d+)-(\d+)", p.stem)
        if not m:
            continue
        kind, a, b = m.group(1), int(m.group(2)), int(m.group(3))
        key = (a, b)
        deltas.setdefault(key, {})
        deltas[key][kind] = read_triples(p)
    return deltas


def resolve_changeset_dir(dataset_dir: Path, changeset_name: str) -> Path | None:
    """Locate a changeset directory, searching the dataset dir and alldata_vdir/."""
    candidates = [
        dataset_dir / changeset_name,
        dataset_dir / "alldata_vdir" / changeset_name,
    ]
    for c in candidates:
        if c.is_dir():
            return c
    return None


def compute_snapshot_metrics(snapshots: list[tuple[int, set]]) -> dict:
    """Snapshot-only metrics: versions, counts, growth, static core, version oblivious."""
    n = len(snapshots)
    if n == 0:
        raise ValueError("Dataset contains no snapshots.")

    first = snapshots[0][1]
    last = snapshots[-1][1]

    growths = [len(snapshots[i + 1][1]) / len(snapshots[i][1]) for i in range(n - 1)]

    static_core = set(first)
    union = set(first)
    for _, snap in snapshots[1:]:
        static_core.intersection_update(snap)
        union.update(snap)

    return {
        "versions": n,
        "cnt_triples_first_version": len(first),
        "cnt_triples_last_version": len(last),
        "mean_growth": statistics.fmean(growths) if growths else float("nan"),
        "cnt_triples_static_core": len(static_core),
        "cnt_triples_version_oblivious": len(union),
    }


def compute_change_ratio_metrics(snapshots: list[tuple[int, set]], deltas: dict) -> dict:
    """Change metrics derived from the changeset added/deleted deltas."""
    change_ratios: list[float] = []
    insertion_ratios: list[float] = []
    deletion_ratios: list[float] = []

    for i in range(len(snapshots) - 1):
        va = snapshots[i][0]
        vb = snapshots[i + 1][0]
        size_va = len(snapshots[i][1])

        delta = deltas.get((va, vb), {})
        added = len(delta.get("added", ()))
        deleted = len(delta.get("deleted", ()))

        union_size = size_va + added  # |Vi ∪ Vj| = |Vi| + |Vj \ Vi|
        change_ratios.append((added + deleted) / union_size)
        insertion_ratios.append(added / union_size)
        deletion_ratios.append(deleted / union_size)

    return {
        "mean_change_ratio": statistics.fmean(change_ratios) if change_ratios else float("nan"),
        "mean_insertion_ratio": statistics.fmean(insertion_ratios) if insertion_ratios else float("nan"),
        "mean_deletion_ratio": statistics.fmean(deletion_ratios) if deletion_ratios else float("nan"),
    }


def compute_dataset_metrics(snapshots: list[tuple[int, set]], deltas: dict) -> dict:
    metrics = compute_snapshot_metrics(snapshots)
    metrics.update(compute_change_ratio_metrics(snapshots, deltas))
    return metrics


def find_datasets(rawdata_dir: Path) -> list[Path]:
    """Return the dataset directories that contain an IC snapshot directory."""
    datasets = []
    for d in sorted(rawdata_dir.iterdir()):
        if d.is_dir() and (d / SNAPSHOT_DIR).is_dir():
            datasets.append(d)
    return datasets


def write_csv(metrics_by_dataset: dict, out_path: Path) -> None:
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        for dataset in sorted(metrics_by_dataset):
            row = {"dataset": dataset}
            row.update(metrics_by_dataset[dataset])
            writer.writerow(row)


def main() -> None:
    run_dir = Path(os.environ["RUN_DIR"])
    rawdata_dir = run_dir / "rawdata"
    if not rawdata_dir.is_dir():
        print(f"[evaluate_dataset_metrics] ERROR: rawdata directory not found: {rawdata_dir}",
              file=sys.stderr)
        sys.exit(1)

    datasets = find_datasets(rawdata_dir)
    if not datasets:
        print(f"[evaluate_dataset_metrics] ERROR: no datasets found in {rawdata_dir}", file=sys.stderr)
        sys.exit(1)

    metrics_computed: dict = {}
    metrics_orig: dict = {}

    for d in datasets:
        name = d.name
        print(f"[evaluate_dataset_metrics] Computing metrics for dataset '{name}'...", flush=True)

        snapshots = load_snapshots(d / SNAPSHOT_DIR)

        computed_delta_dir = resolve_changeset_dir(d, CHANGESET_DIR)
        orig_delta_dir = resolve_changeset_dir(d, CHANGESET_ORIG_DIR)

        deltas_computed = load_changesets(computed_delta_dir) if computed_delta_dir else {}
        deltas_orig = load_changesets(orig_delta_dir) if orig_delta_dir else {}

        metrics_computed[name] = compute_dataset_metrics(snapshots, deltas_computed)
        metrics_orig[name] = compute_dataset_metrics(snapshots, deltas_orig)

        print(f"[evaluate_dataset_metrics]   computed change ratio: {metrics_computed[name]['mean_change_ratio']:.5f} "
               f"(insertion {metrics_computed[name]['mean_insertion_ratio']:.5f}, "
               f"deletion {metrics_computed[name]['mean_deletion_ratio']:.5f})", flush=True)
        print(f"[evaluate_dataset_metrics]   orig     change ratio: {metrics_orig[name]['mean_change_ratio']:.5f} "
               f"(insertion {metrics_orig[name]['mean_insertion_ratio']:.5f}, "
               f"deletion {metrics_orig[name]['mean_deletion_ratio']:.5f})", flush=True)

    measurements_dir = run_dir / "output" / "measurements"
    measurements_dir.mkdir(parents=True, exist_ok=True)

    write_csv(metrics_computed, measurements_dir / OUTPUT_CSV)
    write_csv(metrics_orig, measurements_dir / OUTPUT_CSV_ORIG)

    print(f"[evaluate_dataset_metrics] Wrote {measurements_dir / OUTPUT_CSV}")
    print(f"[evaluate_dataset_metrics] Wrote {measurements_dir / OUTPUT_CSV_ORIG}")


if __name__ == "__main__":
    main()
