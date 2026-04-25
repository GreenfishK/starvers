"""
api.py – Flask backend for the StarVers Evaluation GUI.
"""

import base64
import csv
import math
import os
import re
import subprocess
from collections import defaultdict
from pathlib import Path

import tomli
from flask import Flask, abort, jsonify, render_template
from werkzeug.middleware.proxy_fix import ProxyFix

_here = Path(__file__).parent

app = Flask(
    __name__,
    template_folder=str(_here / 'templates'),
    static_folder=str(_here / 'static'),
)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1, x_prefix=1)

DATA_DIR    = Path(os.environ.get("DATA_DIR", "/starvers_eval/data"))
CONFIG_PATH = Path("/starvers_eval/configs/eval_setup.toml")
PORT        = int(os.environ.get("PORT", 8080))

ALL_STEPS = [
    "download", "preprocess_data", "construct_datasets",
    "ingest", "construct_queries", "evaluate", "visualize",
]

VERSIONING_APPROACH = {
    "alldata.TB_computed.nq": (
        "ex:s1 ex:p1 ex:o1 :v21_22_23_25 .\n"
        ":v21_22_23_25 owl:versionInfo \"21\" :versions .\n"
        ":v21_22_23_25 owl:versionInfo \"22\" :versions .\n"
        ":v21_22_23_25 owl:versionInfo \"23\" :versions ."
    ),
    "alldata.TB_star_hierarchical.ttl": (
        "<< << s p o >> vers:valid_from creation_timestamp >> vers:valid_until expiration_timestamp ."
    ),
    "alldata.ICNG.trig": (
        "GRAPH <http://starvers_eval/ic/v0> { triples }\n"
        "GRAPH <http://starvers_eval/ic/v1> { triples }"
    ),
    "first IC + change sets": (
        "No versioning at RDF-level. Ingested as independent copies (IC) "
        "and change sets (CB), versioned internally by Ostrich."
    ),
}

VARIANT_FILES = [
    ("alldata.CB_computed.nt",            "first IC + change sets",           True),
    ("alldata.TB_computed.nq",            "alldata.TB_computed.nq",           False),
    ("alldata.TB_star_hierarchical.ttl",  "alldata.TB_star_hierarchical.ttl", False),
    ("alldata.ICNG.trig",                 "alldata.ICNG.trig",                False),
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _read_run(run_dir: Path) -> dict:
    csv_path = run_dir / "execution.csv"
    steps = []
    if csv_path.exists():
        with open(csv_path, newline="") as f:
            steps = list(csv.DictReader(f))
    return {"ts": run_dir.name, "steps": steps}


def _load_config() -> dict:
    if not CONFIG_PATH.exists():
        return {}
    with open(CONFIG_PATH, "rb") as f:
        return tomli.load(f)


def _du_mb(path: Path) -> float | None:
    if not path.exists():
        return None
    try:
        result = subprocess.run(
            ["du", "-s", "-L", "--block-size=1M", "--apparent-size", str(path)],
            capture_output=True, text=True, timeout=30,
        )
        return float(result.stdout.split()[0])
    except Exception:
        return None


def _count_txt_files(directory: Path) -> int:
    if not directory.exists():
        return 0
    return sum(1 for f in directory.rglob("*.txt") if f.is_file())


def _discovered_datasets(run_dir: Path) -> list[str]:
    """Return dataset names that actually exist under RUN_DIR/rawdata/."""
    rawdata = run_dir / "rawdata"
    if not rawdata.exists():
        return []
    return [d.name for d in sorted(rawdata.iterdir()) if d.is_dir()]


# ---------------------------------------------------------------------------
# Run list and run detail
# ---------------------------------------------------------------------------

@app.get("/api/runs")
def list_runs():
    if not DATA_DIR.exists():
        return jsonify([])
    dirs = sorted(DATA_DIR.glob("*T*"), reverse=True)
    return jsonify([_read_run(d) for d in dirs if d.is_dir()])


@app.get("/api/runs/<ts>")
def get_run(ts: str):
    run_dir = DATA_DIR / ts
    if not run_dir.is_dir():
        abort(404)
    return jsonify(_read_run(run_dir))


# ---------------------------------------------------------------------------
# Step detail dispatcher
# ---------------------------------------------------------------------------

@app.get("/api/step-detail/<ts>/<step_name>")
def get_step_detail(ts: str, step_name: str):
    run_dir = DATA_DIR / ts
    if not run_dir.is_dir():
        abort(404)
    builders = {
        "download":           _detail_download,
        "preprocess_data":    _detail_preprocess,
        "construct_datasets": _detail_construct_datasets,
        "ingest":             _detail_ingest,
        "construct_queries":  _detail_construct_queries,
        "evaluate":           _detail_evaluate,
        "visualize":          _detail_visualize,
    }
    builder = builders.get(step_name)
    if not builder:
        abort(404)
    return jsonify(builder(run_dir))


# ---------------------------------------------------------------------------
# Step detail builders
# ---------------------------------------------------------------------------

def _detail_download(run_dir: Path) -> dict:
    config       = _load_config()
    datasets_cfg = config.get("datasets", {})
    detail       = {"datasets": [], "query_sets": []}

    # Only show datasets that actually exist on disk
    discovered = set(_discovered_datasets(run_dir))

    # Load size info from datasets_meta.csv
    sizes: dict[str, float] = {}
    meta_csv = run_dir / "output" / "logs" / "download" / "datasets_meta.csv"
    if meta_csv.exists():
        with open(meta_csv, newline="") as f:
            for row in csv.DictReader(f):
                name = row.get("dataset", "")
                try:
                    sizes[name] = float(row.get("size", 0))
                except ValueError:
                    pass

    for name, meta in datasets_cfg.items():
        if name not in discovered:
            continue
        detail["datasets"].append({
            "name":          name,
            "versions":      meta.get("snapshot_versions", "?"),
            "size_mb":       sizes.get(name),
            "download_link": meta.get("download_link_snapshots"),
        })

    # Load query set metadata from queries_meta.csv written by download.sh.
    # Columns: query_set, for_dataset, count, links
    # links format: "filename; url | filename; url | ..."
    queries_csv = run_dir / "output" / "logs" / "download" / "queries_meta.csv"
    if queries_csv.exists():
        with open(queries_csv, newline="") as f:
            for row in csv.DictReader(f):
                qs_name    = row.get("query_set", "").strip()
                for_ds     = row.get("for_dataset", "").strip()
                count_raw  = row.get("count", "0").strip()
                links_raw  = row.get("links", "").strip()

                try:
                    count = int(count_raw)
                except ValueError:
                    count = 0

                # Parse "filename; url | filename; url | ..." into list of dicts
                links = []
                if links_raw:
                    for pair in links_raw.split(" | "):
                        pair = pair.strip()
                        if "; " in pair:
                            fname, url = pair.split("; ", 1)
                            links.append({"filename": fname.strip(), "url": url.strip()})
                        elif pair:
                            links.append({"filename": pair, "url": pair})

                detail["query_sets"].append({
                    "name":        qs_name,
                    "for_dataset": for_ds,
                    "count":       count,
                    "links":       links,
                })
    else:
        # Fallback: derive from toml config (no counts available yet).
        # Links live under datasets.<name>.query_sets.<qs>.download_links
        for ds_name, ds_meta in datasets_cfg.items():
            for qs_name, qs_meta in ds_meta.get("query_sets", {}).items():
                detail["query_sets"].append({
                    "name":        qs_name,
                    "for_dataset": ds_name,
                    "count":       None,
                    "links": [
                        {"filename": lnk.rstrip("/").split("/")[-1].split("?")[0], "url": lnk}
                        for lnk in qs_meta.get("download_links", [])
                    ],
                })

    return detail


def _detail_preprocess(run_dir: Path) -> dict:
    detail: dict = {}

    # Validator versions from POM filenames
    validator_dir = Path("/starvers_eval/scripts/2_preprocess_data/RDFValidator/target")
    rdf4j_ver, jena_ver = None, None
    if validator_dir.exists():
        for pom in validator_dir.rglob("*.pom"):
            name_lower = pom.name.lower()
            m = re.search(r"-([\d.]+)\.pom$", name_lower)
            ver = m.group(1) if m else "?"
            if "rdf4j" in name_lower:
                rdf4j_ver = ver
            elif "jena" in name_lower or "apache-jena" in name_lower:
                jena_ver = ver
    detail["validators"] = {
        "rdf4j": rdf4j_ver or "not found",
        "jena":  jena_ver  or "not found",
    }

    # Skolemization per dataset
    # --- Skolemization per dataset: read from pre-computed CSV ---
    preprocess_csv = run_dir / "output" / "logs" / "preprocess_data" / "preprocess_summary.csv"
    per_dataset: dict[str, dict] = {}

    if preprocess_csv.exists():
        with open(preprocess_csv, newline="") as f:
            for row in csv.DictReader(f):
                dataset = row.get("dataset", "")
                if not dataset:
                    continue
                if dataset not in per_dataset:
                    per_dataset[dataset] = {"subject": 0, "object": 0, "invalid": 0}
                # Sum across all variants and file numbers for this dataset
                per_dataset[dataset]["subject"] += int(float(row.get("skolemized_subjects", 0) or 0))
                per_dataset[dataset]["object"]  += int(float(row.get("skolemized_objects",  0) or 0))
                per_dataset[dataset]["invalid"] += int(float(row.get("invalid_triples",     0) or 0))
    else:
        # Fallback: scan file headers (slow path, only if CSV not yet generated)
        rawdata_dir = run_dir / "rawdata"
        if rawdata_dir.exists():
            for dataset_dir in sorted(rawdata_dir.iterdir()):
                if not dataset_dir.is_dir():
                    continue
                totals = {"subject": 0, "object": 0, "invalid": 0}
                for nt_file in dataset_dir.rglob("*.nt"):
                    try:
                        with open(nt_file, "r", encoding="utf-8", errors="ignore") as f:
                            for _ in range(6):
                                line = f.readline()
                                if not line:
                                    break
                                for key, pattern in [
                                    ("subject", r"#\s*skolemized_blank_nodes_in_subject_position:\s*(\d+)"),
                                    ("object",  r"#\s*skolemized_blank_nodes_in_object_position:\s*(\d+)"),
                                    ("invalid", r"#\s*invalid_lines_excluded:\s*(\d+)"),
                                ]:
                                    mm = re.match(pattern, line)
                                    if mm:
                                        totals[key] += int(mm.group(1))
                    except Exception:
                        continue
                per_dataset[dataset_dir.name] = totals

    detail["skolemization_per_dataset"] = [
        {"dataset": ds, **vals} for ds, vals in per_dataset.items()
    ]

    # Excluded SciQA queries grouped by reason
    excl_csv = run_dir / "output" / "logs" / "preprocess_data" / "excluded_queries.csv"
    if excl_csv.exists():
        counts: dict[str, int] = defaultdict(int)
        with open(excl_csv, newline="") as f:
            for row in csv.DictReader(f):
                if row.get("yn_excluded", "0") == "1":
                    counts[row.get("reason", "(no reason)")] += 1
        detail["excluded_queries"] = dict(counts)

    return detail


def _detail_construct_datasets(run_dir: Path) -> dict:
    config    = _load_config()
    discovered = _discovered_datasets(run_dir)
    variants  = []

    for dataset in discovered:
        ds_dir = run_dir / "rawdata" / dataset
        for fname, variant_name, _ in VARIANT_FILES:
            path    = ds_dir / fname
            size_mb = _du_mb(path)
            variants.append({
                "dataset":             dataset,
                "variant":             variant_name,
                "size_mb":             size_mb,
                "versioning_approach": VERSIONING_APPROACH.get(variant_name, "—"),
            })

    return {"variants": variants}


def _detail_ingest(run_dir: Path) -> dict:
    ingest_csv = run_dir / "output" / "measurements" / "ingestion.csv"
    if not ingest_csv.exists():
        return {}

    groups: dict[tuple, list] = defaultdict(list)
    with open(ingest_csv, newline="") as f:
        for row in csv.DictReader(f, delimiter=";"):
            key = (row.get("triplestore",""), row.get("policy",""), row.get("dataset",""))
            try:
                groups[key].append((
                    float(row.get("ingestion_time", 0)),
                    float(row.get("db_files_disk_usage_MiB", 0)),
                    float(row.get("raw_file_size_MiB", 0)),
                ))
            except ValueError:
                continue

    summary = []
    for (triplestore, policy, dataset), values in sorted(groups.items()):
        summary.append({
            "triplestore":        triplestore,
            "policy":             policy,
            "dataset":            dataset,
            "avg_ingestion_time": sum(v[0] for v in values) / len(values),
            "avg_db_size_mib":    sum(v[1] for v in values) / len(values),
            "avg_raw_size_mib":   sum(v[2] for v in values) / len(values),
        })

    return {"ingestion_summary": summary}


def _detail_construct_queries(run_dir: Path) -> dict:
    # Load the file with the query counts from
    query_counts_path = run_dir / "output" / "logs" / "construct_queries" / "query_counts.csv"

    # Create a dict with key = "policy / dataset / query_set" and value = count
    query_counts: dict[str, int] = {}
    if query_counts_path.exists():
        with open(query_counts_path, newline="") as f:
            for row in csv.DictReader(f):
                policy = row.get("policy", "")
                dataset = row.get("dataset", "")
                query_set = row.get("query_set", "")
                try:
                    count = int(row.get("query_count", 0))
                except ValueError:
                    count = 0
                label = f"{policy} / {dataset} / {query_set}"
                query_counts[label] = count

    # Total per dataset, calculated from query_counts dict
    totals = defaultdict(int)
    for label, count in query_counts.items():
        parts = label.split(" / ")
        if len(parts) == 3:
            _, dataset, _ = parts
            totals[dataset] += count

    POLICY_DIRS  = ["ic_sr_ng", "ostrich", "tb_sr_ng", "tb_sr_rs"]
    DATASET_DIRS = ["bearb_day", "bearb_hour", "bearc", "orkg"]

    return {"query_counts": query_counts, "totals_per_dataset": totals, "policies": POLICY_DIRS, "datasets": DATASET_DIRS}


def _detail_evaluate(run_dir: Path) -> dict:
    config   = _load_config()
    eval_cfg = config.get("evaluations", {})
    rows     = []

    for triplestore, datasets in eval_cfg.items():
        if not isinstance(datasets, dict):
            continue
        for dataset, policies in datasets.items():
            if not isinstance(policies, list):
                continue
            total_queries = sum(
                _count_txt_files(run_dir / "queries" / "final_queries" / p / dataset)
                for p in policies
            )
            rows.append({
                "triplestore":  triplestore,
                "dataset":      dataset,
                "policies":     policies,
                "query_count":  total_queries,
            })

    return {"evaluations": rows}


def _detail_visualize(run_dir: Path) -> dict:
    """Return base64-encoded PNG plots from the figures directory."""
    figures_dir = run_dir / "output" / "figures"
    if not figures_dir.exists():
        return {"plots": []}

    plots = []
    for png in sorted(figures_dir.glob("*.png")):
        try:
            data = base64.b64encode(png.read_bytes()).decode("ascii")
            plots.append({"filename": png.name, "data": data})
        except Exception:
            continue

    return {"plots": plots}


# ---------------------------------------------------------------------------
# Serve GUI
# ---------------------------------------------------------------------------

@app.get("/")
def serve_gui():
    return render_template("index.html")


def run_api():
    app.run(host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    run_api()