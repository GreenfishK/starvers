"""
api.py – Flask backend for the StarVers Evaluation GUI.

Endpoints:
  GET /api/runs                              → list of runs (newest first)
  GET /api/runs/<ts>                         → steps for one run
  GET /api/step-detail/<ts>/<step_name>      → rich detail data for one step
  GET /                                      → index.html

Also registered under /evaluation/starvers/... prefix to handle nginx
configurations that do not strip the path prefix before forwarding.
"""

import csv
import os
import re
import subprocess
from collections import defaultdict
from pathlib import Path
import tomli

from werkzeug.middleware.proxy_fix import ProxyFix
from flask import Flask, abort, jsonify, send_from_directory

_here = os.path.dirname(os.path.abspath(__file__))
 
app = Flask(
    __name__,
    template_folder=os.path.join(_here, 'templates'),
    static_folder=os.path.join(_here, 'static'),
)

# Trust one layer of reverse-proxy headers (nginx)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1, x_prefix=1)
app.secret_key = os.environ['FLASK_SECRET_KEY']

 
DATA_DIR   = Path(os.environ.get("DATA_DIR", "/starvers_eval/data"))
CONFIG_PATH = Path("/starvers_eval/configs/eval_setup.toml")
PORT       = int(os.environ.get("PORT", 8080))

ALL_STEPS = [
    "download", "preprocess_data", "construct_datasets",
    "ingest", "construct_queries", "evaluate", "visualize",
]

# Policy code → human-readable dataset variant name
POLICY_TO_VARIANT = {
    "ic_sr_ng": "alldata.ICNG.trig",
    "ostrich":  "first IC + change sets",
    "tb_sr_ng": "alldata.TB_computed.nq",
    "tb_sr_rs": "alldata.TB_star_hierarchical.ttl",
}

VERSIONING_APPROACH = {
    "alldata.TB_computed.nq": (
        "ex:s1 ex:p1 ex:o1 :v21_22_23_25 .\n"
        ":v21_22_23_25 owl:versionInfo \"21\" :versions .\n"
        ":v21_22_23_25 owl:versionInfo \"22\" :versions ."
    ),
    "alldata.TB_star_hierarchical.ttl": (
        "<< << s p o >> vers:valid_from t1 >> vers:valid_until t2 ."
    ),
    "alldata.ICNG.trig": (
        "GRAPH <http://starvers_eval/ic/v0> { triples }\n"
        "GRAPH <http://starvers_eval/ic/v1> { triples }"
    ),
    "first IC + change sets": "No versioning at RDF-level; versioned internally by Ostrich.",
}


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
    """Return disk usage in MiB for a file or directory, or None if it doesn't exist."""
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


# ---------------------------------------------------------------------------
# Run list and run detail
# ---------------------------------------------------------------------------

@app.get("/api/runs")
@app.get("/evaluation/starvers/api/runs")
def list_runs():
    if not DATA_DIR.exists():
        return jsonify([])
    dirs = sorted(DATA_DIR.glob("*T*"), reverse=True)
    return jsonify([_read_run(d) for d in dirs if d.is_dir()])


@app.get("/api/runs/<ts>")
@app.get("/evaluation/starvers/api/runs/<ts>")
def get_run(ts: str):
    run_dir = DATA_DIR / ts
    if not run_dir.is_dir():
        abort(404)
    return jsonify(_read_run(run_dir))


# ---------------------------------------------------------------------------
# Step detail dispatcher
# ---------------------------------------------------------------------------

@app.get("/api/step-detail/<ts>/<step_name>")
@app.get("/evaluation/starvers/api/step-detail/<ts>/<step_name>")
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
    config   = _load_config()
    datasets = config.get("datasets", {})
    detail   = {"datasets": [], "dataset_sizes": [], "query_sets": []}

    # Dataset info from eval_setup.toml
    for name, meta in datasets.items():
        detail["datasets"].append({
            "name":     name,
            "versions": meta.get("snapshot_versions", "?"),
        })

    # Average snapshot sizes from datasets_meta.csv
    meta_csv = run_dir / "output" / "logs" / "downloads" / "datasets_meta.csv"
    if meta_csv.exists():
        with open(meta_csv, newline="") as f:
            for row in csv.DictReader(f):
                detail["dataset_sizes"].append({
                    "name":        row.get("dataset", ""),
                    "avg_size_mb": float(row.get("avg_size_mb", 0)),
                })

    # Query set counts from downloaded subdirectories
    query_sets_cfg = config.get("query_sets", {})
    for qset_name in query_sets_cfg:
        # Count files across all subdirs of this query set
        qset_dir = run_dir / "queries" / "raw_queries" / qset_name
        count    = _count_txt_files(qset_dir)
        detail["query_sets"].append({"name": qset_name, "count": count})

    return detail


def _detail_preprocess(run_dir: Path) -> dict:
    detail: dict = {}

    # --- Validator versions from POM files
    validator_jar_dir = Path("/starvers_eval/scripts/2_preprocess_data/RDFValidator/target")
    rdf4j_ver, jena_ver = None, None
    if validator_jar_dir.exists():
        for pom in validator_jar_dir.rglob("*.pom"):
            name = pom.name.lower()
            m    = re.search(r"-([\d.]+)\.pom$", name)
            ver  = m.group(1) if m else "?"
            if "rdf4j" in name:
                rdf4j_ver = ver
            elif "jena" in name or "apache-jena" in name:
                jena_ver  = ver
    detail["validators"] = {"rdf4j": rdf4j_ver or "?", "jena": jena_ver or "?"}

    # --- Skolemization + invalid triples: sum across all snapshot files
    total_sub = total_obj = total_invalid = 0
    rawdata_dir = run_dir / "rawdata"
    if rawdata_dir.exists():
        for nt_file in rawdata_dir.rglob("*.nt"):
            try:
                with open(nt_file, "r", encoding="utf-8", errors="ignore") as f:
                    for _ in range(5):   # only scan first 5 lines for headers
                        line = f.readline()
                        if not line:
                            break
                        m = re.match(r"#\s*skolemized_blank_nodes_in_subject_position:\s*(\d+)", line)
                        if m:
                            total_sub += int(m.group(1))
                        m = re.match(r"#\s*skolemized_blank_nodes_in_object_position:\s*(\d+)", line)
                        if m:
                            total_obj += int(m.group(1))
                        m = re.match(r"#\s*invalid_lines_excluded:\s*(\d+)", line)
                        if m:
                            total_invalid += int(m.group(1))
            except Exception:
                continue

    detail["skolemization"] = {
        "subject": total_sub,
        "object":  total_obj,
        "invalid": total_invalid,
    }

    # --- Excluded queries from CSV
    excl_csv = run_dir / "output" / "logs" / "preprocess_data" / "excluded_queries.csv"
    if excl_csv.exists():
        counts: dict[str, int] = defaultdict(int)
        with open(excl_csv, newline="") as f:
            for row in csv.DictReader(f):
                if row.get("yn_excluded", "0") == "1":
                    counts[row.get("reason", "")] += 1
        detail["excluded_queries"] = dict(counts)

    return detail


def _detail_construct_datasets(run_dir: Path) -> dict:
    config   = _load_config()
    datasets = list(config.get("datasets", {}).keys())
    variants = []

    VARIANT_FILES = [
        ("alldata.CB_computed.nt",          "directory"),
        ("alldata.TB_computed.nq",          "file"),
        ("alldata.TB_star_hierarchical.ttl","file"),
        ("alldata.ICNG.trig",               "file"),
    ]
    POLICY_DISPLAY = {
        "first IC + change sets":            "alldata.CB_computed.nt",
        "alldata.TB_computed.nq":            "alldata.TB_computed.nq",
        "alldata.TB_star_hierarchical.ttl":  "alldata.TB_star_hierarchical.ttl",
        "alldata.ICNG.trig":                 "alldata.ICNG.trig",
    }

    for dataset in datasets:
        ds_dir = run_dir / "rawdata" / dataset

        for fname, ftype in VARIANT_FILES:
            path     = ds_dir / fname
            size_mb  = _du_mb(path)
            # Map internal filename to display name
            if fname == "alldata.CB_computed.nt":
                display = "first IC + change sets"
            else:
                display = fname

            variants.append({
                "name":                display,
                "size_mb":             size_mb,
                "versioning_approach": VERSIONING_APPROACH.get(display, "—"),
            })

    return {"variants": variants}


def _detail_ingest(run_dir: Path) -> dict:
    ingest_csv = run_dir / "output" / "measurements" / "ingestion.csv"
    if not ingest_csv.exists():
        return {}

    rows = []
    with open(ingest_csv, newline="") as f:
        for row in csv.DictReader(f, delimiter=";"):
            rows.append({
                "triplestore":           row.get("triplestore", ""),
                "policy":                row.get("policy", ""),
                "dataset":               row.get("dataset", ""),
                "ingestion_time":        row.get("ingestion_time", ""),
                "raw_file_size_mib":     row.get("raw_file_size_MiB", ""),
                "db_files_disk_usage_mib": row.get("db_files_disk_usage_MiB", ""),
            })

    return {"ingestion_table": rows}


def _detail_construct_queries(run_dir: Path) -> dict:
    final_queries_dir = run_dir / "queries" / "final_queries"
    query_counts      = {}

    POLICY_DIRS = ["ic_sr_ng", "ostrich", "tb_sr_ng", "tb_sr_rs"]
    DATASET_DIRS = ["bearb_day", "bearb_hour", "bearc", "orkg"]

    for policy in POLICY_DIRS:
        for dataset in DATASET_DIRS:
            path  = final_queries_dir / policy / dataset
            count = _count_txt_files(path)
            label = f"{POLICY_TO_VARIANT.get(policy, policy)} / {dataset}"
            query_counts[label] = count

    return {"query_counts": query_counts}


def _detail_evaluate(run_dir: Path) -> dict:
    config      = _load_config()
    evaluations = config.get("evaluations", [])
    rows        = []

    for ev in evaluations:
        # Count how many query files exist for this evaluation combination
        policy  = ev.get("policy", "")
        dataset = ev.get("dataset", "")
        qdir    = run_dir / "queries" / "final_queries" / policy / dataset
        count   = _count_txt_files(qdir)

        rows.append({
            "triplestore": ev.get("triplestore", ""),
            "policy":      policy,
            "dataset":     dataset,
            "query_count": count,
        })

    return {"evaluations": rows}


def _detail_visualize(run_dir: Path) -> dict:
    tex_path = run_dir / "output" / "tables" / "latex_table_results.tex"
    if not tex_path.exists():
        return {}
    return {"latex_table": tex_path.read_text(encoding="utf-8")}


# ---------------------------------------------------------------------------
# Static file serving
# ---------------------------------------------------------------------------

@app.get("/")
@app.get("/evaluation/starvers/")
@app.get("/evaluation/starvers")
def serve_gui():
    return send_from_directory(".", "templates/index.html")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_api():
    app.run(host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    run_api()