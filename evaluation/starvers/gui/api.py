"""
api.py – Flask backend for the StarVers Evaluation GUI.

Endpoints:
  GET /api/runs                          → list of runs
  GET /api/runs/<ts>                     → steps for one run
  GET /api/step-detail/<ts>/<step_name>  → rich detail for one step
  GET /                                  → index.html (rendered via Jinja)
"""

import csv
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
        "GRAPH <http://starvers_eval/ic/v1> { triples }\n"
        " ..."
    ),
    "first IC + change sets": (
        "No versioning at RDF-level. Ingested as independent copies (IC) "
        "and change sets (CB), versioned internally by Ostrich."
    ),
}

VARIANT_FILES = [
    ("alldata.CB_computed.nt",           "first IC + change sets",          True),   # is directory
    ("alldata.TB_computed.nq",           "alldata.TB_computed.nq",          False),
    ("alldata.TB_star_hierarchical.ttl", "alldata.TB_star_hierarchical.ttl",False),
    ("alldata.ICNG.trig",                "alldata.ICNG.trig",               False),
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


def _parse_latex_table(tex: str) -> dict | None:
    """
    Parse a LaTeX tabular environment into headers + rows.
    Returns None if parsing fails.
    """
    # Find tabular content
    m = re.search(r'\\begin\{tabular\}.*?\n(.*?)\\end\{tabular\}', tex, re.DOTALL)
    if not m:
        return None
    body = m.group(1)

    rows = []
    for line in body.splitlines():
        line = line.strip()
        if not line or line.startswith('%') or line.startswith('\\hline') or line.startswith('\\toprule') \
                or line.startswith('\\midrule') or line.startswith('\\bottomrule'):
            continue
        if '\\\\' in line:
            row_str = line.split('\\\\')[0]
            cells = [_clean_latex_cell(c) for c in row_str.split('&')]
            rows.append(cells)

    if not rows:
        return None

    return {"headers": rows[0], "rows": rows[1:]}


def _clean_latex_cell(cell: str) -> str:
    cell = cell.strip()
    # Remove common LaTeX commands
    cell = re.sub(r'\\textbf\{([^}]*)\}', r'\1', cell)
    cell = re.sub(r'\\textit\{([^}]*)\}', r'\1', cell)
    cell = re.sub(r'\\multicolumn\{\d+\}\{[^}]*\}\{([^}]*)\}', r'\1', cell)
    cell = re.sub(r'\\[a-zA-Z]+', '', cell)
    cell = re.sub(r'\{|\}', '', cell)
    return cell.strip()


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
    config  = _load_config()
    datasets_cfg = config.get("datasets", {})
    detail  = {"datasets": [], "query_sets": []}

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

    # Build dataset entries with versions, size, and download link from toml
    for name, meta in datasets_cfg.items():
        detail["datasets"].append({
            "name":          name,
            "versions":      meta.get("snapshot_versions", "?"),
            "size_mb":       sizes.get(name),
            "download_link": meta.get("download_link_snapshots", None),
        })

    # Query set file counts
    query_sets_cfg = config.get("query_sets", {})
    for qset_name in query_sets_cfg:
        qset_dir = run_dir / "queries" / "raw_queries" / qset_name
        count    = _count_txt_files(qset_dir)
        detail["query_sets"].append({"name": qset_name, "count": count})

    return detail


def _detail_preprocess(run_dir: Path) -> dict:
    detail: dict = {}

    # --- Validator versions: scan POM files for version numbers
    validator_dir = Path("/starvers_eval/scripts/2_preprocess_data/RDFValidator")
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

    # --- Skolemization per dataset: scan header comments in each .nt file
    per_dataset: dict[str, dict] = {}
    rawdata_dir = run_dir / "rawdata"
    if rawdata_dir.exists():
        for dataset_dir in rawdata_dir.iterdir():
            if not dataset_dir.is_dir():
                continue
            dataset_name = dataset_dir.name
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

            if any(v > 0 for v in totals.values()):
                per_dataset[dataset_name] = totals

    detail["skolemization_per_dataset"] = [
        {"dataset": ds, **vals} for ds, vals in per_dataset.items()
    ]

    # --- Excluded queries from CSV, grouped by reason
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
    config   = _load_config()
    datasets = list(config.get("datasets", {}).keys())
    variants = []

    for dataset in datasets:
        ds_dir = run_dir / "rawdata" / dataset
        for fname, variant_name, is_dir in VARIANT_FILES:
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

    # Aggregate: group by (triplestore, policy, dataset), average ingestion_time and db_size
    from collections import defaultdict
    groups: dict[tuple, list] = defaultdict(list)

    with open(ingest_csv, newline="") as f:
        for row in csv.DictReader(f, delimiter=";"):
            key = (
                row.get("triplestore", ""),
                row.get("policy", ""),
                row.get("dataset", ""),
            )
            try:
                ingest_time = float(row.get("ingestion_time", 0))
                db_size     = float(row.get("db_files_disk_usage_MiB", 0))
                groups[key].append((ingest_time, db_size))
            except ValueError:
                continue

    summary = []
    for (triplestore, policy, dataset), values in sorted(groups.items()):
        avg_time = sum(v[0] for v in values) / len(values)
        avg_db   = sum(v[1] for v in values) / len(values)
        summary.append({
            "triplestore":        triplestore,
            "policy":             policy,
            "dataset":            dataset,
            "avg_ingestion_time": avg_time,
            "avg_db_size_mib":    avg_db,
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

    return {"query_counts": query_counts}


def _detail_evaluate(run_dir: Path) -> dict:
    config     = _load_config()
    # Structure: evaluations.<triplestore>.<dataset> = [list of policies]
    eval_cfg   = config.get("evaluations", {})
    rows       = []

    for triplestore, datasets in eval_cfg.items():
        if not isinstance(datasets, dict):
            continue
        for dataset, policies in datasets.items():
            if not isinstance(policies, list):
                continue
            # Count query files for each policy/dataset combination
            total_queries = 0
            for policy in policies:
                qdir = run_dir / "queries" / "final_queries" / policy / dataset
                total_queries += _count_txt_files(qdir)

            rows.append({
                "triplestore":  triplestore,
                "dataset":      dataset,
                "policies":     policies,
                "query_count":  total_queries,
            })

    return {"evaluations": rows}


def _detail_visualize(run_dir: Path) -> dict:
    tex_path = run_dir / "output" / "tables" / "latex_table_results.tex"
    if not tex_path.exists():
        return {}

    tex = tex_path.read_text(encoding="utf-8")
    parsed = _parse_latex_table(tex)

    if parsed:
        return {"result_table": parsed}
    else:
        # Fallback: show raw LaTeX
        return {"latex_raw": tex}


# ---------------------------------------------------------------------------
# Static serving
# ---------------------------------------------------------------------------

@app.get("/")
def serve_gui():
    return render_template("index.html")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def run_api():
    app.run(host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    run_api()