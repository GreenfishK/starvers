"""
api.py – Flask backend for the StarVers Evaluation GUI.
"""

import base64
import csv
import os
import re
import subprocess
from collections import defaultdict
from pathlib import Path
import xml.etree.ElementTree as ET
import logging
import sys

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

logging.basicConfig(handlers=[logging.StreamHandler(sys.stdout)],
                    format="%(asctime)s %(name)s:%(levelname)s:%(message)s", 
                    datefmt="%F %A %T", 
                    level=logging.INFO)

DATA_DIR    = Path(os.environ.get("DATA_DIR", "/starvers_eval/data"))
CONFIG_PATH = Path("/starvers_eval/configs/eval_setup.toml")
PORT        = int(os.environ.get("PORT", 8080))


VERSIONING_APPROACH = {
    "alldata.TB_computed.nq": (
        "ex:s1 ex:p1 ex:o1 :v21_22_23_25 .\n"
        ":v21_22_23_25 owl:versionInfo \"21\" :versions .\n"
        ":v21_22_23_25 owl:versionInfo \"22\" :versions .\n"
        ":v21_22_23_25 owl:versionInfo \"23\" :versions .\n"
        ":v21_22_23_25 owl:versionInfo \"25\" :versions ."

    ),
    "alldata.TB_star_hierarchical.ttl": (
        "<< << s p o >> vers:valid_from creation_timestamp >> vers:valid_until expiration_timestamp ."
    ),
    "alldata.ICNG.trig": (
        "GRAPH <http://starvers_eval/ic/v0> { triples from v0 }\n"
        "GRAPH <http://starvers_eval/ic/v1> { triples from v1 }\n"
        "...\n"
        "GRAPH <http://starvers_eval/ic/<last_version> { triples from <last_snapshot> }\n"
    ),
    "Base variant: first IC + change sets": (
        "No versioning at RDF-level. Ingested as independent copies (IC) "
        "and change sets (CB) into Ostrich and versioned internally by Ostrich."
    ),
}

VARIANT_FILES = [
    ("alldata_vdir",            "Base variant: first IC + change sets",           True),
    ("alldata.TB_computed.nq",            "alldata.TB_computed.nq",           False),
    ("alldata.TB_star_hierarchical.ttl",  "alldata.TB_star_hierarchical.ttl", False),
    ("alldata.ICNG.trig",                 "alldata.ICNG.trig",                False),
]

DATASET_DESCRIPTIONS = {
    "bearb_day": (
        "Original description from the BEAR webpage (https://aic.ai.wu.ac.at/qadlod/bear.html): Compiled from DBpedia Live changesets over three months, containing the 100 most volatile "
        "resources with their updates and real-world triple pattern queries from user logs. "
        "Every snapshot represents one day."
    ),
    "bearb_hour": (
        "Original description from the BEAR webpage (https://aic.ai.wu.ac.at/qadlod/bear.html): Compiled from DBpedia Live changesets over three months, containing the 100 most volatile "
        "resources with their updates and real-world triple pattern queries from user logs. "
        "Every snapshot represents one hour."
    ),
    "bearc": (
        "Original description from the BEAR webpage (https://aic.ai.wu.ac.at/qadlod/bear.html): Uses the Open Data Portal Watch project to capture dataset descriptions of the European "
        "Open Data portal over 32 weeks."
    ),
    "orkg": (
        "Compiled over 34 weeks by downloading one snapshot each week via ORKG's data access API."
    ),
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
    rawdata = run_dir / "rawdata"
    if not rawdata.exists():
        return []
    return [d.name for d in sorted(rawdata.iterdir()) if d.is_dir()]


def _load_svg_plots(figures_dir: Path, prefix: str) -> list[dict]:
    if not figures_dir.exists():
        return []
    plots = []
    for svg in sorted(figures_dir.glob(f"{prefix}*.svg")):
        try:
            # Embed raw SVG text — no base64 needed, renders inline directly
            data = svg.read_text(encoding="utf-8")
            plots.append({"filename": svg.name, "data": data})
        except Exception:
            continue
    return plots

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
    detail       = {"datasets": []}
    discovered   = set(_discovered_datasets(run_dir))

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

    # Parse query sets, keyed by for_dataset
    query_sets_by_dataset: dict[str, list] = {}
    queries_csv = run_dir / "output" / "logs" / "download" / "queries_meta.csv"
    if queries_csv.exists():
        with open(queries_csv, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                qs_name   = row.get("query_set", "").strip()
                for_ds    = row.get("for_dataset", "").strip()
                links_raw = row.get("links", "").strip().strip("\r")

                links = []
                if links_raw:
                    for pair in links_raw.split(" | "):
                        pair = pair.strip()
                        if "; " in pair:
                            fname, url = pair.split("; ", 1)
                            links.append({"filename": fname.strip(), "url": url.strip()})
                        elif pair:
                            links.append({"filename": pair, "url": pair})

                query_sets_by_dataset.setdefault(for_ds, []).append({
                    "name":  qs_name,
                    "links": links,
                })

    else:
        # Fallback from toml
        for ds_name, ds_meta in datasets_cfg.items():
            for qs_name, qs_meta in ds_meta.get("query_sets", {}).items():
                query_sets_by_dataset.setdefault(ds_name, []).append({
                    "name": qs_name,
                    "links": [
                        {"filename": lnk.rstrip("/").split("/")[-1].split("?")[0], "url": lnk}
                        for lnk in qs_meta.get("download_links", [])
                    ],
                })

    for name, meta in datasets_cfg.items():
        if name not in discovered:
            continue
        detail["datasets"].append({
            "name":          name,
            "description":   DATASET_DESCRIPTIONS.get(name, ""),
            "versions":      meta.get("snapshot_versions", "?"),
            "size_mb":       sizes.get(name),
            "download_link": meta.get("download_link_snapshots"),
            "query_sets":    query_sets_by_dataset.get(name, []),
        })

    
    return detail


def _detail_preprocess(run_dir: Path) -> dict:
    detail: dict = {}

    pom_path = Path("/starvers_eval/scripts/2_preprocess_data/RDFValidator/pom.xml")

    rdf4j_ver, jena_ver = None, None

    if pom_path.exists():
        tree = ET.parse(pom_path)
        root = tree.getroot()

        # Maven uses namespaces → we must handle them
        ns = {"m": "http://maven.apache.org/POM/4.0.0"}

        # --- RDF4J version ---
        for dep in root.findall(".//m:dependency", ns):
            group = dep.find("m:groupId", ns)
            artifact = dep.find("m:artifactId", ns)
            version = dep.find("m:version", ns)

            if group is None or artifact is None or version is None:
                continue

            group_text = group.text or ""
            artifact_text = artifact.text or ""
            version_text = version.text or ""

            if "rdf4j" in group_text or "rdf4j" in artifact_text:
                rdf4j_ver = version_text

            if "jena" in group_text or "jena" in artifact_text:
                jena_ver = version_text

    detail["validators"] = {
        "rdf4j": rdf4j_ver or "not found",
        "jena":  jena_ver  or "not found",
    }

    preprocess_csv = run_dir / "output" / "logs" / "preprocess_data" / "preprocess_summary.csv"
    per_dataset: dict[str, dict] = {}

    if preprocess_csv.exists():
        with open(preprocess_csv, newline="") as f:
            for row in csv.DictReader(f):
                dataset = row.get("dataset", "")
                variant = row.get("variant", "")

                if variant != "ic":
                    continue
                if not dataset:
                    continue
                
                if dataset not in per_dataset:
                    per_dataset[dataset] = {"subject": 0, "object": 0, "invalid": 0, "file_count": 0}
                per_dataset[dataset]["subject"] += int(float(row.get("skolemized_subjects", 0) or 0))
                per_dataset[dataset]["object"]  += int(float(row.get("skolemized_objects",  0) or 0))
                per_dataset[dataset]["invalid"] += int(float(row.get("invalid_triples",     0) or 0))
                per_dataset[dataset]["file_count"] += 1

    else:
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
        {
            "dataset":     ds,
            "subject":     vals["subject"],
            "object":      vals["object"],
            "invalid":     vals["invalid"],
            "invalid_avg": round(vals["invalid"] / vals["file_count"], 2) if vals.get("file_count") else 0,
        }
        for ds, vals in per_dataset.items()
    ]

    # Build a per-query pivot table from excluded_queries.csv.
    # Each row in the CSV records one query + one exclusion reason.
    # We pivot so each query becomes one row with a flag per reason type.
    excl_csv = run_dir / "output" / "logs" / "preprocess_data" / "excluded_queries.csv"
    if excl_csv.exists():
        pivot: dict[str, dict] = {}
        all_rows = []
        with open(excl_csv, newline="") as f:
            for row in csv.DictReader(f):
                name   = row.get("query", "").strip()
                reason = row.get("reason", "").strip()
                excl   = int(row.get("yn_excluded", "0") or 0)
                if not name:
                    continue
                # Only SELECT queries — skip ASK
                if reason == "ASK":
                    continue
                all_rows.append((name, reason, excl))
                if name not in pivot:
                    pivot[name] = {
                        "invalid_in_graphdb":    0,
                        "malformed_graphdb":     0,
                        "invalid_in_jena":       0,
                        "malformed_jena":        0,
                        "invalid_in_ostrich":    0,
                    }
                if reason == "Invalid Original in GraphDB":
                    pivot[name]["invalid_in_graphdb"] = excl
                elif reason == "Invalid Original in Jena TDB2":
                    pivot[name]["invalid_in_jena"] = excl
                elif reason == "Malformed Starvers transformation (GraphDB)":
                    pivot[name]["malformed_graphdb"] = excl
                elif reason == "Malformed Starvers transformation (Jena)":
                    pivot[name]["malformed_jena"] = excl
                elif reason == "Invalid Original in Ostrich":
                    pivot[name]["invalid_in_ostrich"] = excl

        total_queries = len(pivot)
        counts_by_col = {
            "valid_in_graphdb": total_queries - sum(1 for f in pivot.values() if f["invalid_in_graphdb"]),
            "valid_trans_in_graphdb": total_queries - sum(1 for f in pivot.values() if f["invalid_in_graphdb"]) - sum(1 for f in pivot.values() if f["malformed_graphdb"] and not f["invalid_in_graphdb"]),
            "valid_in_jena":   total_queries - sum(1 for f in pivot.values() if f["invalid_in_jena"]),
            "valid_trans_in_jena": total_queries - sum(1 for f in pivot.values() if f["invalid_in_jena"]) -  sum(1 for f in pivot.values() if f["malformed_jena"] and not f["invalid_in_jena"]),
            "valid_in_ostrich": total_queries - sum(1 for f in pivot.values() if f["invalid_in_ostrich"]),
        }

        detail["sciqa_query_table"] = sorted(
            [
                {
                    "query":    name,
                    "excluded": any(v == 1 for v in flags.values()),
                    **flags,
                }
                for name, flags in pivot.items()
            ],
            key=lambda r: r["query"],
        )
        detail["sciqa_col_counts"]  = counts_by_col
        detail["sciqa_total"]       = total_queries

    return detail


def _detail_construct_datasets(run_dir: Path) -> dict:
    discovered = _discovered_datasets(run_dir)
    variants   = []

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
    """
    Returns ingestion summary stats plus ingest_ and storage_ SVG plots.
    Plots are returned as base64-encoded SVG strings.
    If the figures directory or any plot file does not exist yet (visualize step
    has not run), the lists are simply empty — no error is raised.
    """
    ingest_csv = run_dir / "output" / "measurements" / "ingestion.csv"

    # Tabular summary from CSV 
    summary = []
    if ingest_csv.exists():
        groups: dict[tuple, list] = defaultdict(list)
        with open(ingest_csv, newline="") as f:
            for row in csv.DictReader(f, delimiter=";"):
                key = (row.get("triplestore", ""), row.get("policy", ""), row.get("dataset", ""))
                try:
                    groups[key].append((
                        float(row.get("ingestion_time", 0)),
                        float(row.get("db_files_disk_usage_MiB", 0)),
                        float(row.get("raw_file_size_MiB", 0)),
                    ))
                except ValueError:
                    continue

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
    query_counts_path = run_dir / "output" / "logs" / "construct_queries" / "query_counts.csv"

    # nested: counts[policy][dataset] = total across all query sets
    counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    policies_found: set[str] = set()
    datasets_found: set[str] = set()

    if query_counts_path.exists():
        with open(query_counts_path, newline="") as f:
            for row in csv.DictReader(f):
                policy  = row.get("policy", "").strip()
                dataset = row.get("dataset", "").strip()
                try:
                    count = int(row.get("query_count", 0))
                except ValueError:
                    count = 0
                counts[policy][dataset] += count
                policies_found.add(policy)
                datasets_found.add(dataset)

    # Preserve a sensible display order; fall back to sorted if not all present
    POLICY_ORDER  = ["ic_sr_ng", "ostrich", "ostrich_aggchange", "tb_sr_ng", "tb_sr_rs"]
    DATASET_ORDER = ["bearb_day", "bearb_hour", "bearc", "orkg"]

    policies = [p for p in POLICY_ORDER if p in policies_found] + \
               sorted(policies_found - set(POLICY_ORDER))
    datasets = [d for d in DATASET_ORDER if d in datasets_found] + \
               sorted(datasets_found - set(DATASET_ORDER))

    # Serialise defaultdicts to plain dicts for JSON
    query_counts = {p: dict(ds_map) for p, ds_map in counts.items()}

    totals_per_dataset = {
        ds: sum(query_counts[p].get(ds, 0) for p in policies)
        for ds in datasets
    }

    return {
        "query_counts":       query_counts,
        "totals_per_dataset": totals_per_dataset,
        "policies":           policies,
        "datasets":           datasets,
    }


def _detail_evaluate(run_dir: Path) -> dict:
    config   = _load_config()
    eval_cfg = config.get("evaluations", {})
    time_csv  = run_dir / "output" / "measurements" / "time.csv"

    # Build the ordered list of (triple_store, policy, dataset) combinations
    # exactly as main() iterates them — product(triple_stores, policies, datasets)
    # filtered by eval_combi_exists
    combinations = []
    for triplestore, datasets_cfg in eval_cfg.items():
        if not isinstance(datasets_cfg, dict):
            continue
        for dataset, policies in datasets_cfg.items():
            if not isinstance(policies, list):
                continue
            for policy in policies:
                total_queries = _count_txt_files(
                    run_dir / "queries" / "final_queries" / policy / dataset
                )
                combinations.append({
                    "triplestore":   triplestore,
                    "policy":        policy,
                    "dataset":       dataset,
                    "query_count":   total_queries,
                })

    # Sample rows from time.csv
    time_header = []
    time_samples = []
    time_total_rows = 0
    if time_csv.exists():
        with open(time_csv, newline="") as f:
            reader = csv.reader(f, delimiter=";")
            for i, row in enumerate(reader):
                if i == 0:
                    time_header = row
                elif i <= 5:
                    time_samples.append(row)
                else:
                    time_total_rows += 1
        time_total_rows += len(time_samples)

    return {
        "combinations":   combinations,
        "time_header":    time_header,
        "time_samples":   time_samples,
        "time_total_rows": time_total_rows,
    }


def _detail_visualize(run_dir: Path) -> dict:
    """
    Build per-dataset/query-set time series data for the GUI to plot.
    Mirrors create_latex_tables(): merges time.csv with query_rewriting_times.csv,
    clips timeouts to 30s, and adds rewriting_time for tb_sr_rs.
    """
    time_csv  = run_dir / "output" / "measurements" / "time.csv"
    rewrite_csv = run_dir / "output" / "measurements" / "query_rewriting_times.csv"

    if not time_csv.exists():
        return {"plot_data": [], "error": "time.csv not found"}

    rows = []
    with open(time_csv, newline="") as f:
        for row in csv.DictReader(f, delimiter=";"):
            try:
                exec_time = float(row.get("execution_time", -1) or -1)
                yn_timeout = int(float(row.get("yn_timeout", 0) or 0))
                rewrite_time = 0.0

                # Clip: treat timeouts and values >=30 as 30s
                if exec_time >= 30 or yn_timeout:
                    exec_time_clean = 30.0
                    yn_timeout = 1
                elif exec_time < 0:
                    exec_time_clean = -1.0
                else:
                    exec_time_clean = exec_time

                rows.append({
                    "triplestore":  row.get("triplestore", "").strip(),
                    "dataset":      row.get("dataset", "").strip(),
                    "policy":       row.get("policy", "").strip(),
                    "query_set":    row.get("query_set", "").strip(),
                    "snapshot":     row.get("snapshot", "").strip(),
                    "query":        row.get("query", "").strip(),
                    "exec_time":    exec_time_clean,
                    "yn_timeout":   yn_timeout,
                    "rewrite_time": 0.0,
                })
            except (ValueError, TypeError):
                continue

    # Merge rewriting times for tb_sr_rs
    rewrite_map: dict[tuple, float] = {}
    if rewrite_csv.exists():
        with open(rewrite_csv, newline="") as f:
            for row in csv.DictReader(f, delimiter=","):
                try:
                    key = (
                        row.get("dataset", "").strip(),
                        row.get("policy", "").strip(),
                        row.get("query_set", "").strip(),
                        row.get("snapshot", "").strip(),
                        row.get("query", "").strip(),
                    )
                    rewrite_map[key] = float(row.get("rewriting_time", 0) or 0)
                except (ValueError, TypeError):
                    continue

    for r in rows:
        key = (r["dataset"], r["policy"], r["query_set"], r["snapshot"], r["query"])
        rt = rewrite_map.get(key, 0.0)
        r["rewrite_time"] = rt
        if r["exec_time"] >= 0:
            r["total_time"] = min(r["exec_time"] + rt, 30.0)
        else:
            r["total_time"] = r["exec_time"]

    # Aggregate: mean total_time per (triplestore, policy, dataset, query_set, snapshot)
    # snapshot is the version number (integer)
    from collections import defaultdict
    agg: dict[tuple, list] = defaultdict(list)
    for r in rows:
        if r["total_time"] < 0:
            continue
        key = (r["triplestore"], r["policy"], r["dataset"], r["query_set"], r["snapshot"])
        agg[key].append(r["total_time"])

    # Build plot_data: list of series, each with metadata and (version, avg_time) points
    series_map: dict[tuple, dict] = {}
    for (ts, policy, dataset, query_set, snapshot), times in agg.items():
        series_key = (ts, policy, dataset, query_set)
        if series_key not in series_map:
            series_map[series_key] = {
                "triplestore": ts,
                "policy":      policy,
                "dataset":     dataset,
                "query_set":   query_set,
                "points":      {},
            }
        try:
            version = int(snapshot)
        except (ValueError, TypeError):
            version = snapshot
        series_map[series_key]["points"][version] = sum(times) / len(times)

    # Convert points dict to sorted list of [version, avg_time]
    plot_series = []
    for s in series_map.values():
        s["points"] = sorted(s["points"].items())
        plot_series.append(s)

    return {"plot_data": plot_series}

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