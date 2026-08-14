"""
api.py – Flask backend for the StarVers Evaluation GUI.
Returns only data structures — no HTML, no CSS, no SVG markup.
Charts are rendered via matplotlib as base64 PNG.
"""

import base64
import csv
import io
import math
import os
import re
import subprocess
from collections import defaultdict
from pathlib import Path
import xml.etree.ElementTree as ET
import logging
import sys
import platform
import matplotlib.ticker as ticker
from adjustText import adjust_text

import tomli
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

from flask import Flask, abort, jsonify, render_template
from werkzeug.middleware.proxy_fix import ProxyFix

_here = Path(__file__).parent

app = Flask(
    __name__,
    template_folder=str(_here / "templates"),
    static_folder=str(_here / "static"),
)
app.wsgi_app = ProxyFix(
    app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1, x_prefix=1
)

logging.basicConfig(
    handlers=[logging.StreamHandler(sys.stdout)],
    format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
    datefmt="%F %A %T",
    level=logging.INFO,
)

DATA_DIR = Path(os.environ.get("DATA_DIR", "/starvers_eval/data"))
CONFIG_PATH = Path("/starvers_eval/configs/eval_setup.toml")
PORT = int(os.environ.get("PORT", 8080))

ALL_STEPS = [
    "download",
    "preprocess_data",
    "construct_datasets",
    "ingest",
    "construct_queries",
    "evaluate",
    "visualize",
]

STEP_DESCRIPTIONS = {
    "download": (
        "The datasets and query sets are fetched from the provided URLs and the number "
        "of snapshots (versions) and total snapshot size are computed from the source "
        "files and displayed below."
    ),
    "preprocess_data": (
        "The snapshot files of the datasets and the query sets are preprocessed in "
        "different ways. The dataset triples are skolemized and validated for RDF "
        "compliance using two different RDF validators. The queries of the SciQA "
        "dataset are parsed and validated by querying them against the three evaluated "
        "triple stores. The queries are also transformed into the timestamped-based "
        "representation and also executed against the triple stores. If a query is "
        "invalid in at least one of the triple stores in either original or timestamped "
        "form, it is excluded from the evaluation."
    ),
    "construct_datasets": (
        "Four different dataset variants are constructed from the snapshot files. Three "
        "of them use a certain RDF-based versioning approach and the fourth one is a "
        "simple collection of the first snapshots and the consecutive deltas/change "
        "sets, which are ingested and internally versioned by the Ostrich store."
    ),
    "ingest": (
        "Each dataset variant that applys versioning on RDF level is ingested into the "
        "two evaluated RDF-star triple stores, whereas the first snapshot and changesets "
        "variant is ingested into the Ostrich store. The total ingestion time is "
        "measured for one run only. The size of the ingested data is also measured and "
        "displayed below."
    ),
    "construct_queries": (
        "Each dataset variant has their own query form. A query is constructed from a "
        "query template for each dataset, dataset variant (versioning policy), and "
        "version. The table below shows how many queries are generated and executed in "
        "the next step."
    ),
    "evaluate": "The evaluation loop for the query execution is shown below.",
    "visualize": (
        "For each dataset and query set a line is plotted showing the query execution "
        "time over the versions for each dataset variant (versioning policy) and triple "
        "store combination."
    ),
}

VERSIONING_APPROACH = {
    "alldata.TB_computed.nq (BEAR approach)": (
        ":s :p :o :v21_22_23_25 .\n"
        ':v21_22_23_25 owl:versionInfo "21" :versions .\n'
        ':v21_22_23_25 owl:versionInfo "22" :versions .\n'
        ':v21_22_23_25 owl:versionInfo "23" :versions .\n'
        ':v21_22_23_25 owl:versionInfo "25" :versions .'
    ),
    "alldata.TB_star_hierarchical.ttl (RDF-star decorator model)": (
        "<< << :s :p :o >> :valid_from '2025-01-05T01:56:30' >> :valid_until '2025-01-30T02:33:11' .\n"
        "<< << :s :p :o >> :valid_from '2025-03-04T12:31:05' >> :valid_until '9999-12-31T23:59:59' ."
    ),
    "alldata.TB_star_reif.ttl (RDF-star reification model)": (
        "_:b1 rdf:reifies << :s :p :o >> ; :valid_from '2025-01-05T01:56:30' ; :valid_until '2025-01-30T02:33:11' .\n"
        "_:b2 rdf:reifies << :s :p :o >> ; :valid_from '2025-03-04T12:31:05' ; :valid_until '9999-12-31T23:59:59' ."
    ),
    "alldata.ICNG.trig (snapshot-based approach, each snapshot in a named graph)": (
        "GRAPH <http://starvers_eval/ic/v21> { :s :p :o . }\n"
        "GRAPH <http://starvers_eval/ic/v22> { :s :p :o . }\n"
        "GRAPH <http://starvers_eval/ic/v23> { :s :p :o . }\n"
        "GRAPH <http://starvers_eval/ic/v24> { }\n"
        "GRAPH <http://starvers_eval/ic/v25> { :s :p :o . }\n"
    ),
    "Base variant: first IC + change sets": (
        "No versioning at RDF-level. Ingested as independent copies (IC) "
        "and change sets (CB) into Ostrich and versioned internally by Ostrich."
    ),
}

VARIANT_FILES = [
    ("alldata_vdir", "Base variant: first IC + change sets", True),
    ("alldata.TB_computed.nq", "alldata.TB_computed.nq (BEAR approach)", False),
    (
        "alldata.TB_star_hierarchical.ttl",
        "alldata.TB_star_hierarchical.ttl (RDF-star decorator model)",
        False,
    ),
    (
        "alldata.TB_star_reif.ttl",
        "alldata.TB_star_reif.ttl (RDF-star reification model)",
        False,
    ),
    (
        "alldata.ICNG.trig",
        "alldata.ICNG.trig (snapshot-based approach, each snapshot in a named graph)",
        False,
    ),
]

DATASET_DESCRIPTIONS = {
    "bearb_day": (
        "Original description from the BEAR webpage (https://aic.ai.wu.ac.at/qadlod/bear.html): "
        "Compiled from DBpedia Live changesets over three months, containing the 100 most volatile "
        "resources with their updates and real-world triple pattern queries from user logs. "
        "Every snapshot represents one day."
    ),
    "bearb_hour": (
        "Original description from the BEAR webpage (https://aic.ai.wu.ac.at/qadlod/bear.html): "
        "Compiled from DBpedia Live changesets over three months, containing the 100 most volatile "
        "resources with their updates and real-world triple pattern queries from user logs. "
        "Every snapshot represents one hour."
    ),
    "bearc": (
        "Original description from the BEAR webpage (https://aic.ai.wu.ac.at/qadlod/bear.html): "
        "Uses the Open Data Portal Watch project to capture dataset descriptions of the European "
        "Open Data portal over 32 weeks."
    ),
    "orkg": (
        "Compiled over 34 weeks by downloading one snapshot each week via ORKG's data access API."
    ),
}

POLICY_COLORS = {
    "ic_sr_ng": "#006699",
    "cb_sr_ng": "#007E71",
    "tb_sr_ng": "#E18922",
    "tb_sr_rs": "#BA4682",
    "tb_sr_re": "#9D9D9C",
    "ostrich": "#5485AB",
    "ostrich_aggchange": "#000000",
}
POLICY_COLOR_FALLBACKS = ["#646363", "#6AAAA5", "#EEB473", "#CD81A8"]

TS_COLORS_CHART = {
    "graphdb": "#006699",
    "jenatdb2": "#E18922",
    "ostrich": "#BA4682",
    "ostrich_aggchange": "#007E71",
}
TS_COLOR_FALLBACKS = ["#000000", "#646363", "#5485AB", "#6AAAA5", "#EEB473"]

# Store definitions used for query flow classification
QF_STORES_ALL = [
    {"id": "graphdb", "color": "#185FA5", "label": "GraphDB"},
    {"id": "jena", "color": "#BA7517", "label": "Jena TDB2"},
    {"id": "ostrich", "color": "#9B3DB8", "label": "Ostrich"},
]
QF_STORES_TS = [s for s in QF_STORES_ALL if s["id"] != "ostrich"]


# ── Helpers ────────────────────────────────────────────────────


def _fmt(n):
    if n is None:
        return "\u2014"
    return f"{int(n):,}"


def _fmt_mb(mb):
    if mb is None:
        return "\u2014"
    return f"{mb:,.1f} MiB"


def _fmt_sec(t):
    if t is None:
        return "\u2014"
    if t >= 3600:
        return f"{t / 3600:.2f}h"
    if t >= 60:
        return f"{t / 60:.1f}m"
    if t >= 1:
        return f"{t:.3f}s"
    return f"{t * 1000:.1f}ms"


def _get_hardware_info() -> dict:
    cpu = platform.processor() or "Unknown"
    try:
        out = subprocess.check_output(["lscpu"], text=True, stderr=subprocess.DEVNULL)
        for line in out.splitlines():
            if line.startswith("Model name"):
                cpu = line.split(":", 1)[1].strip()
                break
    except Exception:
        pass

    fs_type = "Unknown"
    try:
        with open("/proc/mounts") as f:
            mounts = f.readlines()
        skip = {
            "overlay", "tmpfs", "proc", "sysfs", "devtmpfs", "devpts",
            "cgroup", "cgroup2", "mqueue", "hugetlbfs", "pstore",
            "securityfs", "debugfs", "fusectl",
        }
        for line in mounts:
            parts = line.split()
            if len(parts) >= 3 and parts[1] == "/" and parts[2] not in skip:
                fs_type = parts[2]
                break
        if fs_type == "Unknown":
            for line in mounts:
                parts = line.split()
                if (
                    len(parts) >= 3
                    and parts[2] not in skip
                    and parts[0].startswith("/dev")
                ):
                    fs_type = parts[2]
                    break
    except Exception:
        pass

    disk_name = "Unknown"
    disk_type = "Unknown"
    try:
        out = subprocess.check_output(
            ["lsblk", "-d", "-o", "NAME,ROTA,MODEL", "--noheadings"],
            text=True,
            stderr=subprocess.DEVNULL,
        )
        for line in out.splitlines():
            parts = line.split(None, 2)
            if len(parts) >= 2:
                disk_name = parts[2].strip() if len(parts) == 3 else parts[0].strip()
                disk_type = "HDD" if parts[1].strip() == "1" else "SSD"
                break
    except Exception:
        pass

    return {
        "CPU": cpu,
        "File System": fs_type,
        "Hard Disk Name": disk_name,
        "Hard Disk Type": disk_type,
    }


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


def _du_mb(path: Path):
    if not path.exists():
        return None
    try:
        result = subprocess.run(
            ["du", "-s", "-L", "--block-size=1M", "--apparent-size", str(path)],
            capture_output=True,
            text=True,
            timeout=30,
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


def _parse_ts(ts):
    from datetime import datetime, timezone

    if not ts:
        return None
    m = re.match(r"^(\d{4})(\d{2})(\d{2})T(\d{2}):(\d{2}):(\d{2})", ts)
    if not m:
        return None
    return datetime(
        int(m[1]), int(m[2]), int(m[3]),
        int(m[4]), int(m[5]), int(m[6]),
        tzinfo=timezone.utc,
    )


def _format_ts(ts):
    d = _parse_ts(ts)
    if not d:
        return ts or ""
    return d.strftime("%d %b %Y %H:%M:%S") + " UTC"


def _calc_duration(start, end):
    s = _parse_ts(start)
    e = _parse_ts(end)
    if not s or not e:
        return "\u2014"
    ms = int((e - s).total_seconds() * 1000)
    if ms < 0:
        return "\u2014"
    if ms < 1000:
        return f"{ms}ms"
    total_sec = ms // 1000
    if total_sec < 60:
        return f"{total_sec}s"
    total_min = total_sec // 60
    sec = total_sec % 60
    if total_min < 60:
        return f"{total_min}m {sec}s"
    hrs = total_min // 60
    mins = total_min % 60
    if hrs < 24:
        return f"{hrs}h {mins}m"
    days = hrs // 24
    rem_h = hrs % 24
    return f"{days}d {rem_h}h {mins}m"


def _run_stats(run):
    steps = run.get("steps", [])
    completed = sum(1 for s in steps if s.get("status") == "success")
    failed = sum(1 for s in steps if s.get("status") == "failed")
    total = len(ALL_STEPS)
    if failed > 0:
        overall = "failed"
    elif completed == total:
        overall = "success"
    elif completed > 0:
        overall = "running"
    else:
        overall = "pending"
    return {"completed": completed, "total": total, "overall": overall}


# ── Query flow classification from queries_excluded.csv ────────


def _classify_queries(pivot):
    """
    Given a pivot dict {query_name: {invalid_in_graphdb, invalid_in_jena,
    invalid_in_ostrich, malformed_graphdb, malformed_jena}},
    classify each query into stage 2 and stage 3 partitions.

    Returns a dict with:
      stage1_nodes: sorted list of all query names
      stage2_valid: sorted list of queries valid in at least one store (original)
      stage2_invalid: sorted list of queries invalid in at least one store (original)
      stage3_valid: sorted list of queries valid in all TS stores (timestamped)
      stage3_partial: sorted list valid in some but not all TS stores (timestamped)
      stage3_invalid: sorted list invalid in all TS stores (timestamped)
      edges_s1_s2: list of {src, dst_partition, store_id, store_color}
      edges_s2_s3: list of {src, dst_partition, store_id, store_color}
      orig_counts: [{store_id, label, color, valid, total}]
      ts_counts: [{store_id, label, color, valid_ts, valid_orig}]
      stores_all: list of store dicts
      stores_ts: list of store dicts (without ostrich)
    """
    STORES_ALL = QF_STORES_ALL
    STORES_TS = QF_STORES_TS

    def valid_orig(flags, sid):
        if sid == "graphdb":
            return not flags["invalid_in_graphdb"]
        if sid == "jena":
            return not flags["invalid_in_jena"]
        if sid == "ostrich":
            return not flags["invalid_in_ostrich"]
        return True

    def valid_ts(flags, sid):
        if sid == "graphdb":
            return not flags["invalid_in_graphdb"] and not flags["malformed_graphdb"]
        if sid == "jena":
            return not flags["invalid_in_jena"] and not flags["malformed_jena"]
        return True

    all_queries = sorted(pivot.keys())

    # Stage 2 classification
    s2_valid_set = set()
    s2_invalid_set = set()
    for qname, flags in pivot.items():
        if any(valid_orig(flags, s["id"]) for s in STORES_ALL):
            s2_valid_set.add(qname)
        if any(not valid_orig(flags, s["id"]) for s in STORES_ALL):
            s2_invalid_set.add(qname)

    # Stage 3 classification (only from queries that are valid in original for TS stores)
    s3_valid_set = set()
    s3_partial_set = set()
    s3_invalid_set = set()
    for qname, flags in pivot.items():
        if qname not in s2_valid_set:
            continue
        has_any_ts_orig = any(valid_orig(flags, s["id"]) for s in STORES_TS)
        if not has_any_ts_orig:
            continue
        if all(valid_ts(flags, s["id"]) for s in STORES_TS):
            s3_valid_set.add(qname)
        elif any(valid_ts(flags, s["id"]) for s in STORES_TS):
            s3_partial_set.add(qname)
        else:
            s3_invalid_set.add(qname)

    # Edges stage 1 -> stage 2
    edges_s1_s2 = []
    for qname in all_queries:
        flags = pivot[qname]
        for si, s in enumerate(STORES_ALL):
            dst_partition = "valid" if valid_orig(flags, s["id"]) else "invalid"
            edges_s1_s2.append({
                "src": qname,
                "dst": qname,
                "dst_partition": dst_partition,
                "store_id": s["id"],
                "store_color": s["color"],
                "store_index": si,
            })

    # Edges stage 2 -> stage 3
    edges_s2_s3 = []
    for qname in sorted(s2_valid_set):
        flags = pivot[qname]
        for si, s in enumerate(STORES_TS):
            if not valid_orig(flags, s["id"]):
                continue
            if qname in s3_valid_set:
                dst_partition = "valid"
            elif qname in s3_partial_set:
                dst_partition = "partial"
            else:
                dst_partition = "invalid"
            edges_s2_s3.append({
                "src": qname,
                "dst": qname,
                "dst_partition": dst_partition,
                "store_id": s["id"],
                "store_color": s["color"],
                "store_index": si,
            })

    # Counts
    total = len(all_queries)
    orig_counts = []
    for s in STORES_ALL:
        v = sum(1 for flags in pivot.values() if valid_orig(flags, s["id"]))
        orig_counts.append({
            "store_id": s["id"], "label": s["label"], "color": s["color"],
            "valid": v, "total": total,
        })

    ts_counts = []
    for s in STORES_TS:
        vo = sum(1 for flags in pivot.values() if valid_orig(flags, s["id"]))
        vt = sum(
            1 for flags in pivot.values()
            if valid_orig(flags, s["id"]) and valid_ts(flags, s["id"])
        )
        ts_counts.append({
            "store_id": s["id"], "label": s["label"], "color": s["color"],
            "valid_ts": vt, "valid_orig": vo,
        })

    # For each node we need to know how many stores consider it valid (for fill color)
    node_valid_all_count = {}
    node_valid_ts_count = {}
    for qname, flags in pivot.items():
        node_valid_all_count[qname] = sum(
            1 for s in STORES_ALL if valid_orig(flags, s["id"])
        )
        node_valid_ts_count[qname] = sum(
            1 for s in STORES_TS
            if valid_orig(flags, s["id"]) and valid_ts(flags, s["id"])
        )

    return {
        "stage1_nodes": all_queries,
        "stage2_valid": sorted(s2_valid_set),
        "stage2_invalid": sorted(s2_invalid_set),
        "stage3_valid": sorted(s3_valid_set),
        "stage3_partial": sorted(s3_partial_set),
        "stage3_invalid": sorted(s3_invalid_set),
        "edges_s1_s2": edges_s1_s2,
        "edges_s2_s3": edges_s2_s3,
        "orig_counts": orig_counts,
        "ts_counts": ts_counts,
        "stores_all": STORES_ALL,
        "stores_ts": STORES_TS,
        "node_valid_all_count": node_valid_all_count,
        "node_valid_ts_count": node_valid_ts_count,
        "num_stores_all": len(STORES_ALL),
        "num_stores_ts": len(STORES_TS),
    }


# ── Matplotlib chart builders ─────────────────────────────────


def _fig_to_base64(fig, dpi=150):
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=dpi, bbox_inches="tight", facecolor="white")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("ascii")


def _build_ingest_chart(rows):
    """Build one scatter plot per dataset: DB size (log) vs ingest time (log)."""
    ts_color_map = {}
    fallback_idx = [0]

    def get_color(ts):
        if ts not in ts_color_map:
            ts_color_map[ts] = TS_COLORS_CHART.get(
                ts, TS_COLOR_FALLBACKS[fallback_idx[0] % len(TS_COLOR_FALLBACKS)]
            )
            if ts not in TS_COLORS_CHART:
                fallback_idx[0] += 1
        return ts_color_map[ts]

    for r in rows:
        get_color(r["triplestore"])

    by_dataset = defaultdict(list)
    for r in rows:
        by_dataset[r["dataset"]].append(r)

    charts = {}
    for ds in sorted(by_dataset.keys()):
        data_rows = by_dataset[ds]

        fig, ax = plt.subplots(figsize=(5, 3.5))

        db_vals_gib = [r["avg_db_size_mib"] / 1024 for r in data_rows]
        time_vals = [max(r["avg_ingestion_time"], 0.1) for r in data_rows]
        colors = [ts_color_map[r["triplestore"]] for r in data_rows]

        # Scatter points
        seen_stores = set()
        for i, r in enumerate(data_rows):
            label = r["triplestore"] if r["triplestore"] not in seen_stores else None
            seen_stores.add(r["triplestore"])
            ax.scatter(
                db_vals_gib[i], time_vals[i],
                color=colors[i], s=50, zorder=3,
                edgecolors="white", linewidths=0.5,
                label=label,
            )

        # ── Non-overlapping labels via repulsion ──
        # Work in log-space for uniform spacing
        import math as _math
        log_x = [_math.log10(max(v, 1e-6)) for v in db_vals_gib]
        log_y = [_math.log10(max(v, 1e-6)) for v in time_vals]

        # Initial offsets in points
        offsets = [[8, 6] for _ in data_rows]

        # Iterative repulsion to separate overlapping labels
        for iteration in range(60):
            moved = False
            for i in range(len(data_rows)):
                for j in range(i + 1, len(data_rows)):
                    # Label positions in log-space (approximate)
                    xi = log_x[i] + offsets[i][0] * 0.01
                    yi = log_y[i] + offsets[i][1] * 0.01
                    xj = log_x[j] + offsets[j][0] * 0.01
                    yj = log_y[j] + offsets[j][1] * 0.01

                    dx = xi - xj
                    dy = yi - yj
                    dist = _math.sqrt(dx * dx + dy * dy)

                    # If labels are too close, push them apart
                    min_dist = 0.50
                    if dist < min_dist and dist > 0:
                        push = (min_dist - dist) / 2
                        nx = dx / dist
                        ny = dy / dist
                        offsets[i][0] += nx * push * 80
                        offsets[i][1] += ny * push * 80
                        offsets[j][0] -= nx * push * 80
                        offsets[j][1] -= ny * push * 80
                        moved = True

            if not moved:
                break

        # Draw labels with computed offsets
        texts = []
        for i, r in enumerate(data_rows):
            txt = ax.annotate(
                r["policy"],
                (db_vals_gib[i], time_vals[i]),
                textcoords="offset points",
                xytext=(offsets[i][0], offsets[i][1]),
                fontsize=5.5,
                fontfamily="monospace",
                color="#333",
                arrowprops=dict(
                    arrowstyle="-",
                    color="#aaa",
                    linewidth=0.5,
                    shrinkA=0,
                    shrinkB=3,
                ),
                zorder=4,
            )
            texts.append(txt)


        ax.set_xscale("log")
        ax.set_yscale("log")

        # ── X axis: DB size in GiB ──
        ax.set_xlabel("DB Size (GiB)", fontsize=8)
        db_min = min(db_vals_gib) if db_vals_gib else 0.01
        db_max = max(db_vals_gib) if db_vals_gib else 10
        ax.set_xlim(db_min * 0.3, db_max * 5)
        ax.xaxis.set_major_formatter(ticker.FuncFormatter(
            lambda v, _: f"{v:.2f}" if v < 1 else f"{v:.1f}" if v < 10 else f"{v:.0f}"
        ))
        ax.xaxis.set_minor_formatter(ticker.NullFormatter())

        # ── Y axis: ingest time with human-readable ticks ──
        ax.set_ylabel("Ingest Time", fontsize=8)
        time_tick_vals =   [0.1,    1,    10,    100,      1_000,     10_000,    100_000,   1_000_000]
        time_tick_labels = ["0.1s", "1s", "10s", "1m 40s", "16m 40s", "2h 46m", "1d 3h",  "11d 13h"]
        t_min = min(time_vals) if time_vals else 0.1
        t_max = max(time_vals) if time_vals else 1000
        ax.set_ylim(t_min * 0.3, t_max * 5)
        visible = [(v, l) for v, l in zip(time_tick_vals, time_tick_labels)
                   if v >= t_min * 0.2 and v <= t_max * 6]
        if visible:
            ax.set_yticks([v for v, _ in visible])
            ax.set_yticklabels([l for _, l in visible], fontsize=6, fontfamily="monospace")
        ax.yaxis.set_minor_formatter(ticker.NullFormatter())

        ax.tick_params(axis="x", labelsize=7)
        ax.tick_params(axis="y", labelsize=6)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.grid(True, which="major", alpha=0.2, linewidth=0.5)

        # Legend for triple stores
        handles, labels_leg = ax.get_legend_handles_labels()
        if handles:
            ax.legend(
                handles, labels_leg,
                fontsize=6, framealpha=0.8,
                loc="best", handletextpad=0.3,
                borderpad=0.4, labelspacing=0.3,
            )

        charts[ds] = _fig_to_base64(fig, dpi=200)

    return charts

def _build_time_plots(plot_data):
    policy_color_map = {}
    ci = 0
    for p in sorted(set(s["policy"] for s in plot_data)):
        if p in POLICY_COLORS:
            policy_color_map[p] = POLICY_COLORS[p]
        else:
            policy_color_map[p] = POLICY_COLOR_FALLBACKS[ci % len(POLICY_COLOR_FALLBACKS)]
            ci += 1

    by_ds = {}
    for s in plot_data:
        by_ds.setdefault(s["dataset"], {}).setdefault(
            s["query_set"], {}
        ).setdefault(s["triplestore"], []).append(s)

    DATASET_ORDER = ["bearb_day", "bearb_hour", "orkg", "bearc"]
    available = list(by_ds.keys())
    datasets = [d for d in DATASET_ORDER if d in available] + sorted(
        set(available) - set(DATASET_ORDER)
    )

    result_plots = []

    for ds in datasets:
        for qs in sorted(by_ds[ds].keys()):
            ts_map = by_ds[ds][qs]
            for ts_name in sorted(ts_map.keys()):
                series_list = ts_map[ts_name]

                fig, ax = plt.subplots(figsize=(4, 2.5))
                ax.set_yscale("log")
                ax.set_ylim(0.001, 30)
                ax.set_ylabel("Query time (s)", fontsize=8)
                ax.set_xlabel("Version", fontsize=8)
                ax.set_title(ts_name, fontsize=9, fontweight="bold", color="#006699")
                ax.tick_params(axis="both", labelsize=7)
                ax.set_facecolor("#fafafa")
                ax.grid(True, alpha=0.3)

                averages = []
                for s in sorted(series_list, key=lambda x: x["policy"]):
                    color = policy_color_map.get(s["policy"], "#666")
                    pts = [(p[0], p[1]) for p in s["points"] if p[1] > 0]
                    vals = [p[1] for p in s["points"] if p[1] > 0]
                    avg = sum(vals) / len(vals) if vals else None
                    averages.append({"policy": s["policy"], "avg": avg, "color": color})
                    if len(pts) >= 2:
                        xs = [int(p[0]) for p in pts]
                        ys = [p[1] for p in pts]
                        ax.plot(
                            xs, ys, color=color, linewidth=1.8,
                            label=s["policy"], solid_capstyle="round",
                        )

                img_b64 = _fig_to_base64(fig)
                result_plots.append({
                    "dataset": ds,
                    "query_set": qs,
                    "triplestore": ts_name,
                    "image_b64": img_b64,
                    "averages": averages,
                })

    return {
        "plots": result_plots,
        "policy_legend": [
            {"policy": p, "color": policy_color_map[p]}
            for p in sorted(policy_color_map.keys())
        ],
        "datasets_order": datasets,
    }


# ── Step detail builders ──────────────────────────────────────


def _detail_download(run_dir: Path) -> dict:
    config = _load_config()
    datasets_cfg = config.get("datasets", {})
    detail = {"datasets": []}
    discovered = set(_discovered_datasets(run_dir))

    sizes = {}
    meta_csv = run_dir / "output" / "measurements" / "datasets_meta.csv"
    if meta_csv.exists():
        with open(meta_csv, newline="") as f:
            for row in csv.DictReader(f):
                name = row.get("dataset", "")
                try:
                    sizes[name] = float(row.get("size", 0))
                except ValueError:
                    pass

    query_sets_by_dataset = {}
    queries_csv = run_dir / "output" / "measurements" / "queries_meta.csv"
    if queries_csv.exists():
        with open(queries_csv, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                qs_name = row.get("query_set", "").strip()
                for_ds = row.get("for_dataset", "").strip()
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
                query_sets_by_dataset.setdefault(for_ds, []).append(
                    {"name": qs_name, "links": links}
                )
    else:
        for ds_name, ds_meta in datasets_cfg.items():
            for qs_name, qs_meta in ds_meta.get("query_sets", {}).items():
                query_sets_by_dataset.setdefault(ds_name, []).append({
                    "name": qs_name,
                    "links": [
                        {
                            "filename": lnk.rstrip("/").split("/")[-1].split("?")[0],
                            "url": lnk,
                        }
                        for lnk in qs_meta.get("download_links", [])
                    ],
                })

    for name, meta in datasets_cfg.items():
        if name not in discovered:
            continue
        detail["datasets"].append({
            "name": name,
            "description": DATASET_DESCRIPTIONS.get(name, ""),
            "versions": meta.get("snapshot_versions", "?"),
            "size_mb": sizes.get(name),
            "download_link": meta.get("download_link_snapshots"),
            "query_sets": query_sets_by_dataset.get(name, []),
        })

    return detail


def _detail_preprocess(run_dir: Path) -> dict:
    detail = {}
    pom_path = Path("/starvers_eval/scripts/2_preprocess_data/RDFValidator/pom.xml")
    rdf4j_ver, jena_ver = None, None
    if pom_path.exists():
        tree = ET.parse(pom_path)
        root = tree.getroot()
        ns = {"m": "http://maven.apache.org/POM/4.0.0"}
        for dep in root.findall(".//m:dependency", ns):
            group = dep.find("m:groupId", ns)
            artifact = dep.find("m:artifactId", ns)
            version = dep.find("m:version", ns)
            if group is None or artifact is None or version is None:
                continue
            gt = group.text or ""
            at = artifact.text or ""
            vt = version.text or ""
            if "rdf4j" in gt or "rdf4j" in at:
                rdf4j_ver = vt
            if "jena" in gt or "jena" in at:
                jena_ver = vt
    detail["validators"] = {
        "rdf4j": rdf4j_ver or "not found",
        "jena": jena_ver or "not found",
    }

    preprocess_csv = (
        run_dir / "output" / "measurements" / "preprocess_summary.csv"
    )
    per_dataset = {}
    if preprocess_csv.exists():
        with open(preprocess_csv, newline="") as f:
            for row in csv.DictReader(f):
                dataset = row.get("dataset", "")
                variant = row.get("variant", "")
                if variant != "ic" or not dataset:
                    continue
                if dataset not in per_dataset:
                    per_dataset[dataset] = {
                        "subject": 0, "object": 0, "invalid": 0, "file_count": 0,
                    }
                per_dataset[dataset]["subject"] += int(
                    float(row.get("skolemized_subjects", 0) or 0)
                )
                per_dataset[dataset]["object"] += int(
                    float(row.get("skolemized_objects", 0) or 0)
                )
                per_dataset[dataset]["invalid"] += int(
                    float(row.get("invalid_triples", 0) or 0)
                )
                per_dataset[dataset]["file_count"] += 1

    detail["skolemization_per_dataset"] = [
        {
            "dataset": ds,
            "subject": vals["subject"],
            "object": vals["object"],
            "invalid": vals["invalid"],
            "invalid_avg": (
                round(vals["invalid"] / vals["file_count"], 2)
                if vals.get("file_count")
                else 0
            ),
        }
        for ds, vals in per_dataset.items()
    ]

    # Parse queries_excluded.csv and classify
    excl_csv = (
        run_dir / "output" / "measurements" / "queries_excluded.csv"
    )
    if excl_csv.exists():
        pivot = {}
        with open(excl_csv, newline="") as f:
            for row in csv.DictReader(f):
                name = row.get("query", "").strip()
                reason = row.get("reason", "").strip()
                excl = int(row.get("yn_excluded", "0") or 0)
                if not name or reason == "ASK":
                    continue
                if name not in pivot:
                    pivot[name] = {
                        "invalid_in_graphdb": 0,
                        "malformed_graphdb": 0,
                        "invalid_in_jena": 0,
                        "malformed_jena": 0,
                        "invalid_in_ostrich": 0,
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

        detail["sciqa_total"] = len(pivot)
        detail["query_flow"] = _classify_queries(pivot)

    return detail


def _detail_construct_datasets(run_dir: Path) -> dict:
    discovered = _discovered_datasets(run_dir)
    variants = []
    for dataset in discovered:
        ds_dir = run_dir / "rawdata" / dataset
        for fname, variant_name, _ in VARIANT_FILES:
            path = ds_dir / fname
            size_mb = _du_mb(path)
            variants.append({
                "dataset": dataset,
                "variant": variant_name,
                "size_mb": size_mb,
                "versioning_approach": VERSIONING_APPROACH.get(variant_name, "\u2014"),
            })
    return {"variants": variants}


def _detail_ingest(run_dir: Path) -> dict:
    ingest_csv = run_dir / "output" / "measurements" / "storage_and_ingestion.csv"
    summary = []
    if ingest_csv.exists():
        groups = defaultdict(list)
        with open(ingest_csv, newline="") as f:
            for row in csv.DictReader(f, delimiter=";"):
                key = (
                    row.get("triplestore", ""),
                    row.get("policy", ""),
                    row.get("dataset", ""),
                )
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
                "triplestore": triplestore,
                "policy": policy,
                "dataset": dataset,
                "avg_ingestion_time": sum(v[0] for v in values) / len(values),
                "avg_db_size_mib": sum(v[1] for v in values) / len(values),
                "avg_raw_size_mib": sum(v[2] for v in values) / len(values),
            })

    ingest_charts = {}
    if summary:
        try:
            ingest_charts = _build_ingest_chart(summary)
        except Exception as e:
            logging.error(f"Error building ingest chart: {e}")

    return {"ingestion_summary": summary, "ingest_charts": ingest_charts}


def _detail_construct_queries(run_dir: Path) -> dict:
    query_counts_path = (
        run_dir / "output" / "measurements" / "queries_counts.csv"
    )
    counts = defaultdict(lambda: defaultdict(int))
    policies_found = set()
    datasets_found = set()
    if query_counts_path.exists():
        with open(query_counts_path, newline="") as f:
            for row in csv.DictReader(f):
                policy = row.get("policy", "").strip()
                dataset = row.get("dataset", "").strip()
                try:
                    count = int(row.get("query_count", 0))
                except ValueError:
                    count = 0
                counts[policy][dataset] += count
                policies_found.add(policy)
                datasets_found.add(dataset)

    POLICY_ORDER = ["ic_sr_ng", "ostrich", "ostrich_aggchange", "tb_sr_ng", "tb_sr_rs"]
    DATASET_ORDER = ["bearb_day", "bearb_hour", "bearc", "orkg"]
    policies = [p for p in POLICY_ORDER if p in policies_found] + sorted(
        policies_found - set(POLICY_ORDER)
    )
    datasets = [d for d in DATASET_ORDER if d in datasets_found] + sorted(
        datasets_found - set(DATASET_ORDER)
    )
    query_counts = {p: dict(ds_map) for p, ds_map in counts.items()}
    totals_per_dataset = {
        ds: sum(query_counts.get(p, {}).get(ds, 0) for p in policies) for ds in datasets
    }
    return {
        "query_counts": query_counts,
        "totals_per_dataset": totals_per_dataset,
        "policies": policies,
        "datasets": datasets,
    }


def _detail_evaluate(run_dir: Path) -> dict:
    time_csv = run_dir / "output" / "measurements" / "queries_time.csv"
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

    hardware_info = _get_hardware_info()
    return {
        "time_header": time_header,
        "time_samples": time_samples,
        "time_total_rows": time_total_rows,
        "hardware": hardware_info,
    }


def _detail_visualize(run_dir: Path) -> dict:
    time_csv = run_dir / "output" / "measurements" / "queries_time.csv"
    rewrite_csv = run_dir / "output" / "measurements" / "queries_rewriting_times.csv"

    if not time_csv.exists():
        return {"plot_info": None}

    rows = []
    with open(time_csv, newline="") as f:
        for row in csv.DictReader(f, delimiter=";"):
            try:
                exec_time = float(row.get("execution_time", -1) or -1)
                yn_timeout = int(float(row.get("yn_timeout", 0) or 0))
                if exec_time >= 30 or yn_timeout:
                    exec_time_clean = 30.0
                elif exec_time < 0:
                    exec_time_clean = -1.0
                else:
                    exec_time_clean = exec_time
                rows.append({
                    "triplestore": row.get("triplestore", "").strip(),
                    "dataset": row.get("dataset", "").strip(),
                    "policy": row.get("policy", "").strip(),
                    "query_set": row.get("query_set", "").strip(),
                    "snapshot": row.get("snapshot", "").strip(),
                    "query": row.get("query", "").strip(),
                    "exec_time": exec_time_clean,
                    "rewrite_time": 0.0,
                })
            except (ValueError, TypeError):
                continue

    rewrite_map = {}
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

    agg = defaultdict(list)
    for r in rows:
        if r["total_time"] < 0:
            continue
        key = (
            r["triplestore"], r["policy"], r["dataset"],
            r["query_set"], r["snapshot"],
        )
        agg[key].append(r["total_time"])

    series_map = {}
    for (ts, policy, dataset, query_set, snapshot), times in agg.items():
        series_key = (ts, policy, dataset, query_set)
        if series_key not in series_map:
            series_map[series_key] = {
                "triplestore": ts, "policy": policy,
                "dataset": dataset, "query_set": query_set,
                "points": {},
            }
        try:
            version = int(snapshot)
        except (ValueError, TypeError):
            version = snapshot
        series_map[series_key]["points"][version] = sum(times) / len(times)

    plot_series = []
    for s in series_map.values():
        s["points"] = sorted(s["points"].items())
        plot_series.append(s)

    plot_info = None
    if plot_series:
        try:
            plot_info = _build_time_plots(plot_series)
        except Exception as e:
            logging.error(f"Error building time plots: {e}")

    return {"plot_info": plot_info}



# ── Routes ────────────────────────────────────────────────────


def _get_runs_with_stats():
    runs = []
    if DATA_DIR.exists():
        dirs = sorted(DATA_DIR.glob("*T*"), reverse=True)
        runs = [_read_run(d) for d in dirs if d.is_dir()]
    result = []
    for run in runs:
        stats = _run_stats(run)
        pct = (
            round((stats["completed"] / stats["total"]) * 100)
            if stats["total"]
            else 0
        )
        result.append({**run, **stats, "pct": pct})
    return result


def _template_context(**extra):
    return {
        "ALL_STEPS": ALL_STEPS,
        "STEP_DESCRIPTIONS": STEP_DESCRIPTIONS,
        "format_ts": _format_ts,
        "calc_duration": _calc_duration,
        "fmt": _fmt,
        "fmt_mb": _fmt_mb,
        "fmt_sec": _fmt_sec,
        **extra,
    }


@app.get("/")
def serve_gui():
    runs = _get_runs_with_stats()
    return render_template(
        "index.html",
        **_template_context(
            runs=runs,
            active_run=None,
            active_ts=None,
            step_details={},
            step_map={},
            has_running=False,
        ),
    )


@app.get("/run/<ts>")
def serve_run(ts: str):
    """Render page immediately with static skeleton — no heavy detail computation."""
    run_dir = DATA_DIR / ts
    if not run_dir.is_dir():
        abort(404)

    runs = _get_runs_with_stats()
    active_run = _read_run(run_dir)
    active_stats = _run_stats(active_run)
    active_run.update(active_stats)

    step_map = {}
    for s in active_run.get("steps", []):
        step_map[s.get("step_name", "")] = s

    has_running = any(
        step_map.get(name, {}).get("status") == "running" for name in ALL_STEPS
    )

    # NOTE: no _gather_all_step_details — details load async via JS
    return render_template(
        "index.html",
        **_template_context(
            runs=runs,
            active_run=active_run,
            active_ts=ts,
            step_details={},      # empty — loaded async
            step_map=step_map,
            has_running=has_running,
        ),
    )


@app.get("/api/step-html/<ts>/<step_name>")
def get_step_html(ts: str, step_name: str):
    """Return rendered HTML fragment for a single step's detail body."""
    run_dir = DATA_DIR / ts
    if not run_dir.is_dir():
        abort(404)

    builders = {
        "download": _detail_download,
        "preprocess_data": _detail_preprocess,
        "construct_datasets": _detail_construct_datasets,
        "ingest": _detail_ingest,
        "construct_queries": _detail_construct_queries,
        "evaluate": _detail_evaluate,
        "visualize": _detail_visualize,
    }
    builder = builders.get(step_name)
    if not builder:
        abort(404)

    try:
        info = builder(run_dir)
    except Exception as e:
        logging.error(f"Error building detail for {step_name}: {e}")
        info = {"error": str(e)}

    return render_template(
        "step_detail.html",
        name=step_name,
        info=info,
        fmt=_fmt,
        fmt_mb=_fmt_mb,
        fmt_sec=_fmt_sec,
        format_ts=_format_ts,
        # Query flow layout constants needed by preprocess template
        BOX_W=22, BOX_H=80, PORT_R=3, DX=30, PGAP=48, MARGIN=36,
        STAGE_Y=[60, 290, 520],
    )

@app.get("/api/runs")
def list_runs():
    if not DATA_DIR.exists():
        return jsonify([])
    dirs = sorted(DATA_DIR.glob("*T*"), reverse=True)
    return jsonify([_read_run(d) for d in dirs if d.is_dir()])


def run_api():
    app.run(host="0.0.0.0", port=PORT)


if __name__ == "__main__":
    run_api()