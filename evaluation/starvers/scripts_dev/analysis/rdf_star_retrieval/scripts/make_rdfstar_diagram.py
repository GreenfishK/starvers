#!/usr/bin/env python3
"""
Generate paper/plots/RDF-star_storage_and_retrieval.pdf — a 4-quadrant diagram
sketching how GraphDB and Jena store, index, and retrieve the decorator (tb_sr_rs)
and reification (tb_sr_re) RDF-star models.

Quadrants (row x col):
  (top-left)     GraphDB  : decorator model
  (top-right)    GraphDB  : reification model
  (bottom-left)  Jena     : decorator model
  (bottom-right) Jena     : reification model

Usage: python make_rdfstar_diagram.py --out /path/to/RDF-star_storage_and_retrieval.pdf
"""
import argparse
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from matplotlib.patches import FancyBboxPatch, FancyArrowPatch

# palette
G_GOOD = "#1b7a3d"
G_MID = "#c08424"
G_BAD = "#a4243b"
BLUE = "#1f4e79"
GREY = "#555555"
BOX = "#eef3f8"
BOX_EDGE = "#7f9db9"


def _store_boxes(ax, store, x0, y0, w, h):
    # Storage/indexing summary box
    ax.text(x0 + 0.02, y0 + h - 0.03, store, fontsize=13,
            fontweight="bold", color=BLUE, va="top")
    ax.add_patch(FancyBboxPatch((x0, y0), w, h,
                 boxstyle="round,pad=0.0", fc=BOX, ec=BOX_EDGE, lw=1.2))


def draw_graphdb_decorator(ax):
    _store_boxes(ax, "GraphDB — decorator (tb_sr_rs)", 0, 0.5, 1, 0.5)
    lines = [
        ("storage", "fact = one atom; quoted triples indexed via PSO/SPO/POS stores"),
        ("indexing", "double-nested triple term = a single anonymous node"),
        ("retrieval", "query plan: 2 nested-level TRIPLE operators over one anon. node"),
        ("verdict", "single index segment per fact; point lookup + subject-identity join", G_GOOD),
    ]
    for i, row in enumerate(lines):
        tag = row[0]; txt = row[1]; col = row[2] if len(row) > 2 else None
        ax.text(0.02, 0.40 - i * 0.10, f"{tag.upper()}", fontsize=8,
                fontweight="bold", color=BLUE, va="top")
        ax.text(0.20, 0.40 - i * 0.10, txt, fontsize=8.5, va="top",
                color=col or "#222")
    ax.text(0.02, 0.03, "exec. evidence: exp-graphdb-tb_sr_rs-*.plan.txt",
            fontsize=7, color=GREY, va="bottom")


def draw_graphdb_reification(ax):
    _store_boxes(ax, "GraphDB — reification (tb_sr_re)", 0, 0.5, 1, 0.5)
    lines = [
        ("storage", "fact split across blank-node reifier + 3 predicates"),
        ("indexing", "rdf:reifies, valid_from, valid_until = 3 separate collections"),
        ("retrieval", "query plan: join blank node across the 3 predicate collections"),
        ("verdict", "3 collections each ~85k (bearb_day); 3x the statements", G_BAD),
    ]
    for i, row in enumerate(lines):
        tag = row[0]; txt = row[1]; col = row[2] if len(row) > 2 else None
        ax.text(0.02, 0.40 - i * 0.10, f"{tag.upper()}", fontsize=8,
                fontweight="bold", color=BLUE, va="top")
        ax.text(0.20, 0.40 - i * 0.10, txt, fontsize=8.5, va="top",
                color=col or "#222")
    ax.text(0.02, 0.03, "exec. evidence: exp-graphdb-tb_sr_re-*.plan.txt",
            fontsize=7, color=GREY, va="bottom")


def draw_jena_decorator(ax):
    _store_boxes(ax, "Jena TDB2 — decorator (tb_sr_rs)", 0, 0.5, 1, 0.5)
    lines = [
        ("storage", "quoted triples stored as bytes in NodeTableTRDF (nodes.dat)"),
        ("indexing", "SPO/POS/OSP indexes reference them like ordinary nodes"),
        ("retrieval", "nested term w/ variable -> wildcard + full outer scan + rebuild"),
        ("verdict", "full 85k-row scan of outer valid_until; slowest here", G_BAD),
    ]
    for i, row in enumerate(lines):
        tag = row[0]; txt = row[1]; col = row[2] if len(row) > 2 else None
        ax.text(0.02, 0.40 - i * 0.10, f"{tag.upper()}", fontsize=8,
                fontweight="bold", color=BLUE, va="top")
        ax.text(0.20, 0.40 - i * 0.10, txt, fontsize=8.5, va="top",
                color=col or "#222")
    ax.text(0.02, 0.03, "exec. evidence: exp-jena-tb_sr_rs-*.plan.txt (ALGEBRA/TDB2)",
            fontsize=7, color=GREY, va="bottom")


def draw_jena_reification(ax):
    _store_boxes(ax, "Jena TDB2 — reification (tb_sr_re)", 0, 0.5, 1, 0.5)
    lines = [
        ("storage", "rdf:reifies carries the (variable) quoted triple"),
        ("indexing", "valid_from / valid_until = ordinary triples on the blank node"),
        ("retrieval", "only rdf:reifies suffers scan; valid_* answered from POS/OSP"),
        ("verdict", "scan hits a smaller fraction of each fact; faster than decorator", G_GOOD),
    ]
    for i, row in enumerate(lines):
        tag = row[0]; txt = row[1]; col = row[2] if len(row) > 2 else None
        ax.text(0.02, 0.40 - i * 0.10, f"{tag.upper()}", fontsize=8,
                fontweight="bold", color=BLUE, va="top")
        ax.text(0.20, 0.40 - i * 0.10, txt, fontsize=8.5, va="top",
                color=col or "#222")
    ax.text(0.02, 0.03, "exec. evidence: exp-jena-tb_sr_re-*.plan.txt (ALGEBRA/TDB2)",
            fontsize=7, color=GREY, va="bottom")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    fig, axes = plt.subplots(2, 2, figsize=(13, 8))
    fig.suptitle("RDF-star storage and retrieval: GraphDB vs Jena TDB2",
                 fontsize=14, fontweight="bold", color="#333")

    draw_graphdb_decorator(axes[0, 0])
    draw_graphdb_reification(axes[0, 1])
    draw_jena_decorator(axes[1, 0])
    draw_jena_reification(axes[1, 1])

    for ax in axes.flat:
        ax.set_xlim(0, 1)
        ax.set_ylim(0, 1)
        ax.set_axis_off()
    fig.tight_layout(rect=[0, 0, 1, 0.96])

    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, format="pdf", bbox_inches="tight")
    print(f"[ok] wrote {out} ({out.stat().st_size} bytes)")


if __name__ == "__main__":
    main()
