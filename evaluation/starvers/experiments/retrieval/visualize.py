#!/usr/bin/env python3
"""
visualize.py — produce the LaTeX table for the retrieval experiment (D1/D2/D3,
GraphDB vs. Jena TDB2) with average query times.

Reads the per-run measurements from

    <base>/output/measurements/retrieval.csv

(CSV header: triplestore;dataset;run;query_time_s) and emits a booktabs LaTeX
table that reproduces the paper's Dataset characteristics / hypotheses table
(Target set, Large set, Hypothesis) with two additional columns for the GraphDB
and Jena TDB2 average query time, in seconds.

Output: <base>/output/tables/retrieval_table.tex
Logging: <base>/output/logs/visualize/visualize.log
"""
import os
from pathlib import Path

import pandas as pd

from experiments.logging import setup_logging

BASE = Path(os.environ.get("RETRIEVAL_BASE", "/starvers_eval/data/retrieval_exp"))
OUTPUT = BASE / "output"
MEAS_DIR = OUTPUT / "measurements"
MEAS_FILE = MEAS_DIR / "retrieval.csv"
TABLES_DIR = OUTPUT / "tables"
OUT_FILE = TABLES_DIR / "retrieval_table.tex"

POLICY = "tb_sr_rs"
DATASETS = ["d1", "d2", "d3"]
STORES = ["graphdb", "jenatdb2"]
STORE_LABELS = {"graphdb": "GraphDB", "jenatdb2": "Jena TDB2"}

# Fixed dataset characteristics / hypotheses (from the paper's tab:datasets).
# The average times are filled in from retrieval.csv; the left-hand columns are
# static descriptions of the synthetic datasets.
DATASET_CHARS = {
    "d1": {
        "label": r"$D_1$",
        "target": r"\texttt{rdf:type dbp:Film}, $ct$=2022, $et$=9999",
        "large": r"\texttt{foaf:interest wiki:}, $ct$=2025, $et$=2026",
        "hypothesis": r"GraphDB $\approx$ Jena",
    },
    "d2": {
        "label": r"$D_2$",
        "target": r"\texttt{rdf:type dbp:Film}, $ct$=2022, $et$=9999",
        "large": r"\texttt{foaf:interest wiki:}, $ct$=2025, $et$=9999",
        "hypothesis": r"Jena $>$ GraphDB",
    },
    "d3": {
        "label": r"$D_3$",
        "target": r"\texttt{rdf:type dbp:Film}, $ct$=2022, $et$=9999",
        "large": r"\texttt{rdf:type dbp:Film}, $ct$=2020, $et$=2021",
        "hypothesis": r"GraphDB $\approx$ Jena",
    },
}


def load_averages() -> dict[str, dict[str, float]]:
    """Return {dataset: {store: mean_query_time_s}} from retrieval.csv."""
    if not MEAS_FILE.exists():
        raise FileNotFoundError(f"Measurements not found: {MEAS_FILE}")
    df = pd.read_csv(MEAS_FILE, delimiter=";", dtype=str).dropna(
        subset=["query_time_s", "dataset", "triplestore"])
    df["query_time_s"] = pd.to_numeric(df["query_time_s"], errors="coerce")
    # Drop timeouts (-1) and NaN so they do not skew the average.
    df = df[df["query_time_s"] >= 0]

    avgs: dict[str, dict[str, float]] = {}
    for (store, dataset), grp in df.groupby(["triplestore", "dataset"]):
        avgs.setdefault(dataset, {})[store] = grp["query_time_s"].mean()
    return avgs


def fmt_time(v: float) -> str:
    if v >= 100:
        return f"{v:.1f}"
    if v >= 1:
        return f"{v:.2f}"
    return f"{v:.3f}"


def cell_time(averages: dict[str, dict[str, float]], dataset: str, store: str) -> str:
    v = averages.get(dataset, {}).get(store)
    return fmt_time(v) if v is not None else "--"


def build_table(averages: dict[str, dict[str, float]]) -> str:
    col_spec = "p{0.04\\textwidth}p{0.20\\textwidth}p{0.20\\textwidth}" \
               "p{0.18\\textwidth}p{0.09\\textwidth}p{0.09\\textwidth}"
    lines = [
        r"\begin{table}[htbp]",
        r"\centering",
        r"\caption{Dataset characteristics and hypotheses for the inside-out "
        r"vs. outside-in experiment. Each dataset has 5M triples, with the "
        r"Target Set consisting of 10 triples and the Large set consisting of "
        r"the rest.}",
        "\\begin{tabular}{%s}" % col_spec,
        r"\toprule",
        r" & \textbf{Target set} & \textbf{Large set} & \textbf{Hypothesis} "
        r"& \textbf{GraphDB (s)} & \textbf{Jena TDB2 (s)} \\",
        r"\midrule",
    ]
    for dataset in DATASETS:
        chars = DATASET_CHARS[dataset]
        lines.append(
            "%s\n"
            "  & %s\n"
            "  & %s\n"
            "  & %s\n"
            "  & %s\n"
            "  & %s \\\\" % (
                chars["label"],
                chars["target"],
                chars["large"],
                chars["hypothesis"],
                cell_time(averages, dataset, "graphdb"),
                cell_time(averages, dataset, "jenatdb2"),
            )
        )
        lines.append(r"\addlinespace")
    lines.extend([
        r"\midrule",
        r"\multicolumn{6}{l}{\textbf{Triple pattern $Q$ evaluated against each "
        r"dataset:}} \\",
        r"\multicolumn{6}{l}{\texttt{<< <<?s rdf:type dbp:Film>> "
        r'vers:valid\_from "2022-10-01T12:00:00" >>}} \\',
        r"\multicolumn{6}{l}{\texttt{\hspace{2em}vers:valid\_until "
        r'"9999-12-31T12:00:00" .}} \\',
        r"\bottomrule",
        r"\end{tabular}",
        r"\label{tab:datasets}",
        r"\end{table}",
    ])
    return "\n".join(lines) + "\n"


def main() -> None:
    os.environ["RUN_DIR"] = str(BASE)
    _, log = setup_logging("visualize")
    log.info("RETRIEVAL_BASE = %s", BASE)

    averages = load_averages()
    log.info("Averaged query times (s): %s", averages)

    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    OUT_FILE.write_text(build_table(averages))
    log.info("Wrote LaTeX table to %s", OUT_FILE)


if __name__ == "__main__":
    main()
