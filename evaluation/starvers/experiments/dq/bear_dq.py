#!/usr/bin/env python3
"""
bear_dq.py
==========
Data-quality (DQ) report for the BEAR datasets. Scans the rawdata of the current
evaluation run (RUN_DIR) and writes a machine-readable CSV report plus a LaTeX
table of the quality issues and their impact on the stored dataset size.

Checks (data is gathered into dq_report.csv, one row per dataset)
------------------------------------------------------------------
INVALID BEARA TRIPLES      — per-version count of invalid RDF lines in BEARA
                             (skipped when BEARA rawdata is absent).
ERRONEOUS VERSION STRINGS   — a TB version string reaches the LAST snapshot
                             although the triple is genuinely deleted earlier
                             (OVERSTATED VALIDITY), or a unique triple that is
                             valid across many snapshots is split into several
                             single-version graphs (SHATTERED VERSION STRINGS).
INCONSISTENT DIFF SETS      — the reference change sets (alldata.CB.nt) do NOT
                             reconcile with the reference IC snapshots
                             (alldata.IC.nt) via
                             |IC(prev)| + |added| - |deleted| == |IC(cur)|.
IMPACT ON DB SIZE           — ingest alldata.TB.nq (orig) and alldata.TB_computed.nq
                             (corrected) for BEARB_day / BEARB_hour into both
                             GraphDB and Jena TDB2, then measure the resulting
                             on-disk DB size. Shows how Overstated Validity and
                             Shattered Version Strings inflate (or deflate) the
                             stored dataset size.

Data layout (relative to <RUN_DIR>/rawdata)
------------------------------------------
    beara/alldata.IC.nt/{...}.nt        (optional)
    bearb_day/alldata.TB.nq, alldata.IC.nt, alldata.CB.nt
    bearb_hour/alldata.TB.nq, alldata.IC.nt, alldata.CB.nt

Outputs (under <RUN_DIR>/)
--------------------------
  output/measurements/dq_report.csv            — one row per dataset (the report)
  output/measurements/dq_change_sets_bearb_hour.csv — the inconsistent-diff-set table
  output/tables/dq_report.tex                  — LaTeX table built from dq_report.csv
  output/result_sets/dq/<name>_<ds>.nt         — overstated/shattered triple sets
  databases/dq/<store>/tb_sr_ng_<ds>_<orig|computed>  — the 8 impact ingest repos

Sizes are measured with `du -s -L --block-size=1M --apparent-size` (MiB).
Log: <RUN_DIR>/output/logs/dq_report/dq_report.log (via experiments.logging).
"""
import csv
import hashlib
import os
import re
import subprocess
from pathlib import Path

import tomli  # noqa: F401  (kept for parity with sibling scripts)

from experiments.logging import setup_logging

GRAPH_RE = re.compile(r"<http://example\.org/v([0-9_]+)>")

RUN_DIR = Path(os.environ["RUN_DIR"])
RAW = RUN_DIR / "rawdata"
MEASURE = RUN_DIR / "output" / "measurements"
REPORT_CSV = MEASURE / "dq_report.csv"
CHANGE_SETS_CSV = MEASURE / "dq_change_sets_bearb_hour.csv"
CHANGE_SETS_DAY_CSV = MEASURE / "dq_change_sets_bearb_day.csv"
RESULT_SETS = RUN_DIR / "output" / "result_sets"
DQ_RESULT_SETS = RESULT_SETS / "dq"
TABLES = RUN_DIR / "output" / "tables"
REPORT_TEX = TABLES / "dq_report.tex"

DATASETS = ("bearb_day", "bearb_hour")              # datasets with a TB.nq
TB_FILES = ("alldata.TB.nq", "alldata.TB_computed.nq")  # (orig, corrected)

# Ingested title-case labels (used in dq_report.tex).
DATASET_LABELS = {"bearb_day": "BEARB_day", "bearb_hour": "BEARB_hour"}

# Impact ingests: 8 repositories (4 dataset/variant x 2 stores).
STORES = ("graphdb", "jenatdb2")
DQ_DB = RUN_DIR / "databases" / "dq"
MGMT_DIR = Path("/starvers_eval/scripts/triple_store_mgmt")
MGMT_SCRIPT = {
    "graphdb": MGMT_DIR / "graphdb_mgmt.sh",
    "jenatdb2": MGMT_DIR / "jenatdb2_mgmt.sh",
}
CONFIG_TMPL_DIR = Path("/starvers_eval/scripts/4_ingest/configs")
CONFIG_DIR = RUN_DIR / "configs" / "ingest"

REPORT_CSV_HEADER = [
    "Dataset",
    "triples_total",
    "triples_overstated_validity",
    "triples_shattered_vers_str",
    "cnt_inconsistent_change_sets",
    "raw_size_orig",
    "raw_size_corrected",
    "graphdb_size_orig",
    "graphdb_size_corrected",
    "jenatdb2_size_orig",
    "jenatdb2_size_corrected",
]

_cache: dict[str, dict | None] = {}


# ---------------------------------------------------------------------------
# Sizing helper (du, MiB)
# ---------------------------------------------------------------------------
def du_size(path: Path) -> int:
    """Apparent size in MiB (`du -s -L --block-size=1M --apparent-size`)."""
    res = subprocess.run(
        ["du", "-s", "-L", "--block-size=1M", "--apparent-size", str(path)],
        capture_output=True,
        text=True,
        check=True,
    )
    return int(res.stdout.split("\t")[0].split()[0])


def _size(path: Path) -> int:
    """Return the du size of a path (0 if absent)."""
    try:
        return du_size(path)
    except (subprocess.CalledProcessError, FileNotFoundError, IndexError):
        return 0


# ---------------------------------------------------------------------------
# Triple / evidence helpers (unchanged behaviour)
# ---------------------------------------------------------------------------
def norm(line: str) -> str:
    """Canonicalize a triple line without altering literal content."""
    s = line.strip()
    s = re.sub(r"\.\s*$", "", s)
    return s.strip()


def hkey(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def ic_versions(ic_dir: Path) -> list[int]:
    """Sorted 1-based snapshot numbers from alldata.IC.nt file names."""
    out = []
    for p in ic_dir.glob("*.nt"):
        m = re.match(r"0*(\d+)\.nt$", p.name)
        if m:
            out.append(int(m.group(1)))
    return sorted(out)


_ev_cache: dict[str, dict | None] = {}
_claim_cache: dict[tuple[str, str], dict | None] = {}


def _evidence(ds: str) -> dict | None:
    """Per-triple true validity window from the reference IC snapshots."""
    if ds in _ev_cache:
        return _ev_cache[ds]
    ic = RAW / ds / "alldata.IC.nt"
    versions = ic_versions(ic) if ic.is_dir() else []
    if not versions:
        return None
    evidence = {}
    for p in sorted(ic.glob("*.nt")):
        m = re.match(r"0*(\d+)\.nt$", p.name)
        if not m:
            continue
        ver = int(m.group(1))
        with open(p, encoding="utf-8", errors="replace") as f:
            for line in f:
                if not line.strip() or line.lstrip().startswith("#"):
                    continue
                t = norm(line)
                k = hkey(t)
                rec = evidence.get(k)
                if rec is None:
                    evidence[k] = {"min": ver, "max": ver}
                else:
                    rec["max"] = max(rec["max"], ver)
    ctx = {"evidence": evidence, "last_v": len(versions) - 1}
    _ev_cache[ds] = ctx
    return ctx


def load_claims(ds: str, tbfile: str) -> dict | None:
    """Per-triple claimed validity window {min,max,rows,t} from one TB file."""
    key = (ds, tbfile)
    if key in _claim_cache:
        return _claim_cache[key]
    tb = RAW / ds / tbfile
    if not tb.exists():
        _claim_cache[key] = None
        return None
    claims = {}
    with open(tb, encoding="utf-8") as f:
        for line in f:
            m = GRAPH_RE.search(line)
            if not m:
                continue
            if m.start() == 0:
                # owl:versionInfo metadata statement (version IRI is the subject);
                # not a dataset triple, so it must not count as a claim.
                continue
            vers = tuple(int(x) for x in m.group(1).split("_"))
            t = norm(line[: m.start()])
            k = hkey(t)
            rec = claims.get(k)
            if rec is None:
                claims[k] = {"min": vers[0], "max": vers[-1], "rows": 1, "t": t}
            else:
                if rec["rows"] == 1:
                    rec["t"] = t
                rec["min"] = min(rec["min"], vers[0])
                rec["max"] = max(rec["max"], vers[-1])
                rec["rows"] += 1
    _claim_cache[key] = claims
    return claims


def load_dataset(ds: str) -> dict | None:
    """Compatibility wrapper: claims from the first TB file + IC evidence."""
    ev = _evidence(ds)
    claims = load_claims(ds, TB_FILES[0])
    if ev is None or claims is None:
        return None
    return {"claims": claims, "evidence": ev["evidence"], "last_v": ev["last_v"]}


def _write_outset(ds: str, name: str, triples: list[str], log,
                  write_empty: bool = False) -> None:
    """Write a set of triples to result_sets/<name>_<ds>.nt."""
    if not triples and not write_empty:
        log.info("%s finish: %s (none written)", name, ds)
        return
    RESULT_SETS.mkdir(parents=True, exist_ok=True)
    DQ_RESULT_SETS.mkdir(parents=True, exist_ok=True)
    out = DQ_RESULT_SETS / f"{name}_{ds}.nt"
    with open(out, "w", encoding="utf-8") as w:
        for t in triples:
            w.write(t + " .\n")
    log.info("%s finish: %s wrote %d triples -> %s", name, ds, len(triples), out)


# ---------------------------------------------------------------------------
# 1. invalid BEARA triples
# ---------------------------------------------------------------------------
def check_invalid_beara(log) -> None:
    log.info("invalid BEARA triples start: beara")
    ic = RAW / "beara" / "alldata.IC.nt"
    if not ic.is_dir():
        log.info("invalid BEARA triples finish: beara (skipped)")
        return
    tot_invalid = tot_lines = n_ver = 0
    for p in sorted(ic.glob("*.nt")):
        n_ver += 1
        invalid = 0
        with open(p, encoding="utf-8", errors="replace") as f:
            first = f.readline()
            m = re.search(r"# invalid_lines_excluded:\s*(\d+)", first)
            invalid = int(m.group(1)) if m else 0
            total = 1 + sum(1 for _ in f)
        tot_invalid += invalid
        tot_lines += total
    ratio = (tot_invalid / tot_lines * 100) if tot_lines else 0.0
    log.info("invalid BEARA triples finish: beara %d/%d invalid lines "
             "(%.2f%%, %d versions)", tot_invalid, tot_lines, ratio, n_ver)


# ---------------------------------------------------------------------------
# 2. erroneous version strings (overstated + shattered)
#    Returns a list of per-dataset dicts used to build dq_report.csv.
# ---------------------------------------------------------------------------
def _erroneous_for(ds: str) -> dict | None:
    """Compute overstated / shattered for one dataset (or None if unusable)."""
    ev = _evidence(ds)
    claims = load_claims(ds, TB_FILES[0])
    if ev is None or claims is None:
        return None
    evidence, last_v = ev["evidence"], ev["last_v"]
    overstated = shattered = 0
    for rec in claims.values():
        if rec["max"] >= last_v:
            e = evidence.get(hkey(rec["t"]))
            if e and e["max"] < last_v:
                overstated += 1
        if rec["rows"] >= 2:
            shattered += 1
    return {
        "ds": ds,
        "triples": len(claims),
        "overstated": overstated,
        "shattered": shattered,
    }


def check_erroneous(log) -> list[dict]:
    """Classify erratic version strings and write the per-dataset triple sets."""
    metrics = []
    for ds in DATASETS:
        log.info("erroneous version strings start: %s", ds)
        m = _erroneous_for(ds)
        if m is None:
            log.info("erroneous version strings finish: %s (skipped)", ds)
            metrics.append(None)
            continue
        log.info("erroneous version strings finish: %s "
                 "(overstated=%d, shattered=%d)", ds, m["overstated"], m["shattered"])
        write_overstated_candidates(ds, log)
        write_overstated(ds, log)
        write_shattered(ds, log)
        metrics.append(m)
    return metrics


# ---------------------------------------------------------------------------
# 3. inconsistent change sets — a generic per-dataset count + a row table
# ---------------------------------------------------------------------------
def _ic_count(ic: Path, ver: int) -> int | None:
    p = ic / f"{ver:06d}.nt"
    if not p.exists():
        return None
    with open(p, encoding="utf-8", errors="replace") as f:
        return sum(1 for ln in f if ln.strip() and not ln.lstrip().startswith("#"))


def _cb_count(ref: Path, lo: int, hi: int, kind: str) -> int:
    p = ref / f"data-{kind}_{lo}-{hi}.nt"
    if not p.exists():
        return 0
    with open(p, encoding="utf-8", errors="replace") as f:
        return sum(1 for ln in f if ln.strip() and not ln.lstrip().startswith("#"))


def diff_set_rows(ds: str) -> tuple[list[list[str]], int, int] | None:
    """Return (rows, consistent, inconsistent) for a dataset (or None if absent)."""
    base = RAW / ds
    ref = base / "alldata.CB.nt"
    ic = base / "alldata.IC.nt"
    if not (ref.is_dir() and ic.is_dir()):
        return None
    rows = []
    consistent = inconsistent = 0
    for hi in range(2, len(ic_versions(ic)) + 1):
        lo = hi - 1
        icp, icc = _ic_count(ic, lo), _ic_count(ic, hi)
        if icp is None or icc is None:
            continue
        added, deleted = _cb_count(ref, lo, hi, "added"), _cb_count(ref, lo, hi, "deleted")
        net_added = added - deleted
        ic_diff = icc - icp
        ok = icp + net_added == icc
        if ok:
            consistent += 1
        else:
            inconsistent += 1
        rows.append([f"v{lo - 1}->v{hi - 1}", icc, icp, added, deleted,
                     net_added, ic_diff, "yes" if ok else "NO"])
    return rows, consistent, inconsistent


def count_inconsistent_change_sets(ds: str) -> int:
    """Number of consecutive-snapshot pairs whose change sets do not reconcile."""
    out = diff_set_rows(ds)
    if out is None:
        return 0
    return out[2]


def check_diff_sets(log) -> list[dict]:
    """Compute inconsistent change-set counts per dataset (no markdown table)."""
    counts = {}
    for ds in DATASETS:
        log.info("inconsistent change sets start: %s", ds)
        out = diff_set_rows(ds)
        if out is None:
            log.info("inconsistent change sets finish: %s (skipped)", ds)
            counts[ds] = None
            continue
        rows, consistent, inconsistent = out
        log.info("inconsistent change sets finish: %s %d pairs, "
                 "%d consistent, %d inconsistent", ds, len(rows), consistent, inconsistent)
        counts[ds] = inconsistent
    return counts


def write_change_sets_csv(log) -> None:
    """Write the inconsistent-diff-set tables for BEARB_hour and BEARB_day."""
    header = ["Version", "IC(cur)", "IC(prev)", "Added", "Deleted",
              "Net added", "Diff IC", "Consistent?"]
    MEASURE.mkdir(parents=True, exist_ok=True)
    for ds, path in (("bearb_hour", CHANGE_SETS_CSV),
                     ("bearb_day", CHANGE_SETS_DAY_CSV)):
        log.info("inconsistent change sets csv start: %s", ds)
        out = diff_set_rows(ds)
        if out is None:
            log.info("inconsistent change sets csv: %s skipped", ds)
            continue
        rows, _, _ = out
        with open(path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(header)
            w.writerows(rows)
        log.info("inconsistent change sets csv: wrote %s", path)


# ---------------------------------------------------------------------------
# 4. impact on DB size — ingest orig + corrected into GraphDB & Jena TDB2
# ---------------------------------------------------------------------------
def ingest_dq_repos(log) -> None:
    """Bulk-load orig and corrected TB datasets into 8 repos under databases/dq."""
    log.info("impact ingest start")
    DQ_DB.mkdir(parents=True, exist_ok=True)
    for store in STORES:
        mgmt = MGMT_SCRIPT[store]
        for ds in DATASETS:
            for variant, tbfile in (("orig", TB_FILES[0]), ("computed", TB_FILES[1])):
                repo_dir = DQ_DB / store / f"tb_sr_ng_{ds}_{variant}"
                datafile = RAW / ds / tbfile
                log.info("impact ingest %s %s %s -> %s", store, ds, variant, repo_dir)
                subprocess.run(
                    [str(mgmt), "create_env", "tb_sr_ng", ds, str(repo_dir),
                     str(CONFIG_TMPL_DIR), str(CONFIG_DIR)],
                    check=True,
                )
                subprocess.run(
                    [str(mgmt), "ingest", str(repo_dir), str(datafile),
                     "tb_sr_ng", ds, str(CONFIG_DIR)],
                    check=True,
                )
    log.info("impact ingest finish: 8 repositories")


def measure_dq_sizes() -> dict[str, dict[str, int]]:
    """Measure raw-file and per-store DB sizes for every dataset/variant."""
    sizes: dict[str, dict[str, int]] = {}
    for ds in DATASETS:
        sizes[ds] = {}
        for variant, tbfile in (("orig", TB_FILES[0]), ("computed", TB_FILES[1])):
            raw_size = _size(RAW / ds / tbfile)
            sizes[ds][f"raw_{variant}"] = raw_size
            for store in STORES:
                repo_dir = DQ_DB / store / f"tb_sr_ng_{ds}_{variant}"
                sizes[ds][f"{store}_{variant}"] = _size(repo_dir)
    return sizes


# ---------------------------------------------------------------------------
# dq_report.csv
# ---------------------------------------------------------------------------
def write_report_csv(rows: list[dict], log) -> None:
    MEASURE.mkdir(parents=True, exist_ok=True)
    with open(REPORT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=REPORT_CSV_HEADER)
        w.writeheader()
        for r in rows:
            w.writerow(r)
    log.info("Wrote DQ report to %s", REPORT_CSV)


# ---------------------------------------------------------------------------
# dq_report.tex — built by reading dq_report.csv
# ---------------------------------------------------------------------------
def write_report_tex(log) -> None:
    """Render dq_report.csv as the pivoted LaTeX table (issues + storage impact)."""
    log.info("dq_report tex start")
    if not REPORT_CSV.exists():
        log.info("dq_report tex: %s missing", REPORT_CSV)
        return
    with open(REPORT_CSV, newline="", encoding="utf-8") as f:
        data = list(csv.DictReader(f))

    def cell(row: dict, n: str) -> str:
        return f"{int(row[n]):,}" if row.get(n) not in (None, "") else "-"

    def corr(row: dict, orig: str, corr_n: str) -> str:
        """Corrected value, bolded when it is smaller than the original."""
        v = cell(row, corr_n)
        o = row.get(orig)
        c = row.get(corr_n)
        if o not in (None, "") and c not in (None, "") and int(c) < int(o):
            return r"\textbf{" + v + "}"
        return v

    body = []
    for row in data:
        ds = DATASET_LABELS.get(row["Dataset"], row["Dataset"]).replace("_", r"\_")
        body.append(
            r"\texttt{" + ds + "}"
            " & " + cell(row, "triples_overstated_validity")
            + " & " + cell(row, "triples_shattered_vers_str")
            + " & " + cell(row, "cnt_inconsistent_change_sets")
            + " & " + cell(row, "raw_size_orig")
            + " & " + corr(row, "raw_size_orig", "raw_size_corrected")
            + " & " + cell(row, "graphdb_size_orig")
            + " & " + corr(row, "graphdb_size_orig", "graphdb_size_corrected")
            + " & " + cell(row, "jenatdb2_size_orig")
            + " & " + corr(row, "jenatdb2_size_orig", "jenatdb2_size_corrected")
            + r" \\"
        )

    lines = [
        r"\begin{table}[ht]",
        r"\centering",
        r"\caption{BEAR dataset issues and storage impact of the updated BEAR "
        r"datasets for the \texttt{tb\_ng} variant.}",
        r"\label{tab:dq_report}",
        r"\footnotesize",
        r"\setlength{\tabcolsep}{4pt}",
        r"\begin{tabular}{@{}l",
        r"                rrr",
        r"                |rr",
        r"                rr",
        r"                rr@{}}",
        r"\toprule",
        r"& \multicolumn{3}{c|}{\textbf{Dataset issues}}",
        r"& \multicolumn{6}{c}{\textbf{Storage size (MiB)}} \\",
        r"\cmidrule(lr){2-4}",
        r"\cmidrule(l){5-10}",
        r"",
        r"\textbf{Dataset}",
        r"& \multirow{2}{*}{\shortstack{\# Triples with\\Overstated\\Validity}}",
        r"& \multirow{2}{*}{\shortstack{\# Triples with\\Shattered\\Version Strings}}",
        r"& \multirow{2}{*}{\shortstack{Inconsistent\\change sets}}",
        r"& \multicolumn{2}{c}{\textbf{Raw}}",
        r"& \multicolumn{2}{c}{\textbf{GraphDB}}",
        r"& \multicolumn{2}{c}{\textbf{Jena}} \\",
        r"\cmidrule(lr){5-6}",
        r"\cmidrule(lr){7-8}",
        r"\cmidrule(l){9-10}",
        r"\addlinespace[3pt]",
        r"& & & & Orig & Uptd & Orig & Uptd & Orig & Uptd \\",
        r"\midrule",
    ] + body + [
        r"\bottomrule",
        r"\end{tabular}",
        r"\par\vspace{2pt}",
        r"\setlength{\tabcolsep}{6pt}",
        r"\end{table}",
        "",
    ]
    TABLES.mkdir(parents=True, exist_ok=True)
    REPORT_TEX.write_text("\n".join(lines), encoding="utf-8")
    log.info("dq_report tex: wrote %s", REPORT_TEX)


# ---------------------------------------------------------------------------
# result-set writers (kept for the paper's example triples)
# ---------------------------------------------------------------------------
def write_overstated_candidates(ds: str, log) -> None:
    """Write overstated candidates: triples whose TRUE window ends before last."""
    log.info("Overstated candidates start: %s", ds)
    ctx = load_dataset(ds)
    if ctx is None:
        log.info("Overstated candidates finish: %s (skipped)", ds)
        return
    claims, evidence, last_v = ctx["claims"], ctx["evidence"], ctx["last_v"]
    triples = [rec["t"] for rec in claims.values()
               if (e := evidence.get(hkey(rec["t"]))) and e["max"] < last_v]
    _write_outset(ds, "overstated_candidate_triples", triples, log, write_empty=True)


def write_overstated(ds: str, log) -> None:
    """Write actually overstated triples: claim reaches last but truth ends early."""
    log.info("Overstated triples start: %s", ds)
    ctx = load_dataset(ds)
    if ctx is None:
        log.info("Overstated triples finish: %s (skipped)", ds)
        return
    claims, evidence, last_v = ctx["claims"], ctx["evidence"], ctx["last_v"]
    triples = [rec["t"] for rec in claims.values()
               if rec["max"] >= last_v
               and (e := evidence.get(hkey(rec["t"]))) and e["max"] < last_v]
    _write_outset(ds, "overstated_triples", triples, log, write_empty=True)


def write_shattered(ds: str, log) -> None:
    """Write shattered triples (unique triple split across >1 version string)."""
    log.info("Shattered triples start: %s", ds)
    claims = load_claims(ds, TB_FILES[0])
    if claims is None:
        log.info("Shattered triples finish: %s (skipped)", ds)
        return
    triples = [rec["t"] for rec in claims.values() if rec["rows"] >= 2]
    _write_outset(ds, "shattered_triples", triples, log, write_empty=True)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main() -> None:
    _, log = setup_logging("dq_report")

    MEASURE.mkdir(parents=True, exist_ok=True)

    if not RAW.is_dir():
        log.info("No rawdata found under %s. Nothing to scan.", RAW)
        return

    log.info("run: %s", RUN_DIR.name)
    log.info("rawdata: %s", RAW)

    # 1. invalid BEARA triples (log only)
    check_invalid_beara(log)

    # 2. erroneous version strings (per-dataset metrics + result sets)
    metrics = check_erroneous(log)

    # 3. inconsistent change sets (per-dataset counts + BEARB_hour CSV table)
    change_counts = check_diff_sets(log)
    write_change_sets_csv(log)

    # 4. impact on DB size — ingest orig + corrected into both stores, measure
    ingest_dq_repos(log)
    db_sizes = measure_dq_sizes()

    # Build the per-dataset rows for dq_report.csv.
    rows = []
    for m in metrics:
        if m is None:
            continue
        ds = m["ds"]
        rows.append({
            "Dataset": ds,
            "triples_total": m["triples"],
            "triples_overstated_validity": m["overstated"],
            "triples_shattered_vers_str": m["shattered"],
            "cnt_inconsistent_change_sets": change_counts.get(ds, 0),
            "raw_size_orig": db_sizes[ds]["raw_orig"],
            "raw_size_corrected": db_sizes[ds]["raw_computed"],
            "graphdb_size_orig": db_sizes[ds]["graphdb_orig"],
            "graphdb_size_corrected": db_sizes[ds]["graphdb_computed"],
            "jenatdb2_size_orig": db_sizes[ds]["jenatdb2_orig"],
            "jenatdb2_size_corrected": db_sizes[ds]["jenatdb2_computed"],
        })

    write_report_csv(rows, log)
    write_report_tex(log)

    log.info("DQ report finished")


if __name__ == "__main__":
    main()
