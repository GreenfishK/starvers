#!/usr/bin/env python3
"""
bear_dq.py
==========
Data-quality (DQ) report for the BEAR datasets. Scans the rawdata of the current
evaluation run (RUN_DIR) and reports four error types in a slim Markdown report.

Checks
------
1. INVALID BEARA TRIPLES — per-version count of invalid RDF lines in BEARA.
   Skipped when the BEARA rawdata directory is absent.
2. OVERSTATED VALIDITY PERIODS (BEARB_day, BEARB_hour) — a TB version string
   extends to the LAST version although the triple is genuinely deleted earlier.
3. SHATTERED VERSION STRINGS (BEARB_day, BEARB_hour) — a triple valid across
   many snapshots is split into several single-version graphs instead of one
   continuous version string.
4. INCONSISTENT DIFF SETS (BEARB_hour) — the reference change sets
   (alldata.CB.nt) do NOT reconcile with the reference IC snapshots
   (alldata.IC.nt) via  |IC(prev)| + |added| - |deleted| == |IC(cur)|.

Data layout (relative to <RUN_DIR>/rawdata)
------------------------------------------
    beara/alldata.IC.nt/{...}.nt        (optional)
    bearb_day/alldata.TB.nq, alldata.IC.nt
    bearb_hour/alldata.TB.nq, alldata.IC.nt, alldata.CB.nt

Report:  <RUN_DIR>/output/measurements/dq_report.md
Log:     <RUN_DIR>/output/logs/dq_report/dq_report.log  (via experiments.logging)
"""
import hashlib
import os
import re
from pathlib import Path

import tomli  # noqa: F401  (kept for parity with sibling scripts)

from experiments.logging import setup_logging

GRAPH_RE = re.compile(r"<http://example\.org/v([0-9_]+)>")

RUN_DIR = Path(os.environ["RUN_DIR"])
RAW = RUN_DIR / "rawdata"
MEASURE = RUN_DIR / "output" / "measurements"
REPORT = MEASURE / "dq_report.md"
RESULT_SETS = RUN_DIR / "output" / "result_sets"

DATASETS = ("bearb_day", "bearb_hour")


def norm(line: str) -> str:
    s = line.strip()
    s = re.sub(r"\s*\.\s*$", "", s)
    return re.sub(r"\s+", " ", s).strip()


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


# ---------------------------------------------------------------------------
# 1. invalid BEARA triples
# ---------------------------------------------------------------------------
def check_invalid_beara(log) -> list[str]:
    lines = ["### 1. Invalid BEARA triples"]
    ic = RAW / "beara" / "alldata.IC.nt"
    if not ic.is_dir():
        lines.append("- **skipped**: `rawdata/beara` not found.")
        return lines
    tot_invalid = tot_lines = 0
    n_ver = 0
    for p in sorted(ic.glob("*.nt")):
        n_ver += 1
        invalid = 0
        total = 0
        with open(p, encoding="utf-8", errors="replace") as f:
            first = f.readline()
            m = re.search(r"# invalid_lines_excluded:\s*(\d+)", first)
            invalid = int(m.group(1)) if m else 0
            total = 1 + sum(1 for _ in f)
        tot_invalid += invalid
        tot_lines += total
    ratio = (tot_invalid / tot_lines * 100) if tot_lines else 0.0
    log.info("BEARA: %d versions, %d/%d invalid lines (%.2f%%)",
             n_ver, tot_invalid, tot_lines, ratio)
    lines.append(f"- **versions**: {n_ver}")
    lines.append(f"- **invalid lines**: {tot_invalid} / {tot_lines} ({ratio:.2f}%)")
    lines.append("- _skipped if the BEARA rawdata directory is absent_")
    return lines


# ---------------------------------------------------------------------------
# 2 & 3. overstated / shattered version strings per dataset
# ---------------------------------------------------------------------------
def analyze_dataset(ds: str, log) -> list[str]:
    lines = [f"### 2/3. {ds}"]
    base = RAW / ds
    tb = base / "alldata.TB.nq"
    ic = base / "alldata.IC.nt"
    if not (tb.exists() and ic.is_dir()):
        lines.append("- **skipped**: missing `alldata.TB.nq` / `alldata.IC.nt`.")
        return lines

    versions = ic_versions(ic)
    last = versions[-1] if versions else None
    if last is None:
        lines.append("- **skipped**: no IC snapshots found.")
        return lines

    # One pass over TB.nq -> per-triple claimed {min,max,row count,triple}.
    claims = {}
    with open(tb, encoding="utf-8") as f:
        for line in f:
            m = GRAPH_RE.search(line)
            if not m:
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

    # One pass over the IC snapshots -> per-triple evidence {min,max} AND
    # per-version counts (reused by the diff-set check for this dataset).
    evidence = {}
    per_version = {}
    for p in sorted(ic.glob("*.nt")):
        m = re.match(r"0*(\d+)\.nt$", p.name)
        if not m:
            continue
        ver = int(m.group(1))
        cnt = 0
        with open(p, encoding="utf-8", errors="replace") as f:
            for line in f:
                if not line.strip() or line.lstrip().startswith("#"):
                    continue
                cnt += 1
                t = norm(line)
                k = hkey(t)
                rec = evidence.get(k)
                if rec is None:
                    evidence[k] = {"min": ver, "max": ver}
                else:
                    rec["max"] = max(rec["max"], ver)
        per_version[ver] = cnt

    log.info("%s: %d triples from TB.nq, %d triples in IC, last=v%d",
             ds, len(claims), len(evidence), len(versions) - 1)

    # Error 1 (overstated): TB.nq string reaches the LAST snapshot version but
    # the triple is genuinely deleted earlier.  Error 2 (shattered): a triple
    # valid across >1 snapshot is split into >=2 graph rows.
    #
    # Candidate triples: those whose TB.nq validity window starts after the
    # first snapshot and ends before the last (1-based: min > 0 and max < last).
    last_v = len(versions) - 1
    overstated = shattered = 0
    candidates = {}
    for k, rec in claims.items():
        ev = evidence.get(k)
        if rec["min"] > 0 and rec["max"] < last_v:
            candidates[k] = rec["t"]
        if not ev:
            continue
        if rec["max"] >= last_v and ev["max"] < last_v:
            overstated += 1
        if ev["max"] > ev["min"] and rec["rows"] >= 2:
            shattered += 1

    if candidates:
        RESULT_SETS.mkdir(parents=True, exist_ok=True)
        out = RESULT_SETS / f"candidate_triples{ds}.nt"
        with open(out, "w", encoding="utf-8") as w:
            for t in candidates.values():
                w.write(t + " .\n")
        log.info("%s: wrote %d candidate triples -> %s",
                 ds, len(candidates), out)
    else:
        log.info("%s: no candidate triples written", ds)

    lines.append(f"- **last version**: v{last_v} | **triples**: {len(claims)}")
    lines.append(f"- **overstated validity periods (Error 1)**: {overstated}")
    lines.append(f"  _(TB.nq string reaches v{last_v} but the triple is "
                 f"deleted earlier)_")
    lines.append(f"- **shattered version strings (Error 2)**: {shattered}")
    lines.append("  _(triple valid across snapshots is split into multiple "
                 "graphs instead of one continuous string)_")
    return lines


# ---------------------------------------------------------------------------
# 4. inconsistent diff sets (BEARB_hour): reference CB vs reference IC
# ---------------------------------------------------------------------------
def check_diff_sets(log) -> list[str]:
    lines = ["### 4. Inconsistent diff sets (BEARB_hour)"]
    base = RAW / "bearb_hour"
    ref = base / "alldata.CB.nt"
    ic = base / "alldata.IC.nt"
    if not (ref.is_dir() and ic.is_dir()):
        lines.append("- **skipped**: `alldata.CB.nt` or `alldata.IC.nt` not found.")
        return lines

    name_re = re.compile(r"data-added_(\d+)-(\d+)\.nt$")
    pairs = sorted((int(m2.group(1)), int(m2.group(2)))
                   for m2 in (name_re.match(p.name) for p in ref.glob("data-added_*.nt"))
                   if m2)

    def ic_count(ver: int) -> int | None:
        p = ic / f"{ver:06d}.nt"
        if not p.exists():
            return None
        with open(p, encoding="utf-8", errors="replace") as f:
            return sum(1 for ln in f if ln.strip() and not ln.lstrip().startswith("#"))

    def cb_count(lo: int, hi: int, kind: str) -> int:
        p = ref / f"data-{kind}_{lo}-{hi}.nt"
        if not p.exists():
            return 0
        with open(p, encoding="utf-8", errors="replace") as f:
            return sum(1 for ln in f if ln.strip() and not ln.lstrip().startswith("#"))

    consistent = inconsistent = 0
    first_bad = None
    for lo, hi in pairs:
        icp, icc = ic_count(lo), ic_count(hi)
        if icp is None or icc is None:
            continue
        added, deleted = cb_count(lo, hi, "added"), cb_count(lo, hi, "deleted")
        if icp + added - deleted == icc:
            consistent += 1
        else:
            inconsistent += 1
            if first_bad is None:
                first_bad = (lo, hi, icp, icc, added, deleted)

    log.info("Diff sets: %d pairs, %d consistent, %d inconsistent",
             len(pairs), consistent, inconsistent)
    lines.append(f"- **version pairs checked**: {len(pairs)}")
    lines.append(f"- **consistent**: {consistent}")
    lines.append(f"- **inconsistent**: {inconsistent}")
    if first_bad:
        lo, hi, icp, icc, add, dele = first_bad
        lines.append(f"- **first inconsistent pair**: v{lo - 1}->v{hi - 1} "
                     f"(IC {icp} -> {icc}, added {add}, deleted {dele})")
    lines.append("- _the reference change sets do not reconcile with the "
                 "reference IC snapshots_")
    return lines


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------
def main() -> None:
    _, log = setup_logging("dq_report")

    MEASURE.mkdir(parents=True, exist_ok=True)
    report = ["# BEAR Data Quality report", ""]

    if not RAW.is_dir():
        report.append(f"No rawdata found under `{RAW}`. Nothing to scan.")
    else:
        report.append(f"- **run**: `{RUN_DIR.name}`")
        report.append(f"- **rawdata**: `{RAW}`")
        report.append("")

        report += check_invalid_beara(log)

        for ds in DATASETS:
            if (RAW / ds / "alldata.TB.nq").exists():
                report += analyze_dataset(ds, log)
            else:
                report.append(f"### 2/3. {ds}")
                report.append("- **skipped**: `alldata.TB.nq` not found.")
                report.append("")

        report += check_diff_sets(log)

    REPORT.write_text("\n".join(report) + "\n", encoding="utf-8")
    log.info("Wrote DQ report to %s", REPORT)
    print("\n".join(report))


if __name__ == "__main__":
    main()
