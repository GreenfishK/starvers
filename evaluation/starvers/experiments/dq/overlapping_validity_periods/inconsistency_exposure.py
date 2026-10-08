#!/usr/bin/env python3
"""
inconsistency_exposure.py
=========================
Finds ONE concrete example per erroneous version-string pattern in the BEAR-B
(day/hour) TB dataset variants that motivated the paper's decision *not* to
reuse them for the tb_sr_rs model:

  1. OVERSTATEMENT (overstating the validity period)
     The claimed version string extends to versions the triple is NOT evidenced
     in any snapshot; e.g. inserted after the first version and deleted before
     the last, yet the string claims validity all the way to the LAST version.

  2. MULTIPLICATION (fragmented instead of a continuous version string)
     The same (s,p,o) is carried under >= 2 distinct graph IRIs although it is
     genuinely evidenced in >= 2 snapshots -- i.e. multiplied instead of being
     merged into one continuous version string.

  3. BOTH (both patterns apply to the same triple).

For each pattern the script records a single representative triple, then writes a
human-readable report (Markdown) into the script's own directory:

    <script_dir>/inconsistency_report.md

Ground truth is the per-version snapshot set ``alldata.IC.nt/`` : file
``000NNN.nt`` corresponds to version ``NNN-1`` (000001 -> v0, 001299 -> v1298).

The file under test is the TB variant, e.g. ``alldata.TB.nq``, whose N-Quads
rows are ``<s> <p> <o> <http://example.org/v0_1_2_..._1298> .`` -- the graph
IRI is the *claimed* version list.

Triples are keyed by a hash of the (s,p,o) string so memory stays bounded even
for multi-million-triple files.

Usage
-----
python inconsistency_exposure.py \
    --snapshot-dir /mnt/data_local/starvers_eval/.../alldata.IC.nt \
    --tb-file      /mnt/data_local/starvers_eval/.../alldata.TB.nq

Optional:
    --dataset bearb_hour     # label used in the report
    --max-snapshots N        # only consider the first N snapshot files (fast mode)
    --max-triples N          # stop after N distinct triples (sanity check)
    --report FILE            # override report path (default: <script_dir>/inconsistency_report.md)
"""

import argparse
import hashlib
import re
import sys
from collections import defaultdict
from pathlib import Path


GRAPH_RE = re.compile(r"<http://example\.org/v(?P<vers>[0-9_]+)>")

ERROR_TYPES = ("overstatement", "multiplication", "both")


def parse_version_set(graph_iri: str) -> set[int]:
    """'v0_1_2_15' -> {0, 1, 2, 15}."""
    m = GRAPH_RE.search(graph_iri)
    if not m:
        return set()
    return {int(p) for p in m.group("vers").split("_") if p != ""}


def snapshot_version(path: Path) -> int:
    """000123.nt -> 122 (0-based version)."""
    return int(path.stem) - 1


def _key(triple: str) -> str:
    return hashlib.sha256(triple.encode("utf-8")).hexdigest()


def load_evidence(snapshot_dir: Path, max_snapshots: int | None = None,
                  progress: bool = False) -> dict[str, set[int]]:
    """
    Build triple-key -> set of versions in which it is observed (ground truth).
    Returns {key: {version,...}}.  With max_snapshots, only the first N snapshot
    files (lexical order = ascending version) are considered.
    """
    evidence = defaultdict(set)
    files = sorted(snapshot_dir.glob("*.nt"))
    if max_snapshots:
        files = files[:max_snapshots]
    for path in files:
        ver = snapshot_version(path)
        with open(path, encoding="utf-8") as f:
            for line in f:
                if line.startswith("#"):
                    continue
                s = line.strip()
                if not s:
                    continue
                triple = re.sub(r"\s*\.\s*$", "", s)
                triple = re.sub(r"\s+", " ", triple).strip()
                evidence[_key(triple)].add(ver)
        if progress and ver % 200 == 0:
            print(f"  [evidence] loaded version v{ver} ({path.name})", file=sys.stderr)
    return dict(evidence)


def classify(claim_versions: set[int], claim_graph_count: int,
             evidence: set[int]) -> str:
    """Classify a triple into overstatement / multiplication / both / ok."""
    overstatement = bool(claim_versions - evidence) and bool(evidence)
    multiplication = bool(evidence) and len(evidence) >= 2 and claim_graph_count >= 2
    if overstatement and multiplication:
        return "both"
    if overstatement:
        return "overstatement"
    if multiplication:
        return "multiplication"
    return "ok"


def find_examples(tb_file: Path, evidence: dict[str, set[int]],
                  max_triples: int | None = None) -> dict[str, dict]:
    """
    Scan the TB file and return a dict mapping each error type to a single
    example record.  Stops scanning as soon as all three patterns are found.
    """
    claimed_versions = defaultdict(set)
    claimed_graphs = defaultdict(set)
    examples = {}

    with open(tb_file, encoding="utf-8") as f:
        for line in f:
            if line.startswith("#"):
                continue
            s = line.strip()
            if not s:
                continue
            m = GRAPH_RE.search(s)
            if not m:
                continue
            vers = parse_version_set(m.group(0))
            triple = s[m.end():]
            triple = re.sub(r"\s*\.\s*$", "", triple)
            triple = re.sub(r"\s+", " ", triple).strip()
            k = _key(triple)

            claimed_versions[k].update(vers)
            claimed_graphs[k].add(m.group(0))

            ev = evidence.get(k, set())
            kind = classify(claimed_versions[k], len(claimed_graphs[k]), ev)
            if kind in ERROR_TYPES and kind not in examples:
                examples[kind] = {
                    "triple": triple,
                    "key": k,
                    "claimed_versions": sorted(claimed_versions[k]),
                    "claim_graph_count": len(claimed_graphs[k]),
                    "claim_graphs": sorted(claimed_graphs[k]),
                    "evidence_versions": sorted(ev),
                    "last_version": max(ev) if ev else None,
                }
                print(f"[*] found example for '{kind}'", file=sys.stderr)
                if len(examples) == len(ERROR_TYPES):
                    break
            if max_triples:
                if len(claimed_versions) >= max_triples:
                    print(f"[*] stopping at --max-triples={max_triples}",
                          file=sys.stderr)
                    break
    return examples


def _fmt_versions(vs: list[int], limit: int = 40) -> str:
    if not vs:
        return "(none)"
    head = ", ".join(map(str, vs[:limit]))
    tail = f", ... ({len(vs)} total)" if len(vs) > limit else ""
    return head + tail


def render_report(dataset: str, tb_file: Path, evidence_dir: Path,
                  examples: dict[str, dict], n_snapshots: int,
                  last_version: int) -> str:
    lines = []
    lines.append(f"# Inconsistency exposure report — `{dataset}`")
    lines.append("")
    lines.append(f"- **Dataset under test**: `{dataset}`")
    lines.append(f"- **TB file**: `{tb_file}`")
    lines.append(f"- **Ground-truth snapshots**: `{evidence_dir}` "
                 f"({n_snapshots} snapshots, versions 0..{last_version})")
    lines.append("")
    lines.append("## Error types")
    lines.append("")
    desc = {
        "overstatement": (
            "The claimed version string overstates the triple's validity period: "
            "it includes versions the triple is not evidenced in any snapshot. "
            "Typical form: a triple that does not extend to the last version is "
            "nevertheless claimed as valid through the last version."),
        "multiplication": (
            "The triple is multiplied: it is carried under multiple distinct "
            "version graphs instead of being merged into a single continuous "
            "version string. A triple valid in many snapshots deserves one "
            "continuous label, not N separate labels."),
        "both": (
            "Both the overstatement and the multiplication pattern apply to the "
            "same triple."),
    }
    for kind in ERROR_TYPES:
        lines.append(f"### {kind}")
        lines.append("")
        lines.append(desc[kind])
        lines.append("")
        ex = examples.get(kind)
        if not ex:
            lines.append("*No example found within the scanned data.*")
            lines.append("")
            continue
        lines.append(f"- **Version-graph count (multiplication signal)**: "
                     f"{ex['claim_graph_count']}")
        lines.append(f"- **Claimed versions** ({len(ex['claimed_versions'])}): "
                     f"`{_fmt_versions(ex['claimed_versions'])}`")
        lines.append(f"- **Evidence versions** ({len(ex['evidence_versions'])}): "
                     f"`{_fmt_versions(ex['evidence_versions'])}`")
        graph_disp = ex["claim_graphs"][0] if ex["claim_graphs"] else "(none)"
        if len(ex["claim_graphs"]) > 1:
            graph_disp = f"{graph_disp} ... ({len(ex['claim_graphs'])} graphs)"
        lines.append(f"- **Example graph IRI**: `{graph_disp}`")
        lines.append("")
        lines.append("**Example triple** (`<s> <p> <o>`):")
        lines.append("")
        lines.append("```")
        lines.append(ex["triple"][:400])
        lines.append("```")
        lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("_Generated by `inconsistency_exposure.py`._")
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    script_dir = Path(__file__).resolve().parent
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--snapshot-dir", required=True, type=Path,
                    help="directory of per-version snapshot .nt files (ground truth)")
    ap.add_argument("--tb-file", required=True, type=Path,
                    help="TB .nq file whose version strings are under test")
    ap.add_argument("--dataset", default="bearb_hour", help="report label")
    ap.add_argument("--max-snapshots", type=int, default=None,
                    help="only consider the first N snapshot files (fast mode)")
    ap.add_argument("--max-triples", type=int, default=None,
                    help="stop after N distinct triples (sanity check)")
    ap.add_argument("--report", type=Path,
                    default=script_dir / "inconsistency_report.md",
                    help="report path (default: <script_dir>/inconsistency_report.md)")
    ap.add_argument("--progress", action="store_true",
                    help="print per-version progress while loading snapshots")
    args = ap.parse_args()

    if not args.snapshot_dir.is_dir():
        sys.exit(f"snapshot dir not found: {args.snapshot_dir}")
    if not args.tb_file.is_file():
        sys.exit(f"tb file not found: {args.tb_file}")

    print(f"[*] Loading ground-truth evidence from {args.snapshot_dir}",
          file=sys.stderr)
    evidence = load_evidence(args.snapshot_dir, args.max_snapshots, args.progress)
    n_snapshots = len(set(v for s in evidence.values() for v in s))
    last_version = max((v for s in evidence.values() for v in s), default=1298)
    print(f"[*] Last version = v{last_version}; snapshots = {n_snapshots}",
          file=sys.stderr)

    print(f"[*] Scanning TB file {args.tb_file} for examples", file=sys.stderr)
    examples = find_examples(args.tb_file, evidence, args.max_triples)

    report = render_report(args.dataset, args.tb_file, args.snapshot_dir,
                           examples, n_snapshots, last_version)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(report, encoding="utf-8")

    print()
    print("================== SUMMARY ==================")
    print(f"dataset        : {args.dataset}")
    print(f"last version   : v{last_version}")
    print(f"snapshots      : {n_snapshots}")
    print(f"examples found : {len(examples)}/{len(ERROR_TYPES)}")
    for kind in ERROR_TYPES:
        print(f"  {kind:<15}: "
              f"{'YES' if kind in examples else 'no '}  "
              f"({(examples[kind]['key'][:12] + '...') if kind in examples else ''})")
    print(f"report written : {args.report}")


if __name__ == "__main__":
    main()
