# scripts/parse_eval_output.py
import re
from pathlib import Path
import csv
from typing import List

# Markers used by ostrich-evaluate
M_INSERT_START = re.compile(r"^---INSERTION START---")
M_INSERT_END   = re.compile(r"^---INSERTION END---")
M_QUERIES_START= re.compile(r"^---QUERIES START: (.+)---")
M_QUERIES_END  = re.compile(r"^---QUERIES END---")
M_PATTERN_START= re.compile(r"^---PATTERN START: (.+)$")
M_VM           = re.compile(r"^--- ---VERSION MATERIALIZED")
M_DM           = re.compile(r"^--- ---DELTA MATERIALIZED")
M_VQ           = re.compile(r"^--- ---VERSION$")

def _collect_table(lines, start_idx):
    """
    Collect a simple CSV-like block starting at start_idx (header line),
    until an empty line or a line starting with '---'.
    """
    out = []
    i = start_idx
    # header
    header = lines[i].strip()
    out.append(header)
    i += 1
    while i < len(lines):
        line = lines[i].rstrip("\n")
        if not line or line.startswith("---"):
            break
        out.append(line)
        i += 1
    return out, i

def _write_csv(path: Path, rows):
    """
    Parsed die eingehenden Zeilen (primär Komma-getrennt, Fallbacks bei Bedarf)
    und schreibt sie als korrekt getrennte CSV mit **Semikolon** als Delimiter
    unter genau `path`.
    """
    import csv
    import re

    # falls im Input „kommaartige“ Zeichen vorkommen, auf echtes Komma normalisieren
    def normalize_commas(s: str) -> str:
        return s.translate(str.maketrans({
            "，": ",", "‚": ",", "﹐": ",", "､": ",",
        }))

    parsed_rows = []
    for raw in rows:
        line = normalize_commas(raw)

        # 1) bevorzugt Komma-Parsing
        rec = next(csv.reader([line], delimiter=",", quotechar='"'))

        # 2) Fallbacks, falls nur 1 Spalte rauskommt
        if len(rec) == 1:
            alt = next(csv.reader([line], delimiter=";", quotechar='"'))
            if len(alt) > 1:
                rec = alt
            else:
                alt2 = next(csv.reader([line], delimiter="\t", quotechar='"'))
                if len(alt2) > 1:
                    rec = alt2
                else:
                    spl = re.split(r"\s{2,}", line.strip())
                    if len(spl) > 1:
                        rec = spl

        parsed_rows.append([c.strip() for c in rec])

    # einziges Output: Semikolon-getrennte CSV unter dem Originalpfad
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=";", lineterminator="\n", quoting=csv.QUOTE_MINIMAL)
        writer.writerows(parsed_rows)

def parse_eval_stdout_to_csv(log_path: Path, insertion_csv: Path,
                             vm_csv: Path, dm_csv: Path, vq_csv: Path):
    txt = Path(log_path).read_text(encoding="utf-8", errors="ignore")
    lines = txt.splitlines()

    # states
    i = 0
    insert_rows = []
    vm_rows = []
    dm_rows = []
    vq_rows = []

    while i < len(lines):
        line = lines[i]

        if M_INSERT_START.match(line):
            # next line should be header
            i += 1
            block, i = _collect_table(lines, i)
            insert_rows.extend(block)
            continue

        if M_QUERIES_START.match(line):
            # inside queries block, we may see multiple patterns,
            # each with VM/DM/VQ tables; we just append all
            i += 1
            while i < len(lines) and not M_QUERIES_END.match(lines[i]):
                if M_PATTERN_START.match(lines[i]):
                    i += 1
                    continue
                if M_VM.match(lines[i]):
                    i += 1
                    block, i = _collect_table(lines, i)
                    if vm_rows:
                        vm_rows.extend(block[1:])  # drop header after first
                    else:
                        vm_rows.extend(block)
                    continue
                if M_DM.match(lines[i]):
                    i += 1
                    block, i = _collect_table(lines, i)
                    if dm_rows:
                        dm_rows.extend(block[1:])
                    else:
                        dm_rows.extend(block)
                    continue
                if M_VQ.match(lines[i]):
                    i += 1
                    block, i = _collect_table(lines, i)
                    if vq_rows:
                        vq_rows.extend(block[1:])
                    else:
                        vq_rows.extend(block)
                    continue
                i += 1
            # skip the QUERIES END line
            if i < len(lines) and M_QUERIES_END.match(lines[i]):
                i += 1
            continue

        i += 1

    # write CSVs
    if insert_rows:
        _write_csv(Path(insertion_csv), insert_rows)
    if vm_rows:
        _write_csv(Path(vm_csv), vm_rows)
    if dm_rows:
        _write_csv(Path(dm_csv), dm_rows)
    if vq_rows:
        _write_csv(Path(vq_csv), vq_rows)
