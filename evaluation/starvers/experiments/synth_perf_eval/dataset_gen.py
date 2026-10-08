#!/usr/bin/env python3
"""
dataset_gen.py — generate the synthetic 5M-triple datasets (D1, D2, D3) for the
Starvers Retrieval Evaluation (GraphDB vs Jena TDB2, decorator / tb_sr_rs model).

Each dataset contains 5,000,000 decorator-style RDF-star triples of the form

    << << <s> <rdf:type> <dbpedia:Film> >> <vers:valid_from> "vf"^^xsd:dateTime >>
       <vers:valid_until> "vu"^^xsd:dateTime > .

which is exactly the shape matched by the retrieval query Q:

    << <<?s rdf:type dbpedia:Film>> vers:valid_from "2022-10-01T12:00:00.000+00:00" >>
       vers:valid_until "9999-12-31T12:00:00.000+00:00" .

Dataset layout (host): <base>/data/<d1|d2|d3>/dataset.ttl
Logs:                  <base>/logs/dataset_gen.log

Run either on the host or inside the starvers_eval container:
    python experiments/synth_perf_eval/dataset_gen.py
The output base dir can be overridden with the SYNTH_PERF_EVAL_BASE env var
(default in-container: /starvers_eval/data/synth_perf_eval).
"""
import os
import sys
from pathlib import Path

from experiments.logging import setup_logging

BASE = Path(os.environ.get(
    "SYNTH_PERF_EVAL_BASE", "/starvers_eval/data/synth_perf_eval"))

DATA_DIR = BASE / "data"
LOG_FILE = BASE / "output" / "logs" / "dataset_gen" / "dataset_gen.log"

TOTAL = 5_000_000
N_MATCH = 10            # triples matching query Q exactly
N_BULK = TOTAL - N_MATCH

# ---- IRIs -------------------------------------------------------------------
RDF_TYPE = "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>"
FILM = "<http://dbpedia.org/ontology/Film>"
VERS = "https://github.com/GreenfishK/DataCitation/versioning/"
VALID_FROM = f"<{VERS}valid_from>"
VALID_UNTIL = f"<{VERS}valid_until>"
XSD_DT = "<http://www.w3.org/2001/XMLSchema#dateTime>"
FOAF_INTEREST = "<http://xmlns.com/foaf/0.1/interest>"
WIKIMEDIA = "<http://www.wikimedia.org>"

# ---- Timestamps (must match query Q textually) ------------------------------
T_MATCH_VF = "2022-10-01T12:00:00.000+00:00"   # 10 matching triples
T_MATCH_VU = "9999-12-31T12:00:00.000+00:00"
T_BULK_VF = "2025-10-01T12:00:00.000+00:00"    # D1/D2 bulk valid_from
T_D1_VU = "2026-12-31T12:00:00.000+00:00"      # D1 bulk valid_until
T_D3_VF = "2020-10-01T12:00:00.000+00:00"      # D3 bulk valid_from
T_D3_VU = "2021-12-31T12:00:00.000+00:00"      # D3 bulk valid_until


def subject(i: int) -> str:
    return f"<http://example.org/retrieval/s{i}>"


def line(s: str, p: str, o: str, vf: str, vu: str) -> str:
    return (f"<< << {s} {p} {o} >> {VALID_FROM} \"{vf}\"^^{XSD_DT} >> "
            f"{VALID_UNTIL} \"{vu}\"^^{XSD_DT} .\n")


def gen_d1() -> None:
    """10 matching films + 999,990 foaf:interest with valid_until=2026."""
    out = DATA_DIR / "d1" / "dataset.ttl"
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w") as f:
        for i in range(N_MATCH):
            f.write(line(subject(i), RDF_TYPE, FILM, T_MATCH_VF, T_MATCH_VU))
        for i in range(N_MATCH, TOTAL):
            f.write(line(subject(i), FOAF_INTEREST, WIKIMEDIA, T_BULK_VF, T_D1_VU))


def gen_d2() -> None:
    """10 matching films + 999,990 foaf:interest with valid_until=9999 (== Q)."""
    out = DATA_DIR / "d2" / "dataset.ttl"
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w") as f:
        for i in range(N_MATCH):
            f.write(line(subject(i), RDF_TYPE, FILM, T_MATCH_VF, T_MATCH_VU))
        for i in range(N_MATCH, TOTAL):
            f.write(line(subject(i), FOAF_INTEREST, WIKIMEDIA, T_BULK_VF, T_MATCH_VU))


def gen_d3() -> None:
    """10 matching films + 999,990 films with valid_until=2021 (all are films)."""
    out = DATA_DIR / "d3" / "dataset.ttl"
    out.parent.mkdir(parents=True, exist_ok=True)
    with open(out, "w") as f:
        for i in range(N_MATCH):
            f.write(line(subject(i), RDF_TYPE, FILM, T_MATCH_VF, T_MATCH_VU))
        for i in range(N_MATCH, TOTAL):
            f.write(line(subject(i), RDF_TYPE, FILM, T_D3_VF, T_D3_VU))


def count(lines_path: Path) -> int:
    with open(lines_path) as f:
        return sum(1 for _ in f)


def main() -> None:
    os.environ["RUN_DIR"] = str(BASE)
    _, logger = setup_logging("dataset_gen")
    logger.info("Retrieval base: %s", BASE)
    DATA_DIR.mkdir(parents=True, exist_ok=True)

    for name, gen in (("d1", gen_d1), ("d2", gen_d2), ("d3", gen_d3)):
        logger.info("Generating %s (%s triples) ...", name, TOTAL)
        gen()
        n = count(DATA_DIR / name / "dataset.ttl")
        logger.info("Wrote %s -> %s triples", name, n)

    logger.info("Done. Data in %s, log at %s", DATA_DIR, LOG_FILE)


if __name__ == "__main__":
    main()
