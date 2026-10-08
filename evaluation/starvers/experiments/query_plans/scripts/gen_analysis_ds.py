#!/usr/bin/env python3
import os
RUN = os.environ["RUN_DIR"]
RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
FILM = "http://dbpedia.org/ontology/Film"
VERS = "https://github.com/GreenfishK/DataCitation/versioning/"
TS = "http://www.w3.org/2001/XMLSchema#dateTime"

os.makedirs(f"{RUN}/rawdata/analysis", exist_ok=True)
out = f"{RUN}/rawdata/analysis/analysis_ds.ttl"
lines = []
for i in range(1, 9):
    lines.append(f"<http://example/registry> <http://example/contains> << <http://example/m{i}> <{RDF}type> <{FILM}> >> .")
for i in [1, 2]:
    s = f"http://example/m{i}"
    lines.append(
        f"<< << <{s}> <{RDF}type> <{FILM}> >> <{VERS}valid_from> \"2022-10-01T10:00:00Z\"^^<{TS}> >> "
        f"<{VERS}valid_until> \"9999-12-31T23:59:59Z\"^^<{TS}> ."
    )
for i in range(1, 9):
    for p in range(1, 13):
        lines.append(f"<http://example/m{i}> <http://example/p{p}> \"p{p}{i}\" .")
with open(out, "w") as f:
    f.write("\n".join(lines) + "\n")
print("wrote", out, len(lines), "lines")
