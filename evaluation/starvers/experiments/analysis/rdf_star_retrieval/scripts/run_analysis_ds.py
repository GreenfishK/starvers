#!/usr/bin/env python3
"""Ingest the artificial analysis dataset (tb_sr_rs decorator model) into a
dedicated GraphDB + Jena repository under databases/analysis, then capture the
execution plans, writing evidence files to --out.

Run INSIDE the starvers_eval container (root).
"""
import argparse
import os
import re
import subprocess
import time
import urllib.parse
import urllib.request
import urllib.error
from pathlib import Path

RUN = Path(os.environ["RUN_DIR"])
POLICY = "tb_sr_rs"
DATASET = "analysis"
REPO = f"{POLICY}_{DATASET}"

MGMT_GRAPHDB = "/starvers_eval/scripts/triple_store_mgmt/graphdb_mgmt.sh"
MGMT_JENA = "/starvers_eval/scripts/triple_store_mgmt/jenatdb2_mgmt.sh"
CONFIG_TMPL = "/starvers_eval/scripts/4_ingest/configs"
CONFIG_OUT = RUN / "configs" / "analysis"
DB_BASE = RUN / "databases" / "analysis"
DATA_FILE = RUN / "rawdata" / "analysis" / "analysis_ds.ttl"
GET_GRAPHDB = "http://Starvers:7200/repositories/{repo}"
JENA_JAR = "/jena-fuseki/fuseki-server.jar"
JAVA = "/opt/java/java17/openjdk"

QUERY = """PREFIX vers: <https://github.com/GreenfishK/DataCitation/versioning/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
SELECT ?s{
<< <<?s <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://dbpedia.org/ontology/Film>>> vers:valid_from ?valid_from_1 >> vers:valid_until ?valid_until_1.
filter(?valid_from_1 <= ?tsBGP_0 && ?tsBGP_0 < ?valid_until_1)
bind("2022-10-01T12:00:00.000+00:00"^^<http://www.w3.org/2001/XMLSchema#dateTime> as ?tsBGP_0)}
"""


def run(cmd, **kw):
    return subprocess.run(cmd, capture_output=True, text=True, **kw)


def _strip_result_set(text: str) -> str:
    """Remove the query result set (tabular output) from the --explain output."""
    lines = text.splitlines()
    cut = None
    for i, line in enumerate(lines):
        if "INFO  exec" in line and "Execute" in line:
            for j in range(i + 1, len(lines)):
                if lines[j].lstrip().startswith("-") and set(lines[j].strip()) == {"-"}:
                    cut = j
                    break
            break
    if cut is not None:
        return "\n".join(lines[:cut]) + "\n"
    return text


def sh(cmd):
    print("  $", " ".join(map(str, cmd)), flush=True)
    r = subprocess.run(list(map(str, cmd)), capture_output=True, text=True)
    if r.stdout:
        print(r.stdout[-800:], flush=True)
    if r.returncode != 0:
        print("  [rc]", r.returncode, r.stderr[-800:], flush=True)
    return r


def post(endpoint, query):
    data = urllib.parse.urlencode({"query": query}).encode()
    req = urllib.request.Request(endpoint, data=data, method="POST")
    with urllib.request.urlopen(req, timeout=120) as r:
        return r.read().decode("utf-8", "replace")


def wait_pid(pidfile, tries=30):
    pidfile = Path(pidfile)
    for _ in range(tries):
        if pidfile.exists():
            return
        time.sleep(2)
    raise RuntimeError(f"no pidfile {pidfile}")


def graphdb_plan(out_dir):
    print("=== GraphDB ===", flush=True)
    db = DB_BASE / "graphdb" / REPO
    sh([MGMT_GRAPHDB, "--log-file", str(RUN/"output/logs/graphdb_mgmt_analysis.txt"),
        "create_env", POLICY, DATASET, str(db), CONFIG_TMPL, str(CONFIG_OUT)])
    sh([MGMT_GRAPHDB, "--log-file", str(RUN/"output/logs/graphdb_mgmt_analysis.txt"),
        "ingest", str(db), str(DATA_FILE), POLICY, DATASET, str(CONFIG_OUT)])
    sh([MGMT_GRAPHDB, "--log-file", str(RUN/"output/logs/graphdb_mgmt_analysis.txt"),
        "startup", str(db), POLICY, DATASET, str(CONFIG_OUT)])
    wait_pid(f"/tmp/graphdb_{POLICY}_{DATASET}.pid")
    endpoint = GET_GRAPHDB.format(repo=REPO)
    # explain injection
    m = re.search(r"\{", QUERY)
    eq = QUERY[:m.start()] + " FROM <http://www.ontotext.com/explain> WHERE " + QUERY[m.start():]
    eh = "PREFIX onto: <http://www.ontotext.com/>\n" + eq
    try:
        plan = post(endpoint, eh)
    except urllib.error.HTTPError as e:
        plan = f"HTTP {e.code}: {e.read().decode('utf-8','replace')}"
    out = out_dir / f"exp-graphdb-{POLICY}-{DATASET}.plan.txt"
    out.write_text(f"# GraphDB 10.5 explain-plan proof\n# repo: {REPO}\n# dataset: {DATA_FILE}\n# query:\n" +
                   "\n".join("  "+l for l in QUERY.splitlines()) + "\n\n" + plan)
    print("[ok]", out, flush=True)
    sh([MGMT_GRAPHDB, "--log-file", str(RUN/"output/logs/graphdb_mgmt_analysis.txt"), "shutdown"])
    try:
        sh(["pkill", "-9", "-f", "GraphDBServer"])
    except Exception:
        pass
    time.sleep(2)


def jena_plan(out_dir):
    print("=== Jena TDB2 ===", flush=True)
    db = DB_BASE / "jenatdb2" / REPO
    sh([MGMT_JENA, "--log-file", str(RUN/"output/logs/jenatdb2_mgmt_analysis.txt"),
        "create_env", POLICY, DATASET, str(db), CONFIG_TMPL, str(CONFIG_OUT)])
    sh([MGMT_JENA, "--log-file", str(RUN/"output/logs/jenatdb2_mgmt_analysis.txt"),
        "ingest", str(db), str(DATA_FILE), POLICY, DATASET, str(CONFIG_OUT)])
    qpath = f"/tmp/{REPO}.rq"
    Path(qpath).write_text(QUERY)
    env = dict(os.environ)
    env["JAVA_HOME"] = JAVA
    env["PATH"] = f"{JAVA}/bin:" + env.get("PATH", "")
    r = run([f"{JAVA}/bin/java", "-cp", JENA_JAR, "tdb2.tdbquery",
             "--loc", str(db), "--query", qpath, "--explain"], env=env)
    out = out_dir / f"exp-jena-{POLICY}-{DATASET}.plan.txt"
    out.write_text(f"# Jena TDB2 5.1 (ARQ) --explain proof\n# repo: {REPO}\n# query:\n" +
                   "\n".join("  "+l for l in QUERY.splitlines()) + "\n\n" + _strip_result_set(r.stdout or "") + (r.stderr or ""))
    print("[ok]", out, flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--store", default="both", choices=["graphdb", "jena", "both"])
    args = ap.parse_args()
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    if args.store in ("graphdb", "both"):
        graphdb_plan(out_dir)
    if args.store in ("jena", "both"):
        jena_plan(out_dir)
    print("done", flush=True)


if __name__ == "__main__":
    main()
