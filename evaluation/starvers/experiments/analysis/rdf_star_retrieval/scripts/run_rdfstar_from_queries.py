#!/usr/bin/env python3
"""Capture GraphDB onto:explain and Jena tdb2.tdbquery --explain plans for the
real bearc tb_sr_rs / tb_sr_re retrieval queries.

Must run INSIDE the starvers_eval container."""
import argparse
import os
import subprocess
import time
import urllib.parse
import urllib.request
from pathlib import Path

RUN = Path(os.environ.get("RUN_DIR", "/starvers_eval/data/20260426T15-22-09.348"))
GRAPHDB = {
    "mgmt": "/starvers_eval/scripts/triple_store_mgmt/graphdb_mgmt.sh",
    "get": "http://Starvers:7200/repositories/{repo}",
}
JENA = {"jar": "/jena-fuseki/fuseki-server.jar", "java": "/opt/java/java17/openjdk"}
CONFIG_DIR = RUN / "configs" / "ingest"
DB_BASE = RUN / "databases"


def post(endpoint, query: str) -> str:
    data = urllib.parse.urlencode({"query": query}).encode()
    req = urllib.request.Request(endpoint, data=data, method="POST")
    with urllib.request.urlopen(req, timeout=180) as r:
        return r.read().decode("utf-8", "replace")


def wait_pid(tag, pidfile, tries=20):
    pidfile = Path(pidfile)
    for _ in range(tries):
        if pidfile.exists():
            return
        time.sleep(2)
    raise RuntimeError(f"pid file not created: {pidfile} ({tag})")


def graphdb_plan(db_dir, repo, query_file, out_file):
    mgmt = str(GRAPHDB["mgmt"])
    parts = repo.split("_")
    policy = "_".join(parts[:3])
    dataset = "_".join(parts[3:])
    subprocess.run([mgmt, "startup", str(db_dir), policy, dataset, str(CONFIG_DIR)], check=True)
    wait_pid("graphdb", f"/tmp/graphdb_{policy}_{dataset}.pid")
    endpoint = GRAPHDB["get"].format(repo=repo)
    q = query_file.read_text()
    # GraphDB explain: add PREFIX onto + FROM <http://www.ontotext.com/explain>
    # before the WHERE/pattern block (original query omits the bare WHERE).
    import re
    m = re.search(r"\{", q)
    if m:
        explain_q = q[:m.start()] + " FROM <http://www.ontotext.com/explain> WHERE " + q[m.start():]
        if "PREFIX onto:" not in explain_q:
            explain_q = "PREFIX onto: <http://www.ontotext.com/>\n" + explain_q
    else:
        explain_q = q
    plan = post(endpoint, explain_q)
    header = f"# GraphDB 10.5 explain-plan proof\n# repository: {repo}\n# query file: {query_file.name}\n# query:\n"
    body = "\n".join("  " + l for l in q.splitlines())
    out_file.write_text(header + body + "\n\n" + plan)
    try:
        subprocess.run([mgmt, "shutdown"], check=True)
    except subprocess.CalledProcessError:
        subprocess.run(["pkill", "-9", "-f", "GraphDBServer"], check=False)
    time.sleep(3)
    print(f"[ok] GraphDB {repo} -> {out_file}", flush=True)


def _strip_result_set(text: str) -> str:
    """Remove the query result set (tabular output) from the --explain output.

    The plan/ALGEBRA/TDB2 sections are shown first; the actual result set (the
    ASCII table of matched bindings rendered after the ``Execute ::`` line)
    carries no informational value for a plan proof, so we cut everything from
    the first separator line of dashes that follows the ``Execute`` line.
    """
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


def jena_plan(repo_dir, repo, query_file, out_file):
    qpath = f"/tmp/{repo}.rq"
    Path(qpath).write_text(query_file.read_text())
    env = dict(os.environ)
    env["JAVA_HOME"] = str(JENA["java"])
    env["PATH"] = f"{JENA['java']}/bin:" + env.get("PATH", "")
    cmd = [f"{JENA['java']}/bin/java", "-cp", JENA["jar"],
           "tdb2.tdbquery", "--loc", str(repo_dir), "--query", qpath, "--explain"]
    res = subprocess.run(cmd, capture_output=True, text=True)
    header = f"# Jena TDB2 5.1 (ARQ) --explain proof\n# repository: {repo}\n# query file: {query_file.name}\n# query:\n"
    body = "\n".join("  " + l for l in query_file.read_text().splitlines())
    out_file.write_text(header + body + "\n\n" + _strip_result_set(res.stdout or "") + (res.stderr or ""))
    print(f"[ok] Jena {repo} -> {out_file}", flush=True)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", required=True)
    ap.add_argument("--store", required=True, choices=["graphdb", "jena"])
    ap.add_argument("--model", required=True, choices=["tb_sr_rs", "tb_sr_re"])
    ap.add_argument("--query", required=True)
    ap.add_argument("--prefix", default="exp")
    ap.add_argument("--dataset", default="bearc")
    args = ap.parse_args()

    dataset = args.dataset
    repo = f"{args.model}_{dataset}"
    query_file = Path(args.query)
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"{args.prefix}-{args.store}-{args.model}-{dataset}.plan.txt"

    if args.store == "graphdb":
        db_dir = DB_BASE / "graphdb" / repo
        if not (db_dir / "repositories").exists():
            print(f"[skip] GraphDB not ingested: {db_dir}", flush=True)
            return
        graphdb_plan(db_dir, repo, query_file, out_file)
    else:
        repo_dir = DB_BASE / "jenatdb2" / repo
        if not (repo_dir / "Data-0001").exists():
            print(f"[skip] Jena not ingested: {repo_dir}", flush=True)
            return
        jena_plan(repo_dir, repo, query_file, out_file)


if __name__ == "__main__":
    main()
