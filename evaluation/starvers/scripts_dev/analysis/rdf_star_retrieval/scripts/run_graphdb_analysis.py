"""
run_graphdb_analysis.py – RDF-star retrieval / indexing proof for GraphDB.

Executed INSIDE the starvers_eval container (no `docker` calls). It mirrors the
lifecycle handling used by evaluate_queries.py: it starts / stops GraphDB with
the existing `graphdb_mgmt.sh` and then asks GraphDB for the query plan via the
`onto:explain` pseudo-graph.

Plan files (GraphDB's explain-plan output) are written to
    <out>/<prefix>-graphdb-<model>-<scenario>.plan.txt

Usage (inside the container):
    python /starvers_eval/scripts/analysis/rdf_star_retrieval/scripts/run_graphdb_analysis.py \
        --run-dir /starvers_eval/data/<TIMESTAMP> \
        --out /starvers_eval/paper/RDF-star-retrieval/GraphDB \
        [--dataset orkg] [--model tb_sr_rs|tb_sr_re] [--prefix exp]
"""

import argparse
import codecs
import json
import logging
import os
import re
import subprocess
import time
import urllib.parse
import urllib.request
from pathlib import Path

import tomli

from common import build_scenarios

CONFIG_PATH = Path("/starvers_eval/configs/eval_setup.toml")

PREDICATE = "http://www.w3.org/2000/01/rdf-schema#label"


def _load_config():
    with open(CONFIG_PATH, "rb") as f:
        return tomli.load(f)


def config_engine(store_params, repo):
    """Return (get_endpoint, post_endpoint) for a repository."""
    return (
        store_params["get"].format(repo=repo),
        store_params["post"].format(repo=repo),
    )


def post_query(endpoint, query: str) -> str:
    data = urllib.parse.urlencode({"query": query}).encode()
    req = urllib.request.Request(endpoint, data=data, method="POST")
    with urllib.request.urlopen(req, timeout=120) as r:
        return r.read().decode("utf-8", "replace")


def extract_plan(raw: str) -> str:
    raw = (raw or "").strip()
    if not raw:
        return "(no plan returned)"
    m = re.search(r'"plan"\s*:\s*"((?:[^"\\]|\\.)*)"', raw, re.S)
    if m:
        try:
            return json.loads('"' + m.group(1) + '"')
        except Exception:
            return m.group(1)
    lines = raw.splitlines()
    if lines and lines[0].strip() in ("plan", '"plan"'):
        rest = "\n".join(lines[1:]).strip()
        if rest.startswith('"') and rest.endswith('"'):
            rest = rest[1:-1]
        rest = rest.replace(r"\"", '"').replace(r"\\", "\\")
        try:
            rest = codecs.decode(rest, "unicode_escape")
        except Exception:
            pass
        return rest
    return raw


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-dir", required=True,
                    help="in-container run dir, e.g. /starvers_eval/data/<TIMESTAMP>")
    ap.add_argument("--out", required=True,
                    help="output dir for the plan files (mounted into the container)")
    ap.add_argument("--dataset", default="orkg")
    ap.add_argument("--model", default=None,
                    help="tb_sr_rs or tb_sr_re (default: run both present)")
    ap.add_argument("--prefix", default="exp")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    static = _load_config()
    triple_store = "graphdb"
    store_params = static["rdf_stores"][triple_store]
    mgmt_script = store_params["mgmt_script"]
    databases_dir = f"{args.run_dir}/databases"
    config_dir = f"{args.run_dir}/configs/ingest"
    out_dir = Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    models = [args.model] if args.model else ["tb_sr_rs", "tb_sr_re"]
    scenarios = build_scenarios(PREDICATE)

    for model in models:
        policy = model
        repo = f"{model}_{args.dataset}"
        db_dir = f"{databases_dir}/{triple_store}/{repo}"
        if not (Path(db_dir) / "repositories").exists():
            print(f"[skip] GraphDB repository not ingested: {db_dir}", flush=True)
            continue

        # Startup database
        logging.info("Startup graphdb for %s, %s", policy, args.dataset)
        subprocess.run([mgmt_script, "startup", db_dir, policy, args.dataset,
                        config_dir], check=True)

        # Wait for PID file
        pid_file = f"/tmp/{triple_store}_{policy}_{args.dataset}.pid"
        for _ in range(3):
            time.sleep(3)
            if os.path.exists(pid_file):
                break
        if not os.path.exists(pid_file):
            raise RuntimeError("PID not found")

        get_endpoint, _ = config_engine(store_params, repo)

        for scen in scenarios:
            if scen["model"] != model:
                continue
            out_file = out_dir / f"{args.prefix}-graphdb-{model}-{scen['id']}.plan.txt"
            try:
                plan_raw = post_query(get_endpoint, scen["graphdb_query"])
            except Exception as e:
                print(f"[warn] explain failed for {scen['id']}: {e}", flush=True)
                continue
            header = (
                f"# GraphDB 10.5 explain-plan proof\n"
                f"# repository : {repo}\n"
                f"# scenario   : {scen['id']} (model {scen['model']})\n"
                f"# predicate  : {PREDICATE}\n"
                f"# query      :\n"
            )
            body = "\n".join("  " + l for l in scen["graphdb_query"].splitlines())
            out_file.write_text(header + body + "\n\n" + extract_plan(plan_raw))
            print(f"[ok] {out_file}", flush=True)

        # Shutdown database
        subprocess.run([mgmt_script, "shutdown"], check=True)
        print(f"[stop] GraphDB {repo} stopped", flush=True)
        time.sleep(3)


if __name__ == "__main__":
    main()
