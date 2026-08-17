"""
run_jena_analysis.py – RDF-star retrieval / indexing proof for Jena TDB2.

Executed INSIDE the starvers_eval container (no `docker` calls).

Jena's query plan is produced by ARQ's command-line analyzer `tdb2.tdbquery
--explain`. Unlike GraphDB's server-side `onto:explain`, this analyzer opens the
TDB2 dataset directly from disk and acquires an *exclusive* lock (tdb.lock) on
the location -- so a running Fuseki server (jenatdb2_mgmt.sh startup) would hold
that lock and block the analysis. For that reason no Fuseki lifecycle is used
here; the analyzer reads the on-disk repository exactly as the evaluation's Jena
query parser does.

Plan files (ARQ query / algebra / TDB2 plan) are written to
    <out>/<prefix>-jena-<model>-<scenario>.plan.txt

Usage (inside the container):
    python /starvers_eval/scripts/analysis/rdf_star_retrieval/scripts/run_jena_analysis.py \
        --run-dir /starvers_eval/data/<TIMESTAMP> \
        --out /starvers_eval/paper/RDF-star-retrieval/Jena \
        [--dataset orkg] [--model tb_sr_rs|tb_sr_re] [--prefix exp]
"""

import argparse
import logging
import os
import subprocess
from pathlib import Path

from common import build_scenarios

JENA_JAR = "/jena-fuseki/fuseki-server.jar"
JAVA_HOME = "/opt/java/java17/openjdk"          # Jena needs Java 17
PREDICATE = "http://www.w3.org/2000/01/rdf-schema#label"


def run(cmd):
    return subprocess.run(cmd, capture_output=True, text=True, check=False)


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

    out_dir = Path(args.out)
    run_dir = Path(args.run_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    if not Path(JENA_JAR).exists():
        print("[error] Jena distribution not found; aborting")
        return

    models = [args.model] if args.model else ["tb_sr_rs", "tb_sr_re"]
    scenarios = build_scenarios(PREDICATE)

    for model in models:
        repo = f"{model}_{args.dataset}"
        repo_dir = run_dir / "databases" / "jenatdb2" / repo
        if not (repo_dir / "Data-0001").exists():
            print(f"[skip] Jena repository not ingested: {repo_dir}", flush=True)
            continue

        for scen in scenarios:
            if scen["model"] != model:
                continue
            out_file = out_dir / f"{args.prefix}-jena-{model}-{scen['id']}.plan.txt"

            qpath = f"/tmp/{scen['id']}_{model}_{args.dataset}.rq"
            Path(qpath).write_text(scen["jena_query"])

            env = dict(os.environ)
            env["JAVA_HOME"] = JAVA_HOME
            env["PATH"] = "/opt/java/java17/openjdk/bin:" + env.get("PATH", "")
            cmd = [
                f"{JAVA_HOME}/bin/java", "-cp", JENA_JAR,
                "tdb2.tdbquery",
                "--loc", str(repo_dir),
                "--query", qpath,
                "--explain",
            ]
            res = run(cmd)

            header = (
                f"# Jena TDB2 5.1 (ARQ) query-plan proof\n"
                f"# repository : {repo}\n"
                f"# scenario   : {scen['id']} (model {scen['model']})\n"
                f"# predicate  : {PREDICATE}\n"
                f"# command    : tdb2.tdbquery --explain\n"
                f"# query      :\n"
            )
            body = "\n".join("  " + l for l in scen["jena_query"].splitlines())
            text = header + body + "\n\n" + (res.stdout or "") + (res.stderr or "")
            out_file.write_text(text)
            print(f"[ok] {out_file}", flush=True)


if __name__ == "__main__":
    main()
