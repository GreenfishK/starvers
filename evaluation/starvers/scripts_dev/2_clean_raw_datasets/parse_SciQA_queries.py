#!/usr/bin/env python3

import json
import re
from pathlib import Path
import logging
from venv import logging
import shutil

############################################# Logging #############################################
with open('/starvers_eval/output/logs/clean_raw_datasets/parse_sciqa_queries.txt', "w") as log_file:
    log_file.write("")
logging.basicConfig(handlers=[logging.FileHandler(filename="/starvers_eval/output/logs/clean_raw_datasets/parse_sciqa_queries.txt", 
                                                  encoding='utf-8', mode='a+')],
                    format="%(asctime)s %(name)s:%(levelname)s:%(message)s", 
                    datefmt="%F %A %T", 
                    level=logging.INFO)

###################################### Parameters ######################################
# Bash arguments and directory paths

PREFIXES = \
"""PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX orkgr: <http://orkg.org/orkg/resource/>
PREFIX orkgc: <http://orkg.org/orkg/class/>
PREFIX orkgp: <http://orkg.org/orkg/predicate/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
"""


BASE_DIR = Path("/starvers_eval/queries/raw_queries/orkg/complex")
SUBDIRS = ["train", "test", "valid"]


ALIAS_EXPR_RE = re.compile(
    r"""(?<!\()                 # not already wrapped
        (
            [^()\s]+             # function or expression start
            \s*\(.*?\)           # (...) arguments
            \s+AS\s+\?\w+        # AS ?alias
        )
    """,
    re.IGNORECASE | re.VERBOSE | re.DOTALL,
)


def wrap_select_aliases(sparql: str) -> str:
    """
    Wrap SELECT alias expressions like:
      COUNT(?x) AS ?y
    into:
      (COUNT(?x) AS ?y)
    """

    def replacer(match):
        expr = match.group(1)
        return f"({expr})"

    return ALIAS_EXPR_RE.sub(replacer, sparql)


def extract_queries():
    for subdir in SUBDIRS:
        json_path = BASE_DIR / "SciQA-dataset" / subdir / "questions.json"
        if not json_path.exists():
            logging.info(f"Skipping missing file: {json_path}")
            continue

        with json_path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        for entry in data.get("questions", []):
            if entry.get("auto_generated") is False:
                qid = entry.get("id")
                sparql = entry.get("query", {}).get("sparql")

                if not qid or not sparql:
                    continue

                sparql = wrap_select_aliases(sparql.strip())

                out_file = BASE_DIR / f"{qid}.sparql"
                out_file.write_text(
                    PREFIXES + "\n" + sparql + "\n",
                    encoding="utf-8",
                )
                logging.info(f"Wrote {out_file}")

def cleanup():
    # Remove all directories and files that are not *.sparql files in BASE_DIR
    for item in BASE_DIR.iterdir():
        if item.is_dir() and item.name in SUBDIRS:
            for subitem in item.iterdir():
                if subitem.is_file() and subitem.suffix != ".sparql":
                    subitem.unlink()
            item.rmdir()
        elif item.is_file() and item.suffix != ".sparql":
            item.unlink()

    # Remove SciQA-dataset directory
    shutil.rmtree(f"{BASE_DIR}/SciQA-dataset")
    logging.info(f"Cleanup completed in {BASE_DIR}")


if __name__ == "__main__":
    extract_queries()
    cleanup()

