#!/usr/bin/env python3

import json
import re
from pathlib import Path
import logging
import shutil
from starvers.starvers import TripleStoreEngine

############################################# Logging #############################################
with open('/starvers_eval/output/logs/preprocess_data/parse_sciqa_queries.txt', "w") as log_file:
    log_file.write("")
logging.basicConfig(
    handlers=[logging.FileHandler(
        filename="/starvers_eval/output/logs/preprocess_data/parse_sciqa_queries.txt",
        encoding='utf-8',
        mode='a+'
    )],
    format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
    datefmt="%F %A %T",
    level=logging.INFO
)

###################################### Parameters ######################################

PREFIXES = \
"""PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX orkgr: <http://orkg.org/orkg/resource/>
PREFIX orkgc: <http://orkg.org/orkg/class/>
PREFIX orkgp: <http://orkg.org/orkg/predicate/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
"""

BASE_DIR = Path("/starvers_eval/queries/raw_queries/orkg/complex")
SCRIPTS_DIR = Path("/starvers_eval/scripts")
SUBDIRS = ["train", "test", "valid"]

###################################### Regex logic ######################################


SELECT_ALIAS_RE = re.compile(
    r"""
    (SELECT(?:\s+\?[a-zA-Z0-9_-]*)*)         # capture 'SELECT' + optional variables before alias
    \s*                                       # optional whitespace
    (?<!\()                                   # negative lookbehind: not already wrapped
    (                                         # start group 2: the thing we want to wrap
        (?:                                   # non-capturing group for aggregation OR simple var
            (?:COUNT|SUM|AVG|MIN|MAX)\([^\)]+\)  # aggregation function with parentheses
            |                                 # OR
            \?[a-zA-Z0-9_-]+                  # simple variable
        )
        \s+AS\s+\?[a-zA-Z0-9_-]+             # the alias part
    )
    (?!\))                                    # negative lookahead: not already wrapped
    """,
    re.VERBOSE | re.IGNORECASE
)


AGG_FUNC_RE = re.compile(
    r"""
    (?<!\()                     # not already wrapped
    \b
    (COUNT|SUM|AVG|MIN|MAX)     # aggregation functions
    \s*\(                        # opening parenthesis
        ([^\)]+)                 # everything inside the parentheses
    \)
    (?!\s+AS\s+\?[a-zA-Z0-9_-]+) # not already aliased
    """,
    re.IGNORECASE | re.VERBOSE
)



def rewrite_select_aliases(sparql: str) -> str:
    # keep the first capture group (SELECT + vars) and wrap only the alias
    return SELECT_ALIAS_RE.sub(r"\1 (\2)", sparql)

def wrap_aggregations(sparql: str) -> str:
    def repl(m):
        func = m.group(1).upper()
        var = m.group(2).strip()
        # create a default alias
        alias = f"?{func.lower()}_{var.strip('?')}"
        return f"({func}({var}) AS {alias})"
    return AGG_FUNC_RE.sub(repl, sparql)




###################################### Extraction ######################################

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

                sparql = rewrite_select_aliases(sparql.strip())
                sparql = wrap_aggregations(sparql.strip())
                
                # Fix query 56
                sparql = sparql.replace("(AVG(?installed_cap_value AS ?avg_installed_cap_value))",
                                             "(AVG(?installed_cap_value) AS ?avg_installed_cap_value)")

                out_file = BASE_DIR / f"{qid}.sparql"
                out_file.write_text(
                    PREFIXES + "\n" + sparql + "\n",
                    encoding="utf-8",
                )
                logging.info(f"Wrote {out_file}")


###################################### Exclusion ######################################
# Queries that starvers cannot process are excluded

def exclude_queries():
    queries_to_exlcude = [""]

    for query_file in BASE_DIR.iterdir():
        if query_file.suffix != ".sparql":
            continue  # skip non-SPARQL files

        with query_file.open("r", encoding="utf-8") as f:
            sparql = f.read()

        # Exclude if this is an ASK query 
        ASK_REGEX = re.compile(
            r"\bASK\s+(WHERE\s+)?\{",
            re.IGNORECASE | re.MULTILINE
        )

        if ASK_REGEX.search(sparql):
            logging.info(f"Query {query_file.name} is an ASK query and will be excluded")
            queries_to_exlcude.append(query_file.name)
            query_file.unlink()
            continue

        # Exlude if the RDF-star engine returns an exception (because the query was not rewritten properly).
        # Startup GraphDB repository 
        # Call SCRIPTS_DIR / script 3_construct_datasets start_graphdb.sh "dummy" "orkg" "false" "true" "false"
        #
        #rdf_star_engine = TripleStoreEngine("http://Starvers:7200/repositories/dummy_orkg", "http://Starvers:7200/repositories/dummy_orkg/statements")
        #try:
        #    result_set = rdf_star_engine.query(sparql)
        #except Exception as e:
        #    logging.info(f"Query {query_file.name} could not get transformed successfully and will be excluded: {e}")
        #    queries_to_exlcude.append(query_file.name)
        #    query_file.unlink()

    logging.info(f"Exluded the following queries: {queries_to_exlcude}")


###################################### Cleanup ######################################

def cleanup():
    for item in BASE_DIR.iterdir():
        if item.is_dir() and item.name in SUBDIRS:
            for subitem in item.iterdir():
                if subitem.is_file() and subitem.suffix != ".sparql":
                    subitem.unlink()
            item.rmdir()
        elif item.is_file() and item.suffix != ".sparql":
            item.unlink()

    shutil.rmtree(f"{BASE_DIR}/SciQA-dataset")
    logging.info(f"Cleanup completed in {BASE_DIR}")

###################################### Main ######################################

if __name__ == "__main__":
    extract_queries()
    exclude_queries()
    #cleanup()
