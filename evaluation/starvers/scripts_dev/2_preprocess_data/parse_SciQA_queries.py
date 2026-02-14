import json
import re
from pathlib import Path
import logging
import shutil
import sys
from starvers.starvers import TripleStoreEngine, split_prefixes_query
sys.path.append(str(Path("/starvers_eval/scripts/construct_queries").resolve()))
from construct_queries import split_solution_modifiers_query
import subprocess
import shlex
import time
import unicodedata
from SPARQLWrapper import SPARQLWrapper, Wrapper, GET, POST, POSTDIRECTLY, JSON



############################################# Logging #############################################
with open('/starvers_eval/output/logs/preprocess_data/parse_sciqa_queries.txt', "w") as log_file:
    log_file.write("")
with open('/starvers_eval/output/logs/preprocess_data/excluded_queries.csv', "w") as query_log_file:
    query_log_file.write("query,yn_excluded,reason\n")
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
CONFIG_TMPL_DIR="/starvers_eval/scripts/2_preprocess_data/configs"
CONFIG_DIR="/starvers_eval/configs/preprocess_data"
GRAPHDB_DATABASE_DIR="/starvers_eval/databases/preprocess_data/graphdb/dummy_orkg"
OSTRICH_DATABASE_DIR="/starvers_eval/databases/preprocess_data/ostrich/dummy_orkg"
GRAPHDB_MGMT_SCRIPT="/starvers_eval/scripts/triple_store_mgmt/graphdb_mgmt.sh"
OSTRICH_MGMT_SCRIPT="/starvers_eval/scripts/triple_store_mgmt/ostrich_mgmt.sh"

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


# Helper
def rewrite_select_aliases(sparql: str) -> str:
    # keep the first capture group (SELECT + vars) and wrap only the alias
    return SELECT_ALIAS_RE.sub(r"\1 (\2)", sparql)


# Helper
def wrap_aggregations(sparql: str) -> str:
    def repl(m):
        func = m.group(1).upper()
        var = m.group(2).strip()
        # create a default alias
        alias = f"?{func.lower()}_{var.strip('?')}"
        return f"({func}({var}) AS {alias})"
    return AGG_FUNC_RE.sub(repl, sparql)


# Start GraphDB
def startup():
    # Startup GraphDB repository 
    logging.info("Create database environment for GraphDB")
    subprocess.run([f"{GRAPHDB_MGMT_SCRIPT}", "create_env", "dummy", "orkg", f"{GRAPHDB_DATABASE_DIR}", f"{CONFIG_TMPL_DIR}", f"{CONFIG_DIR}"], check=True)
    
    logging.info("Ingest empty dataset for testing.")
    subprocess.run([f"{GRAPHDB_MGMT_SCRIPT}", "ingest_empty", f"{GRAPHDB_DATABASE_DIR}", f"dummy", f"orkg", f"{CONFIG_DIR}"], check=True)
    logging.info("Ingested empty dataset.")

    logging.info("Start GraphDB engine.")
    subprocess.run([f"{GRAPHDB_MGMT_SCRIPT}", "startup", f"{GRAPHDB_DATABASE_DIR}", f"dummy", f"orkg"], check=True)
    logging.info("GraphDB is up")

    # Startup Ostrich
    logging.info("Create database environment for Ostrich")
    subprocess.run([f"{OSTRICH_MGMT_SCRIPT}", "create_env", "dummy", "orkg", f"{OSTRICH_DATABASE_DIR}", "", ""], check=True)
    
    logging.info("Ingest the first ORKG snapshot.")
    subprocess.run([f"{OSTRICH_MGMT_SCRIPT}", "ingest", f"{OSTRICH_DATABASE_DIR}", f"/starvers_eval/rawdata/orkg/alldata_vdir", f"ostrich", "orkg", "", "1"], check=True)
    logging.info("Ingested empty dataset.")

    logging.info("Start Ostrich engine.")
    subprocess.run([f"{OSTRICH_MGMT_SCRIPT}", "startup", f"{OSTRICH_DATABASE_DIR}", f"dummy", f"orkg"], check=True)
    logging.info("Ostrich is up")




# Extraction from JSON files
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


                # Normalize Unicode (important!)
                sparql = unicodedata.normalize("NFC", sparql)

                # Remove problematic control characters (keep newline/tab)
                sparql = re.sub(r"[^\x09\x0A\x0D\x20-\uFFFF]", "", sparql)

                out_file = BASE_DIR / f"{qid}.txt"
                out_file.write_text(
                    PREFIXES + "\n" + sparql + "\n",
                    encoding="utf-8",
                )
                logging.info(f"Wrote {out_file}")


# Queries that starvers cannot process are excluded
def exclude_queries():
    # For querying Ostrich
    engine = SPARQLWrapper(endpoint="http://Starvers:42564/sparql")
    engine.timeout = 30
    engine.setReturnFormat(JSON)
    engine.setOnlyConneg(True)
    engine.setMethod(POST)
    engine.addCustomHttpHeader("Accept", "application/sparql-results+json")

    # For querying GraphDB
    rdf_star_engine = TripleStoreEngine("http://Starvers:7200/repositories/dummy_orkg",
                                            "http://Starvers:7200/repositories/dummy_orkg/statements", 
                                            skip_connection_test=True)

    queries = []

    for query_file in BASE_DIR.iterdir():
        if query_file.suffix != ".txt":
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
            queries.append([query_file.name, 1, "ASK"])
            query_file.unlink()
            continue

        
        # Execute original query against GraphDB
        try:
            result_set = rdf_star_engine.query(sparql, yn_timestamp_query=False)
        except Exception as e:
            logging.info(f"Original query {query_file.name} is invalid in GraphDB and will be excluded: {e}")
            queries.append([query_file.name, 1, "Invalid Original in GraphDB"])
            query_file.unlink()
            continue
        
        # Execute original query against Ostrich
        try:
            versioned_query = sparql
            prefixes, versioned_query = split_prefixes_query(versioned_query)
            modifiers, versioned_query = split_solution_modifiers_query(versioned_query)
            with open("/starvers_eval/scripts/construct_queries/templates/ostrich/sparql_full.txt", 'r') as templateFile:
                template = templateFile.read()
                versioned_query = template.format(prefixes, 0, versioned_query, modifiers)
                logging.info(f"Versioned query for Ostrich execution: {versioned_query}")

            engine.setQuery(versioned_query) 
            result_set = engine.query()
        except Exception as e:
            logging.info(f"Original query {query_file.name} is invalid in Ostrich and will be excluded: {e}")
            queries.append([query_file.name, 1, "Invalid Original in Ostrich"])
            query_file.unlink()
            continue

        # Execute transformed query
        try:
            result_set = rdf_star_engine.query(sparql, yn_timestamp_query=True)
        except Exception as e:
            logging.info(f"Query {query_file.name} could not get transformed successfully and will be excluded: {e}")
            queries.append([query_file.name, 1, "Malformed Starvers transformation"])
            query_file.unlink()
            continue
        queries.append([query_file.name, 0, ""])


    queries_to_exlcude = [row[0] for row in queries if row[1] == 1]
    logging.info(f"Exluded the following {len(queries_to_exlcude)} queries: {queries_to_exlcude}")

    with open('/starvers_eval/output/logs/preprocess_data/excluded_queries.csv', "a") as query_log_file:
        for row in queries:
            query_log_file.write(",".join(map(str, row)) + "\n")

    # Shutting down GraphDB
    subprocess.call(shlex.split(f"{GRAPHDB_MGMT_SCRIPT} shutdown"))
            

# Remove database and raw dataset files
def cleanup():
    for item in BASE_DIR.iterdir():
        if item.is_dir() and item.name in SUBDIRS:
            for subitem in item.iterdir():
                if subitem.is_file() and subitem.suffix != "txt":
                    subitem.unlink()
            item.rmdir()
        elif item.is_file() and item.suffix != ".txt":
            item.unlink()

    shutil.rmtree(f"{BASE_DIR}/SciQA-dataset")
    logging.info(f"Removed directory {BASE_DIR}")

    shutil.rmtree(f"{DATABASE_DIR}")
    logging.info(f"Removed directory {DATABASE_DIR}")


# Main 
if __name__ == "__main__":
    startup()
    extract_queries()
    exclude_queries()
    cleanup()
