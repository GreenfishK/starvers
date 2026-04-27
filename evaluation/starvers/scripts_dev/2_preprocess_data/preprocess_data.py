"""
preprocess_data.py

Rewrite of clean_datasets.sh + parse_SciQA_queries.py executed consecutively.

Phase 1 — clean_datasets:
  For each dataset and each variant (ic, BEAR_ng):
    1. Skolemize blank nodes in subject position
    2. Skolemize blank nodes in object position
    3. Comment out invalid triples via the Java RDF validator
  Each step is idempotent: a header comment is written on the first run
  and checked on subsequent runs to skip re-processing.
  Files are processed in parallel across a thread pool capped at
  min(PREPROCESS_WORKERS env var, cpu_count) workers.

Phase 2 — parse_SciQA_queries:
  1. startup()      — spin up GraphDB and Ostrich with a dummy ORKG snapshot
  2. extract_queries() — pull hand-crafted queries from SciQA JSON files,
                         rewrite aliases/aggregations, write one .txt per query
  3. exclude_queries() — test each query against GraphDB and Ostrich,
                         remove those that fail
  4. cleanup()      — remove raw files and temp databases

Phase 3 — count all query sets (including orkg which was just extracted)

"""

import csv as _csv
import json
import logging
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

import pandas as pd
import tomli
from SPARQLWrapper import GET, JSON, POST, POSTDIRECTLY, SPARQLWrapper

from starvers.starvers import TripleStoreEngine, split_prefixes_query

sys.path.append(str(Path("/starvers_eval/scripts/5_construct_queries").resolve()))
from construct_queries import split_solution_modifiers_query


# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

LOG_DIR = Path(os.environ["RUN_DIR"]) / "output" / "logs" / "preprocess_data"
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE         = LOG_DIR / "preprocess_data.txt"
EXCLUDE_CSV      = LOG_DIR / "excluded_queries.csv"
PREPROCESS_CSV   = LOG_DIR / "preprocess_summary.csv"
QUERIES_META_CSV = Path(os.environ["RUN_DIR"]) / "output" / "logs" / "download" / "queries_meta.csv"
QUERY_COUNTS_CSV = LOG_DIR / "query_counts.csv"

LOG_FILE.write_text("")
EXCLUDE_CSV.write_text("query,yn_excluded,reason\n")

logging.basicConfig(
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8", mode="a+"),
        logging.StreamHandler(sys.stdout),
    ],
    format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
    datefmt="%F %A %T",
    level=logging.INFO,
)

clean_handler = logging.FileHandler(LOG_FILE, encoding="utf-8", mode="a+")
clean_handler.setFormatter(logging.Formatter(
    "%(asctime)s root:%(levelname)s:%(message)s",
    datefmt="%Y-%m-%d %A %H:%M:%S",
))
LOG = logging.getLogger("clean_datasets")
LOG.addHandler(clean_handler)

# ---------------------------------------------------------------------------
# Environment / path constants
# ---------------------------------------------------------------------------

RUN_DIR         = Path(os.environ["RUN_DIR"])
SCRIPT_DIR      = Path("/starvers_eval/scripts")
CONFIG_PATH     = Path("/starvers_eval/configs/eval_setup.toml")
CONFIG_TMPL_DIR = "/starvers_eval/scripts/2_preprocess_data/configs"
CONFIG_DIR      = "/starvers_eval/configs/preprocess_data"

GRAPHDB_MGMT_SCRIPT = "/starvers_eval/scripts/triple_store_mgmt/graphdb_mgmt.sh"
OSTRICH_MGMT_SCRIPT = "/starvers_eval/scripts/triple_store_mgmt/ostrich_mgmt.sh"
JENA_MGMT_SCRIPT    = "/starvers_eval/scripts/triple_store_mgmt/jenatdb2_mgmt.sh"

RDF_VALIDATOR_JAR   = str(SCRIPT_DIR / "2_preprocess_data/RDFValidator/target/rdfvalidator-1.0-jar-with-dependencies.jar")

DATASET_VARIANTS = ["ic", "BEAR_ng"]
DATASETS         = os.environ.get("datasets", "").split()

JAVA_ENV = {**os.environ, "JAVA_HOME": "/opt/java/java11/openjdk",
            "PATH": "/opt/java/java11/openjdk/bin:" + os.environ.get("PATH", "")}

# Worker count for Phase 1: capped at cpu_count so over-provisioning is harmless.
# Override with the PREPROCESS_WORKERS env var; default is 10.
MAX_WORKERS = min(
    int(os.environ.get("PREPROCESS_WORKERS", 10)),
    os.cpu_count() or 1,
)

# ---------------------------------------------------------------------------
# TOML config helpers
# ---------------------------------------------------------------------------

def _load_config() -> dict:
    with open(CONFIG_PATH, "rb") as f:
        return tomli.load(f)


def get_snapshot_version(config: dict, dataset: str) -> int | None:
    entry = config.get("datasets", {}).get(dataset)
    if not entry or "snapshot_versions" not in entry:
        LOG.error(
            f"The dataset {dataset} is not within the registered datasets in eval_setup.toml. "
            "This dataset will be skipped."
        )
        return None
    return entry["snapshot_versions"]


def get_snapshot_filename_format(config: dict, dataset: str) -> str | None:
    entry = config.get("datasets", {}).get(dataset)
    if not entry or "ic_basename_length" not in entry:
        LOG.error(
            f"The dataset {dataset} is not within the registered datasets in eval_setup.toml. "
            "This dataset will be skipped."
        )
        return None
    return f"%0{entry['ic_basename_length']}g"


# ---------------------------------------------------------------------------
# Phase 1 helpers: file I/O
# ---------------------------------------------------------------------------

def _read_lines(path: Path) -> list[str]:
    with open(path, "r", encoding="utf-8") as f:
        return f.readlines()


def _write_lines(path: Path, lines: list[str]):
    with open(path, "w", encoding="utf-8") as f:
        f.writelines(lines)


def _prepend_comment(path: Path, comment: str):
    lines = _read_lines(path)
    _write_lines(path, [comment + "\n"] + lines)


def _first_n_lines(path: Path, n: int = 3) -> list[str]:
    lines = []
    with open(path, "r", encoding="utf-8") as f:
        for _ in range(n):
            line = f.readline()
            if not line:
                break
            lines.append(line)
    return lines


# ---------------------------------------------------------------------------
# Phase 1 cleaning steps — each operates on a single file and returns a count.
# Signatures no longer accept dataset/variant/file_number or preprocess_df;
# those are handled by the caller so df writes happen on the main thread only.
# ---------------------------------------------------------------------------

def skolemize_subject_blanks(raw_ds: Path) -> int:
    """
    Replace blank nodes in subject position (_:xxx) with URIs (<_:xxx>).
    Idempotent. Returns the count of nodes skolemized (from header if already done).
    """
    header_marker = "# skolemized_blank_nodes_in_subject_position"
    first = _first_n_lines(raw_ds)

    if any(header_marker in l for l in first):
        match = re.search(
            r"# skolemized_blank_nodes_in_subject_position: (\d+)",
            "".join(first),
        )
        cnt = int(match.group(1)) if match else 0
        LOG.info(f"{raw_ds}: subject skolemization already done ({cnt} previously)")
        return cnt

    pattern = re.compile(r"^(_:[a-zA-Z0-9]+)", re.MULTILINE)
    content  = raw_ds.read_text(encoding="utf-8")
    cnt      = len(pattern.findall(content))
    content  = pattern.sub(r"<\1>", content)
    raw_ds.write_text(content, encoding="utf-8")
    _prepend_comment(raw_ds, f"# skolemized_blank_nodes_in_subject_position: {cnt}")
    LOG.info(f"{raw_ds}: skolemized blank nodes in subject position: {cnt}")
    return cnt


def skolemize_object_blanks(raw_ds: Path) -> int:
    """
    Replace blank nodes in object position with URIs.
    Idempotent. Returns the count of nodes skolemized (from header if already done).
    """
    header_marker = "# skolemized_blank_nodes_in_object_position"
    first = _first_n_lines(raw_ds)

    if any(header_marker in l for l in first):
        match = re.search(
            r"# skolemized_blank_nodes_in_object_position: (\d+)",
            "".join(first),
        )
        cnt = int(match.group(1)) if match else 0
        LOG.info(f"{raw_ds}: object skolemization already done ({cnt} previously)")
        return cnt

    pattern = re.compile(
        r"(^[^#].*)(_:[a-zA-Z0-9]+)(\s*(<[a-zA-Z0-9_/:.]+>){0,1}\s*\.$)",
        re.MULTILINE,
    )
    content = raw_ds.read_text(encoding="utf-8")
    cnt     = len(pattern.findall(content))
    content = pattern.sub(r"\1<\2>\3", content)
    raw_ds.write_text(content, encoding="utf-8")
    _prepend_comment(raw_ds, f"# skolemized_blank_nodes_in_object_position: {cnt}")
    LOG.info(f"{raw_ds}: skolemized blank nodes in object position: {cnt}")
    return cnt


def validate_and_comment_invalid_triples(raw_ds: Path) -> int:
    """
    Run the Java RDF validator to comment out invalid triples.
    Idempotent. Returns the number of lines excluded (from header if already done).
    """
    header_marker = "# invalid_lines_excluded"
    first = _first_n_lines(raw_ds)
    LOG.info(f"Validating {raw_ds}")

    if any(header_marker in l for l in first):
        match = re.search(r"# invalid_lines_excluded: (\d+)", "".join(first))
        cnt = int(match.group(1)) if match else 0
        LOG.info(f"{raw_ds}: validation already done ({cnt} previously excluded)")
        return cnt

    clean_ds = raw_ds.with_name(raw_ds.stem + "_clean" + raw_ds.suffix)
    subprocess.run(
        ["java", "-jar", RDF_VALIDATOR_JAR, str(raw_ds), str(clean_ds)],
        env=JAVA_ENV,
        check=True,
    )
    shutil.move(str(clean_ds), str(raw_ds))

    comment_lines = sum(1 for l in _read_lines(raw_ds) if l.startswith("# "))
    excluded = max(0, comment_lines - 2)
    _prepend_comment(raw_ds, f"# invalid_lines_excluded: {excluded}")
    LOG.info(f"{raw_ds}: invalid lines excluded: {excluded}")
    return excluded


# ---------------------------------------------------------------------------
# Phase 1: per-file worker — runs all three steps sequentially on one file
# ---------------------------------------------------------------------------

def _clean_one_file(
    dataset: str,
    variant: str,
    raw_ds: Path,
    file_number: int,
) -> dict:
    """
    Run all three cleaning steps on a single file in order.
    Each step must complete before the next starts because they modify the file
    in-place and each subsequent step depends on the previous one's output.
    Returns a plain dict; the main thread writes it into preprocess_df.
    """
    subj = skolemize_subject_blanks(raw_ds)
    obj  = skolemize_object_blanks(raw_ds)
    inv  = validate_and_comment_invalid_triples(raw_ds)
    return {
        "dataset":             dataset,
        "variant":             variant,
        "file_number":         file_number,
        "skolemized_subjects": subj,
        "skolemized_objects":  obj,
        "invalid_triples":     inv,
    }


# ---------------------------------------------------------------------------
# Phase 1: clean_datasets — builds work list, runs pool, merges results
# ---------------------------------------------------------------------------

def clean_datasets():
    LOG.info(f"Start corrections (parallel workers: {MAX_WORKERS})")
    config = _load_config()

    preprocess_df = pd.DataFrame(
        columns=["dataset", "variant", "file_number",
                 "skolemized_subjects", "skolemized_objects", "invalid_triples"]
    )
    preprocess_df.set_index(["dataset", "variant", "file_number"], inplace=True)

    # Collect all work items up front so the pool can schedule freely
    # across datasets and variants rather than being serialised by the outer loops.
    work_items: list[tuple[str, str, Path, int]] = []

    for dataset in DATASETS:
        versions     = get_snapshot_version(config, dataset)
        filename_fmt = get_snapshot_filename_format(config, dataset)
        if versions is None or filename_fmt is None:
            continue

        for ds_var in DATASET_VARIANTS:
            if ds_var == "BEAR_ng":
                ds_path = RUN_DIR / "rawdata" / dataset / "alldata.TB.nq"
                if not ds_path.is_file():
                    LOG.info(
                        f"The BEAR named graphs dataset does not exist at {ds_path}. "
                        "Skipping."
                    )
                    continue
                # BEAR_ng is a single file; treat as file_number=1
                work_items.append((dataset, ds_var, ds_path, 1))

            elif ds_var == "ic":
                for c_int in range(1, versions + 1):
                    c      = filename_fmt % c_int
                    raw_ds = RUN_DIR / "rawdata" / dataset / "alldata.IC.nt" / f"{c}.nt"
                    work_items.append((dataset, ds_var, raw_ds, c_int))

            else:
                LOG.error("Dataset variant must be ic or BEAR_ng.")
                sys.exit(2)

    LOG.info(f"Submitting {len(work_items)} files to the cleaning pool")

    # ThreadPoolExecutor is the right primitive here: the bottleneck is I/O
    # (large file reads/writes) and subprocess (Java JVM per file), not
    # CPU-bound Python.  MAX_WORKERS is already capped at cpu_count so
    # over-provisioning cannot cause contention or resource exhaustion.
    results: list[dict] = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {
            pool.submit(_clean_one_file, dataset, ds_var, raw_ds, c_int): (dataset, ds_var, c_int)
            for dataset, ds_var, raw_ds, c_int in work_items
        }
        for future in as_completed(futures):
            dataset, ds_var, c_int = futures[future]
            try:
                results.append(future.result())
            except Exception as exc:
                # Log with full traceback, then re-raise so the step is marked failed
                LOG.error(
                    f"Cleaning failed for {dataset}/{ds_var}/file {c_int}: {exc}",
                    exc_info=True,
                )
                raise

    # Write all results into the DataFrame on the main thread — no locking needed
    for r in results:
        preprocess_df.loc[
            (r["dataset"], r["variant"], r["file_number"])
        ] = [r["skolemized_subjects"], r["skolemized_objects"], r["invalid_triples"]]

    preprocess_df.to_csv(PREPROCESS_CSV)
    LOG.info(f"Saved preprocessing summary to {PREPROCESS_CSV}")


# ---------------------------------------------------------------------------
# Phase 2 constants and regex
# ---------------------------------------------------------------------------

PREFIXES = """\
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX orkgr: <http://orkg.org/orkg/resource/>
PREFIX orkgc: <http://orkg.org/orkg/class/>
PREFIX orkgp: <http://orkg.org/orkg/predicate/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
"""

DOWNLOADED_QUERIES_DIR = RUN_DIR / "queries" / "downloaded_queries"
RAW_QUERIES_DIR = RUN_DIR / "queries" / "raw_queries" 
BASE_DIR     = RUN_DIR / "queries" / "raw_queries" / "orkg" / "complex"
SUBDIRS      = ["train", "test", "valid"]

GRAPHDB_DATABASE_DIR = RUN_DIR / "databases" / "preprocess_data" / "graphdb" / "dummy_orkg"
OSTRICH_DATABASE_DIR = RUN_DIR / "databases" / "preprocess_data" / "ostrich" / "dummy_orkg"
JENA_DATABASE_DIR    = RUN_DIR / "databases" / "preprocess_data" / "jenatdb2" / "dummy_orkg"

SELECT_ALIAS_RE = re.compile(
    r"""
    (SELECT(?:\s+\?[a-zA-Z0-9_-]*)*)
    \s*
    (?<!\()
    (
        (?:
            (?:COUNT|SUM|AVG|MIN|MAX)\([^\)]+\)
            |
            \?[a-zA-Z0-9_-]+
        )
        \s+AS\s+\?[a-zA-Z0-9_-]+
    )
    (?!\))
    """,
    re.VERBOSE | re.IGNORECASE,
)

AGG_FUNC_RE = re.compile(
    r"""
    (?<!\()
    \b
    (COUNT|SUM|AVG|MIN|MAX)
    \s*\(
        ([^\)]+)
    \)
    (?!\s+AS\s+\?[a-zA-Z0-9_-]+)
    """,
    re.IGNORECASE | re.VERBOSE,
)

ASK_REGEX = re.compile(r"\bASK\s+(WHERE\s+)?\{", re.IGNORECASE | re.MULTILINE)


# ---------------------------------------------------------------------------
# Phase 2 helpers: SPARQL rewriting
# ---------------------------------------------------------------------------

def rewrite_select_aliases(sparql: str) -> str:
    return SELECT_ALIAS_RE.sub(r"\1 (\2)", sparql)


def wrap_aggregations(sparql: str) -> str:
    def repl(m):
        func  = m.group(1).upper()
        var   = m.group(2).strip()
        alias = f"?{func.lower()}_{var.strip('?')}"
        return f"({func}({var}) AS {alias})"
    return AGG_FUNC_RE.sub(repl, sparql)


# ---------------------------------------------------------------------------
# Phase 2: startup, extract, exclude, cleanup
# ---------------------------------------------------------------------------

def _startup_graphdb():
    LOG.info("Create database environment for GraphDB")
    subprocess.run([GRAPHDB_MGMT_SCRIPT, "create_env", "dummy", "orkg",
                    str(GRAPHDB_DATABASE_DIR), CONFIG_TMPL_DIR, CONFIG_DIR], check=True)

    LOG.info("Ingest empty dataset for testing.")
    subprocess.run([GRAPHDB_MGMT_SCRIPT, "ingest_empty", str(GRAPHDB_DATABASE_DIR),
                    "dummy", "orkg", CONFIG_DIR], check=True)

    LOG.info("Start GraphDB engine.")
    subprocess.run([GRAPHDB_MGMT_SCRIPT, "startup", str(GRAPHDB_DATABASE_DIR),
                    "dummy", "orkg"], check=True)
    LOG.info("GraphDB is up")

def _startup_ostrich():
    LOG.info("Create database environment for Ostrich")
    subprocess.run([OSTRICH_MGMT_SCRIPT, "create_env", "dummy", "orkg",
                    str(OSTRICH_DATABASE_DIR), "", ""], check=True)

    LOG.info("Ingest the first ORKG snapshot.")
    subprocess.run([OSTRICH_MGMT_SCRIPT, "ingest", str(OSTRICH_DATABASE_DIR),
                    str(RUN_DIR / "rawdata" / "orkg" / "alldata_vdir"),
                    "ostrich", "orkg", "", "1"], check=True)

    LOG.info("Start Ostrich engine.")
    subprocess.run([OSTRICH_MGMT_SCRIPT, "startup", str(OSTRICH_DATABASE_DIR),
                    "dummy", "orkg"], check=True)
    LOG.info("Ostrich is up")

def _startup_jena():
    LOG.info("Create database environment for Jena.")
    subprocess.run([JENA_MGMT_SCRIPT, "create_env", "dummy", "orkg",
                    str(JENA_DATABASE_DIR), CONFIG_TMPL_DIR, CONFIG_DIR], check=True)

    LOG.info("Ingest the first ORKG snapshot.")
    subprocess.run([JENA_MGMT_SCRIPT, "ingest_empty", str(JENA_DATABASE_DIR),
                    "dummy", "orkg", CONFIG_DIR], check=True)

    LOG.info("Start Jena engine.")
    subprocess.run([JENA_MGMT_SCRIPT, "startup", str(JENA_DATABASE_DIR),
                    "dummy", "orkg", CONFIG_DIR], check=True)
    LOG.info("Jena is up")

def startup():
    """Spin up GraphDB and Ostrich with a dummy ORKG snapshot for query validation."""
    _startup_graphdb()
    _startup_ostrich()
    _startup_jena()


def _copy_subdirs(src, dst):
    """
    Copies all immediate subdirectories from src to dst.
    The destination directory 'dst' must already exist.
    """
    # Ensure destination exists
    if not os.path.exists(dst):
        os.makedirs(dst)

    for entry in os.scandir(src):
        # Check if the entry is a directory (not a symlink)
        if entry.is_dir(follow_symlinks=False):
            # Construct the destination path for this specific subdirectory
            dest_path = os.path.join(dst, entry.name)
            
            # Copy the directory tree. 
            # dirs_exist_ok=True allows copying into an existing directory (Python 3.8+)
            shutil.copytree(entry.path, dest_path, dirs_exist_ok=True)


def extract_queries():
    """
    Read hand-crafted queries from SciQA JSON files, rewrite SPARQL syntax
    to be compatible with starvers, and write one .txt file per query.
    """

    # Create raw queries DIR and copy queries from downloaded_queriey to raw_queries
    shutil.rmtree(RAW_QUERIES_DIR, ignore_errors=True)
    RAW_QUERIES_DIR.mkdir(parents=True, exist_ok=True)
    _copy_subdirs(DOWNLOADED_QUERIES_DIR, RAW_QUERIES_DIR)

    for subdir in SUBDIRS:
        json_path = BASE_DIR / "SciQA-dataset" / subdir / "questions.json"
        if not json_path.exists():
            LOG.info(f"Skipping missing file: {json_path}")
            continue

        with json_path.open("r", encoding="utf-8") as f:
            data = json.load(f)

        for entry in data.get("questions", []):
            if entry.get("auto_generated") is not False:
                continue

            qid    = entry.get("id")
            sparql = entry.get("query", {}).get("sparql")
            if not qid or not sparql:
                continue

            sparql = rewrite_select_aliases(sparql.strip())
            sparql = wrap_aggregations(sparql.strip())
            sparql = sparql.replace(
                "(AVG(?installed_cap_value AS ?avg_installed_cap_value))",
                "(AVG(?installed_cap_value) AS ?avg_installed_cap_value)",
            )
            sparql = unicodedata.normalize("NFC", sparql)
            sparql = re.sub(r"[^\x09\x0A\x0D\x20-\uFFFF]", "", sparql)

            out_file = BASE_DIR / f"{qid}.txt"
            out_file.write_text(PREFIXES + "\n" + sparql + "\n", encoding="utf-8")
            LOG.info(f"Wrote {out_file}")


    
def exclude_queries():
    """
    Test every extracted query against GraphDB and Ostrich.
    Queries that fail any check are removed from disk and logged to excluded_queries.csv.
    """
    ostrich_engine = SPARQLWrapper(endpoint="http://Starvers:42564/sparql")
    ostrich_engine.timeout = 120
    ostrich_engine.setReturnFormat(JSON)
    ostrich_engine.setOnlyConneg(True)
    ostrich_engine.setMethod(POST)
    ostrich_engine.addCustomHttpHeader("Accept", "application/sparql-results+json")

    graphdb_starvers_engine = TripleStoreEngine(
        "http://Starvers:7200/repositories/dummy_orkg",
        "http://Starvers:7200/repositories/dummy_orkg/statements",
        skip_connection_test=True,
    )

    jena_starvers_engine = TripleStoreEngine(
        "http://Starvers:3030/dummy_orkg/sparql",
        "http://Starvers:3030/dummy_orkg/update",
        skip_connection_test=True,
    )

    ostrich_template_path = "/starvers_eval/scripts/5_construct_queries/templates/ostrich/sparql_full.txt"
    with open(ostrich_template_path, "r") as f:
        ostrich_template = f.read()

    query_results = []

    for query_file in BASE_DIR.iterdir():
        if query_file.suffix != ".txt":
            continue

        sparql = query_file.read_text(encoding="utf-8")
        flag_for_deletion = False

        if ASK_REGEX.search(sparql):
            LOG.info(f"Query {query_file.name} is an ASK query and will be excluded")
            query_results.append([query_file.name, 1, "ASK"])
            query_file.unlink()
            continue

        # GraphDB block
        try:
            LOG.info(f"Executing query {query_file.name} against GraphDB")
            graphdb_starvers_engine.query(sparql, yn_timestamp_query=False)
            LOG.info(f"Query {query_file.name} successfully executed against GraphDB")
        except Exception as e:
            LOG.info(f"Original query {query_file.name} is invalid in GraphDB and will be excluded: {e}")
            query_results.append([query_file.name, 1, "Invalid Original in GraphDB"])
            flag_for_deletion = True

        try:
            LOG.info(f"Executing timestamped SPARQL query {query_file.name}.")
            graphdb_starvers_engine.query(sparql, yn_timestamp_query=True)
            LOG.info(f"Timestamped query {query_file.name} successfully executed against GraphDB")
        except Exception as e:
            LOG.info(f"Query {query_file.name} could not get transformed successfully and will be excluded: {e}")
            query_results.append([query_file.name, 1, "Malformed Starvers transformation (GraphDB)"])
            flag_for_deletion = True  

        # Jena block
        try:
            LOG.info(f"Executing query {query_file.name} against Jena TDB2")
            jena_starvers_engine.query(sparql, yn_timestamp_query=False)
            LOG.info(f"Query {query_file.name} successfully executed against Jena TDB2")
        except Exception as e:
            LOG.info(f"Original query {query_file.name} is invalid in Jena TDB2 and will be excluded: {e}")
            query_results.append([query_file.name, 1, "Invalid Original in Jena TDB2"])
            flag_for_deletion = True

        try:
            LOG.info(f"Executing timestamped SPARQL query {query_file.name}.")
            jena_starvers_engine.query(sparql, yn_timestamp_query=True)
            LOG.info(f"Timestamped query {query_file.name} successfully executed against Jena TDB2")
        except Exception as e:
            LOG.info(f"Query {query_file.name} could not get transformed successfully and will be excluded: {e}")
            query_results.append([query_file.name, 1, "Malformed Starvers transformation (Jena)"])
            flag_for_deletion = True

        # Ostrich block    
        try:
            prefixes, versioned_query = split_prefixes_query(sparql)
            modifiers, versioned_query = split_solution_modifiers_query(versioned_query)
            versioned_query = ostrich_template.format(prefixes, 0, versioned_query, modifiers)
            LOG.info(f"Executing query {query_file.name} against Ostrich: {versioned_query}")
            ostrich_engine.setQuery(versioned_query)
            ostrich_engine.query()
            LOG.info(f"Query {query_file.name} successfully executed against Ostrich.")
        except Exception as e:
            LOG.info(f"Original query {query_file.name} is invalid in Ostrich and will be excluded: {e}")
            query_results.append([query_file.name, 1, "Invalid Original in Ostrich"])
            flag_for_deletion = True


        if flag_for_deletion:
            query_file.unlink()
            LOG.info(f"Deleted query file {query_file.name} due to validation failure.")
        else:
            query_results.append([query_file.name, 0, ""])

    excluded = [r[0] for r in query_results if r[1] == 1]
    LOG.info(f"Excluded the following {len(excluded)} queries: {excluded}")

    with open(EXCLUDE_CSV, "a") as f:
        for row in query_results:
            f.write(",".join(map(str, row)) + "\n")


def cleanup():
    """Remove raw SciQA files and temporary database directories."""

    subprocess.call(shlex.split(f"{GRAPHDB_MGMT_SCRIPT} shutdown"))
    time.sleep(5)

    for item in BASE_DIR.iterdir():
        if item.is_dir() and item.name in SUBDIRS:
            for subitem in item.iterdir():
                if subitem.is_file() and subitem.suffix != ".txt":
                    subitem.unlink()
            item.rmdir()
        elif item.is_file() and item.suffix != ".txt":
            item.unlink()

    shutil.rmtree(BASE_DIR / "SciQA-dataset", ignore_errors=True)
    LOG.info(f"Removed directory {BASE_DIR / 'SciQA-dataset'}")

    for attempt in range(3):
        try:
            shutil.rmtree(GRAPHDB_DATABASE_DIR)
            LOG.info(f"Removed directory {GRAPHDB_DATABASE_DIR}")
            break
        except OSError as e:
            LOG.warning(f"Could not remove {GRAPHDB_DATABASE_DIR} (attempt {attempt + 1}): {e}")
            time.sleep(5)
    else:
        LOG.error(f"Failed to remove {GRAPHDB_DATABASE_DIR} after 3 attempts.")

    LOG.info(f"Removing {OSTRICH_DATABASE_DIR} forcefully.")
    shutil.rmtree(OSTRICH_DATABASE_DIR, ignore_errors=True)
    LOG.info(f"Removed directory {OSTRICH_DATABASE_DIR}")


# ---------------------------------------------------------------------------
# Phase 3: count extracted queries and write query_counts.csv
# ---------------------------------------------------------------------------

def _count_by_lines(directory: Path) -> int:
    """Sum non-empty lines across all .txt files (one line = one query)."""
    total = 0
    for f in directory.glob("*.txt"):
        total += sum(1 for line in f.read_text(encoding="utf-8").splitlines() if line.strip())
    return total


def _count_by_files(directory: Path) -> int:
    """Count non-zip files recursively (one file = one query)."""
    return sum(1 for f in directory.rglob("*") if f.is_file() and f.suffix != ".zip")


def write_query_counts():
    """
    Read queries_meta.csv (written by download.sh, contains links but no counts),
    count queries in each directory using the count_method from eval_setup.toml,
    and write query_counts.csv with the results.
    """
    config      = _load_config()
    raw_queries = RUN_DIR / "queries" / "raw_queries"

    if not QUERIES_META_CSV.exists():
        LOG.warning(f"queries_meta.csv not found at {QUERIES_META_CSV}, skipping query count.")
        return

    rows: list[dict] = []
    with open(QUERIES_META_CSV, newline="") as f:
        for row in _csv.DictReader(f):
            rows.append(dict(row))

    def get_count_method(dataset: str, qs_name: str) -> str:
        return (config.get("datasets", {})
                      .get(dataset, {})
                      .get("query_sets", {})
                      .get(qs_name, {})
                      .get("count_method", "files"))

    SUPERSET_MAP = {
        "beara":      "beara",
        "bearb_hour": "bearb",
        "bearb_day":  "bearb",
        "bearc":      "bearc",
        "orkg":       "orkg",
    }

    counts = []
    for row in rows:
        qs_name   = row.get("query_set", "").strip()
        for_label = row.get("for_dataset", "").strip()
        superset  = SUPERSET_MAP.get(for_label, for_label)
        qs_dir    = raw_queries / superset / qs_name
        method    = get_count_method(for_label, qs_name)

        if not qs_dir.exists():
            LOG.warning(f"Query set directory not found: {qs_dir}")
            count = 0
        elif method == "lines":
            count = _count_by_lines(qs_dir)
        else:
            count = _count_by_files(qs_dir)

        LOG.info(f"Query count for {for_label}/{qs_name}: {count} ({method})")
        counts.append({"query_set": qs_name, "for_dataset": for_label, "count": count})

    with open(QUERY_COUNTS_CSV, "w", newline="") as f:
        writer = _csv.DictWriter(f, fieldnames=["query_set", "for_dataset", "count"])
        writer.writeheader()
        writer.writerows(counts)

    LOG.info(f"Saved query counts to {QUERY_COUNTS_CSV}")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Phase 1: clean all raw datasets (parallelised across files)
    #clean_datasets()

    # Phase 2: parse and validate SciQA queries
    startup()
    extract_queries()
    exclude_queries()
    cleanup()

    # Phase 3: count all query sets (including orkg extracted above)
    write_query_counts()