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

Phase 2 — parse_SciQA_queries:
  1. startup()      — spin up GraphDB and Ostrich with a dummy ORKG snapshot
  2. extract_queries() — pull hand-crafted queries from SciQA JSON files,
                         rewrite aliases/aggregations, write one .txt per query
  3. exclude_queries() — test each query against GraphDB and Ostrich,
                         remove those that fail
  4. cleanup()      — remove raw files and temp databases
"""

import json
import logging
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
import pandas as pd
import unicodedata
from datetime import datetime
from pathlib import Path
import tomli
from SPARQLWrapper import GET, JSON, POST, POSTDIRECTLY, SPARQLWrapper

from starvers.starvers import TripleStoreEngine, split_prefixes_query

sys.path.append(str(Path("/starvers_eval/scripts/5_construct_queries").resolve()))
from construct_queries import split_solution_modifiers_query


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
RDF_VALIDATOR_JAR   = str(SCRIPT_DIR / "2_preprocess_data/RDFValidator/target/rdfvalidator-1.0-jar-with-dependencies.jar")

# Only these two dataset variants are cleaned — order matters, do not change
DATASET_VARIANTS = ["ic", "BEAR_ng"]

# Datasets come from the environment variable (space-separated, mirroring bash ${datasets})
DATASETS = os.environ.get("datasets", "").split()

# Java home for the RDF validator subprocess
JAVA_ENV = {**os.environ, "JAVA_HOME": "/opt/java/java11/openjdk",
            "PATH": "/opt/java/java11/openjdk/bin:" + os.environ.get("PATH", "")}

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------

LOG_DIR = RUN_DIR / "output" / "logs" / "preprocess_data"
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_FILE   = LOG_DIR / "preprocess_data.txt"
EXCLUDE_CSV = LOG_DIR / "excluded_queries.csv"
PREPROCESS_CSV = LOG_DIR / "preprocess_summary.csv"

# Clear log files (mirrors > $log_file and the open(..., "w") calls in the original)
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

# Dedicated logger for the cleaning phase (writes to clean_datasets.txt)
clean_handler = logging.FileHandler(LOG_FILE, encoding="utf-8", mode="a+")
clean_handler.setFormatter(logging.Formatter("%(asctime)s root:%(levelname)s:%(message)s", datefmt="%Y-%m-%d %A %H:%M:%S"))
LOG = logging.getLogger("clean_datasets")
LOG.addHandler(clean_handler)


# ---------------------------------------------------------------------------
# TOML config helpers (replaces get_snapshot_version / get_snapshot_filename_struc)
# ---------------------------------------------------------------------------

def _load_config() -> dict:
    with open(CONFIG_PATH, "rb") as f:
        return tomli.load(f)


def get_snapshot_version(config: dict, dataset: str) -> int | None:
    """Return snapshot_versions for the given dataset, or None if not registered."""
    entry = config.get("datasets", {}).get(dataset)
    if not entry or "snapshot_versions" not in entry:
        LOG.error(
            f"The dataset {dataset} is not within the registered datasets in eval_setup.toml. "
            "This dataset will be skipped."
        )
        return None
    return entry["snapshot_versions"]


def get_snapshot_filename_format(config: dict, dataset: str) -> str | None:
    """Return a zero-padded format string like '%04g', or None if not registered."""
    entry = config.get("datasets", {}).get(dataset)
    if not entry or "ic_basename_length" not in entry:
        LOG.error(
            f"The dataset {dataset} is not within the registered datasets in eval_setup.toml. "
            "This dataset will be skipped."
        )
        return None
    return f"%0{entry['ic_basename_length']}g"



# ---------------------------------------------------------------------------
# Phase 1 helpers: skolemization and RDF validation (idempotent)
# ---------------------------------------------------------------------------

def _read_lines(path: Path) -> list[str]:
    with open(path, "r", encoding="utf-8") as f:
        return f.readlines()


def _write_lines(path: Path, lines: list[str]):
    with open(path, "w", encoding="utf-8") as f:
        f.writelines(lines)


def _prepend_comment(path: Path, comment: str):
    """Insert a comment line at the top of a file."""
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


def skolemize_subject_blanks(dataset: str, variant: str, raw_ds: Path, file_number: int, preprocess_df: pd.DataFrame):
    """
    Replace blank nodes in subject position (_:xxx ...) with URIs (<_:xxx ...>).
    Idempotent: skips if the header comment is already present.
    """
    header_marker = "# skolemized_blank_nodes_in_subject_position"
    already_done = any(header_marker in l for l in _first_n_lines(raw_ds))

    if already_done:
        # Extract the number with regex
        match = re.search(r"# skolemized_blank_nodes_in_subject_position: (\d+)", "".join(_first_n_lines(raw_ds)))
        preprocess_df.loc[(dataset, variant, file_number), "skolemized_subjects"] = int(match.group(1)) if match else 0
        
        LOG.info(
            f"{raw_ds}: skolemized blank nodes in subject position: 0 in this run. "
            f"Previously skolemized nodes: See comment in {raw_ds}"
        )
        return

    # Count and replace blank nodes in subject position
    pattern = re.compile(r"^(_:[a-zA-Z0-9]+)", re.MULTILINE)
    content = raw_ds.read_text(encoding="utf-8")
    cnt = len(pattern.findall(content))
    content = pattern.sub(r"<\1>", content)
    raw_ds.write_text(content, encoding="utf-8")

    _prepend_comment(raw_ds, f"# skolemized_blank_nodes_in_subject_position: {cnt}")
    LOG.info(f"{raw_ds}: skolemized blank nodes in subject position: {cnt}")

    preprocess_df.loc[(dataset, variant, file_number), "skolemized_subjects"] = cnt


def skolemize_object_blanks(dataset: str, variant: str, raw_ds: Path, file_number: int, preprocess_df: pd.DataFrame):
    """
    Replace blank nodes in object position with URIs.
    Idempotent: skips if the header comment is already present.
    """
    header_marker = "# skolemized_blank_nodes_in_object_position"
    already_done = any(header_marker in l for l in _first_n_lines(raw_ds))

    if already_done:
        match = re.search(r"# skolemized_blank_nodes_in_object_position: (\d+)", "".join(_first_n_lines(raw_ds)))
        preprocess_df.loc[(dataset, variant, file_number), "skolemized_objects"] = int(match.group(1)) if match else 0
        
        LOG.info(
            f"{raw_ds}: skolemized blank nodes in object position: 0 in this run. "
            f"Previously skolemized nodes: See comment in {raw_ds}"
        )
        return

    pattern = re.compile(
        r"(^[^#].*)(_:[a-zA-Z0-9]+)(\s*(<[a-zA-Z0-9_/:.]+>){0,1}\s*\.$)",
        re.MULTILINE,
    )
    content = raw_ds.read_text(encoding="utf-8")
    cnt = len(pattern.findall(content))
    content = pattern.sub(r"\1<\2>\3", content)
    raw_ds.write_text(content, encoding="utf-8")

    _prepend_comment(raw_ds, f"# skolemized_blank_nodes_in_object_position: {cnt}")
    LOG.info(f"{raw_ds}: skolemized blank nodes in object position: {cnt}")

    preprocess_df.loc[(dataset, variant, file_number), "skolemized_objects"] = cnt

def validate_and_comment_invalid_triples(dataset: str, variant: str, raw_ds: Path, file_number: int, preprocess_df: pd.DataFrame):
    """
    Run the Java RDF validator to comment out invalid triples.
    Idempotent: skips if the header comment is already present.
    The validator writes a cleaned copy which we then move back over the original.
    """
    header_marker = "# invalid_lines_excluded"
    already_done = any(header_marker in l for l in _first_n_lines(raw_ds))

    LOG.info(f"Validating {raw_ds}")

    if already_done:
        match = re.search(r"# invalid_lines_excluded: (\d+)", "".join(_first_n_lines(raw_ds)))
        preprocess_df.loc[(dataset, variant, file_number), "invalid_triples"] = int(match.group(1)) if match else 0

        LOG.info(
            f"{raw_ds}: 0 in this run. Previously excluded lines: see first comment in {raw_ds}"
        )
        return

    clean_ds = raw_ds.with_name(raw_ds.stem + "_clean" + raw_ds.suffix)
    subprocess.run(
        ["java", "-jar", RDF_VALIDATOR_JAR, str(raw_ds), str(clean_ds)],
        env=JAVA_ENV,
        check=True,
    )
    shutil.move(str(clean_ds), str(raw_ds))

    # Count comment lines added by the validator (subtract the 2 pre-existing header comments)
    comment_lines = sum(1 for l in _read_lines(raw_ds) if l.startswith("# "))
    excluded = max(0, comment_lines - 2)

    _prepend_comment(raw_ds, f"# invalid_lines_excluded: {excluded}")
    LOG.info(f"{raw_ds}: {excluded}")

    preprocess_df.loc[(dataset, variant, file_number), "invalid_triples"] = excluded


# ---------------------------------------------------------------------------
# Phase 1: clean_datasets  
# ---------------------------------------------------------------------------

def clean_datasets():
    LOG.info("Start corrections")
    config = _load_config()

    preprocess_df = pd.DataFrame(columns=["dataset", "variant", "file_number", "skolemized_subjects", "skolemized_objects", "invalid_triples"])
    preprocess_df.set_index(["dataset", "variant", "file_number"], inplace=True)

    for dataset in DATASETS:
        versions = get_snapshot_version(config, dataset)
        filename_fmt = get_snapshot_filename_format(config, dataset)

        if versions is None or filename_fmt is None:
            continue

        for ds_var in DATASET_VARIANTS:

            # Resolve variant-specific path and version count
            if ds_var == "ic":
                # ds_rel_path and base_name_tmpl are resolved per snapshot version below
                pass
            elif ds_var == "BEAR_ng":
                ds_path = RUN_DIR / "rawdata" / dataset / "alldata.TB.nq"
                if not ds_path.is_file():
                    LOG.info(
                        f"The BEAR named graphs dataset does not exist at {ds_path}. "
                        "Skipping processing of this dataset."
                    )
                    continue
                versions = 1  # BEAR_ng has exactly one file
            else:
                LOG.error("Dataset variant must be in ic or BEAR_ng.")
                sys.exit(2)

            LOG.info(f"Correcting {dataset} for {ds_var} dataset variant")

            # Iterate over snapshot versions (seq -f equivalent)
            for c_int in range(1, versions + 1):
                c = filename_fmt % c_int  # e.g. "0001"

                if ds_var == "ic":
                    raw_ds = RUN_DIR / "rawdata" / dataset / "alldata.IC.nt" / f"{c}.nt"
                else:  # BEAR_ng
                    raw_ds = RUN_DIR / "rawdata" / dataset / "alldata.TB.nq"

                # --- Three idempotent cleaning steps ---
                skolemize_subject_blanks(dataset, ds_var, raw_ds, c_int, preprocess_df)
                skolemize_object_blanks(dataset, ds_var, raw_ds, c_int, preprocess_df)
                validate_and_comment_invalid_triples(dataset, ds_var, raw_ds, c_int, preprocess_df)

    # Save summary of preprocessing steps to CSV
    preprocess_df.to_csv(PREPROCESS_CSV)
    LOG.info(f"Saved preprocessing summary to {PREPROCESS_CSV}")
# ---------------------------------------------------------------------------
# Phase 2 constants and regex (was parse_SciQA_queries.py top-level)
# ---------------------------------------------------------------------------

PREFIXES = """\
PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
PREFIX orkgr: <http://orkg.org/orkg/resource/>
PREFIX orkgc: <http://orkg.org/orkg/class/>
PREFIX orkgp: <http://orkg.org/orkg/predicate/>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
"""

BASE_DIR     = RUN_DIR / "queries" / "raw_queries" / "orkg" / "complex"
SUBDIRS      = ["train", "test", "valid"]

GRAPHDB_DATABASE_DIR = RUN_DIR / "databases" / "preprocess_data" / "graphdb" / "dummy_orkg"
OSTRICH_DATABASE_DIR = RUN_DIR / "databases" / "preprocess_data" / "ostrich" / "dummy_orkg"

SELECT_ALIAS_RE = re.compile(
    r"""
    (SELECT(?:\s+\?[a-zA-Z0-9_-]*)*)         # SELECT + optional variables before alias
    \s*
    (?<!\()                                   # not already wrapped
    (
        (?:
            (?:COUNT|SUM|AVG|MIN|MAX)\([^\)]+\)
            |
            \?[a-zA-Z0-9_-]+
        )
        \s+AS\s+\?[a-zA-Z0-9_-]+
    )
    (?!\))                                    # not already wrapped
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
# Phase 2: startup, extract, exclude, cleanup  (was parse_SciQA_queries.py)
# ---------------------------------------------------------------------------

def startup():
    """Spin up GraphDB and Ostrich with a dummy ORKG snapshot for query validation."""

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


def extract_queries():
    """
    Read hand-crafted queries from SciQA JSON files, rewrite SPARQL syntax
    to be compatible with starvers, and write one .txt file per query.
    """
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

            # Rewrite aliases and bare aggregations to be SPARQL-spec compliant
            sparql = rewrite_select_aliases(sparql.strip())
            sparql = wrap_aggregations(sparql.strip())

            # Fix known broken query (hardcoded, mirrors original script)
            sparql = sparql.replace(
                "(AVG(?installed_cap_value AS ?avg_installed_cap_value))",
                "(AVG(?installed_cap_value) AS ?avg_installed_cap_value)",
            )

            # Normalise Unicode and strip control characters (keep tab/newline/CR)
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
    # Ostrich SPARQL endpoint
    ostrich_engine = SPARQLWrapper(endpoint="http://Starvers:42564/sparql")
    ostrich_engine.timeout = 120
    ostrich_engine.setReturnFormat(JSON)
    ostrich_engine.setOnlyConneg(True)
    ostrich_engine.setMethod(POST)
    ostrich_engine.addCustomHttpHeader("Accept", "application/sparql-results+json")

    # GraphDB / RDF-star endpoint
    rdf_star_engine = TripleStoreEngine(
        "http://Starvers:7200/repositories/dummy_orkg",
        "http://Starvers:7200/repositories/dummy_orkg/statements",
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

        # --- Exclude ASK queries ---
        if ASK_REGEX.search(sparql):
            LOG.info(f"Query {query_file.name} is an ASK query and will be excluded")
            query_results.append([query_file.name, 1, "ASK"])
            query_file.unlink()
            continue

        # --- Validate against GraphDB (original, non-timestamped) ---
        try:
            rdf_star_engine.query(sparql, yn_timestamp_query=False)
        except Exception as e:
            LOG.info(f"Original query {query_file.name} is invalid in GraphDB and will be excluded: {e}")
            query_results.append([query_file.name, 1, "Invalid Original in GraphDB"])
            query_file.unlink()
            continue

        # --- Validate against Ostrich (versioned query) ---
        try:
            prefixes, versioned_query = split_prefixes_query(sparql)
            modifiers, versioned_query = split_solution_modifiers_query(versioned_query)
            versioned_query = ostrich_template.format(prefixes, 0, versioned_query, modifiers)
            LOG.info(f"Versioned query for Ostrich execution: {versioned_query}")
            ostrich_engine.setQuery(versioned_query)
            ostrich_engine.query()
        except Exception as e:
            LOG.info(f"Original query {query_file.name} is invalid in Ostrich and will be excluded: {e}")
            query_results.append([query_file.name, 1, "Invalid Original in Ostrich"])
            query_file.unlink()
            continue

        # --- Validate starvers timestamped transformation ---
        try:
            rdf_star_engine.query(sparql, yn_timestamp_query=True)
        except Exception as e:
            LOG.info(f"Query {query_file.name} could not get transformed successfully and will be excluded: {e}")
            query_results.append([query_file.name, 1, "Malformed Starvers transformation"])
            query_file.unlink()
            continue

        query_results.append([query_file.name, 0, ""])

    # Log summary
    excluded = [r[0] for r in query_results if r[1] == 1]
    LOG.info(f"Excluded the following {len(excluded)} queries: {excluded}")

    with open(EXCLUDE_CSV, "a") as f:
        for row in query_results:
            f.write(",".join(map(str, row)) + "\n")


def cleanup():
    """Remove raw SciQA files and temporary database directories."""

    # Shut down GraphDB
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

    # Retry rmtree for GraphDB dir since it may still be releasing locks
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

    LOG.info(f"Removing {GRAPHDB_DATABASE_DIR} forcefully.")
    shutil.rmtree(OSTRICH_DATABASE_DIR, ignore_errors=True)
    LOG.info(f"Removed directory {OSTRICH_DATABASE_DIR}")



# ---------------------------------------------------------------------------
# Entry point: run both phases in order
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    # Phase 1: clean all raw datasets
    clean_datasets()

    # Phase 2: parse and validate SciQA queries
    startup()
    extract_queries()
    exclude_queries()
    cleanup()
