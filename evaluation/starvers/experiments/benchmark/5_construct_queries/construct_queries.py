from pathlib import Path
import os
import sys
import time
from datetime import datetime, timezone, timedelta
import shutil
import logging
import re
import tomli
from collections import defaultdict

from starvers.starvers import timestamp_query, split_prefixes_query
from experiments.logging import setup_logging, log_start, log_end

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
BASE_LOG_DIR, LOG = setup_logging("construct_queries")

# ---------------------------------------------------------------------------
# Static eval parameters
# ---------------------------------------------------------------------------
CONFIG_PATH = Path("/starvers_eval/configs/eval_setup.toml")
def _load_config() -> dict:
    with open(CONFIG_PATH, "rb") as f:
        return tomli.load(f)
static_eval_params = _load_config()


# ---------------------------------------------------------------------------
# Environment / path constants
# ---------------------------------------------------------------------------
raw_queries_base=f"{os.environ['RUN_DIR']}/queries/raw_queries/"
output_queries_base=f"{os.environ['RUN_DIR']}/queries/final_queries/"
query_rewriting_measurements_path=f"{os.environ['RUN_DIR']}/output/measurements/queries_rewriting_times.csv"

policies = os.environ.get("policies").split(" ")
datasets = os.environ.get("datasets").split(" ")
triple_stores = os.environ.get("triple_stores").split(" ")

LOCAL_TIMEZONE = datetime.now(timezone.utc).astimezone().tzinfo

# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------
def split_solution_modifiers_query(query: str) -> list:
    """
    Separates following solution modifiers from the query:
    * Order by
    * Offset
    * Limit
    """
    query_without_solution_modifiers = re.sub(r'(ORDER BY.*|LIMIT.*|OFFSET.*)', '', query, flags=re.DOTALL)
    solution_modifiers = ' '.join(re.findall(r'(ORDER BY.*|LIMIT.*|OFFSET.*)', query, flags=re.DOTALL))
    return solution_modifiers, query_without_solution_modifiers


def _output_policy_dir(policy: str, triple_store: str) -> str:
    """Return the top-level output policy directory for a (policy, triple_store).

    The tb_sr_re policy is emitted under the serialization-specific directory:
    Jena RDF 1.2 uses tb_sr_re_RDF1_2, all other stores use tb_sr_re_star. The
    template file itself is shared (templates/tb_sr_re/<type>.txt).
    """
    if policy == "tb_sr_re":
        return "tb_sr_re_RDF1_2" if triple_store == "jenatdb2_RDF1_2" else "tb_sr_re_star"
    return policy


def _mode_for(policy: str, triple_store: str) -> str | None:
    """Return the starvers versioning mode for a (policy, triple_store), or None."""
    if policy == "tb_sr_rs":
        return "decorator"
    if policy == "tb_sr_re":
        return "reification_rdf12" if triple_store == "jenatdb2_RDF1_2" else "reification"
    return None


def _count_queries():
    # The generated query tree is laid out as
    #   final_queries/<policy>/<dataset>/<query_set>/<query_set_version>/<file>.txt
    # (see _output_policy_dir and the write loop below). There is no triple
    # store level, and each query set is split into per-version directories.
    # Aggregate the number of .txt files per (policy, dataset) and write one
    # row per (policy, dataset) to queries_counts.csv. The GUI reads only the
    # policy, dataset, and query_count columns, so we emit exactly those.

    QUERIES_DIR = f"{os.environ['RUN_DIR']}/queries/final_queries/"
    counts = defaultdict(int)
    for policy in os.listdir(QUERIES_DIR):
        policy_path = os.path.join(QUERIES_DIR, policy)
        if not os.path.isdir(policy_path):
            continue
        for dataset in os.listdir(policy_path):
            dataset_path = os.path.join(policy_path, dataset)
            if not os.path.isdir(dataset_path):
                continue
            for query_set in os.listdir(dataset_path):
                query_set_path = os.path.join(dataset_path, query_set)
                if not os.path.isdir(query_set_path):
                    continue
                for query_set_version in os.listdir(query_set_path):
                    version_path = os.path.join(query_set_path, query_set_version)
                    if not os.path.isdir(version_path):
                        continue
                    counts[(policy, dataset)] += sum(
                        len(files) for _, _, files in os.walk(version_path)
                        if any(file.endswith('.txt') for file in files)
                    )
    with open(f"{os.environ['RUN_DIR']}/output/measurements/queries_counts.csv", 'w') as count_file:
        count_file.write("policy,dataset,query_count\n")
        for (policy, dataset), count in sorted(counts.items()):
            count_file.write(f"{policy},{dataset},{count}\n")


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------
def main():
    log_start("Start Construct Queries", LOG)
    init_version_timestamp = datetime(2022,10,1,12,0,0,0,LOCAL_TIMEZONE)
    vers_ts = init_version_timestamp

    # Write measure file header
    with open(query_rewriting_measurements_path, 'w') as measure_file:
        measure_file.write("policy,triple_store,dataset,query_set,snapshot,query,rewriting_time\n")

    # Generate queries
    LOG.info("Start generating queries.")
    for triple_store in triple_stores:
        LOG.info(f"Generating queries for triple store: {triple_store}")
        for dataset, dataset_infos in static_eval_params['datasets'].items():
            if dataset not in datasets:
                LOG.info(f"Skipping dataset {dataset} as it is not in the list of requested datasets: {datasets}")
                continue

            LOG.info(f"Generating queries for dataset: {dataset}")
            if 'query_sets' not in dataset_infos:
                LOG.info("No query sets defined for dataset {0}, skipping query construction.".format(dataset))
                continue

            query_sets = dataset_infos['query_sets'].items()
            query_set_context = dataset_infos['superset']
            
            for query_set_name, query_set in query_sets:
                LOG.info(f"Generating queries for dataset: {dataset} and query set {query_set_name}")

                policy_infos = query_set['policies'].items()
                raw_queries_dir_path = raw_queries_base + query_set_context + "/" + query_set_name

                for policy, infos in policy_infos:
                    if policy not in policies:
                        LOG.info("Queries for policy {0} will not be constructed as it is not one of the requested policies: {1}".format(policy, policies))
                        continue
                    
                    template_relative_path = infos['template']
                    if isinstance(template_relative_path, list):
                        template_relative_path = template_relative_path[0]
                    query_set_versions = infos['versions']

                    LOG.info(f"Generating queries for {query_set_versions} dataset versions of {dataset}, query set {query_set_name}, and policy {policy}")

                    # The output dir and versioning mode are decided by the (triple_store, policy)
                    # combination; the template file itself is shared.
                    output_policy_dir = _output_policy_dir(policy, triple_store)
                    mode = _mode_for(policy, triple_store)

                    LOG.info(f"Policy {policy}, triple store {triple_store} -> output policy dir {output_policy_dir}, mode {mode}, template {template_relative_path}")

                    for query_set_version in range(query_set_versions):
                        LOG.info(f"Generating queries for dataset: {dataset}, query set {query_set_name}, policy {output_policy_dir}, triple store {triple_store}, and query set version {query_set_version}")
                        output_queries_dir_path = Path(output_queries_base + output_policy_dir + "/" + dataset + "/" + query_set_name + "/" + str(query_set_version))
                        if output_queries_dir_path.exists():
                            shutil.rmtree(output_queries_dir_path)

                        LOG.info("Create directory {0}".format(output_queries_dir_path))
                        output_queries_dir_path.mkdir(parents=True, exist_ok=True)

                        LOG.info("Create queries and save to {0}".format(output_queries_dir_path))
                        LOG.info(f"There are {len(os.listdir(raw_queries_dir_path))} raw queries.")
                        LOG.info(f"The raw query diretory is: {raw_queries_dir_path}")

                        for k, raw_query_name in enumerate(os.listdir(raw_queries_dir_path)):
                            if not raw_query_name.endswith(".txt"):
                                LOG.info(f"Skipping file {raw_query_name} as it does not end with .txt")
                                continue

                            if not os.path.isfile(raw_queries_dir_path + "/" + raw_query_name):
                                logging.error("No such file: " + raw_queries_dir_path + "/" + raw_query_name)

                            LOG.info(f"Template relative path is: {template_relative_path}")
                            with open(raw_queries_dir_path + "/" + raw_query_name, 'r', encoding='utf-8', errors='ignore') as raw_query_file:
                                if template_relative_path.split('/')[1] == 'ts' and query_set_name == "lookup": # BEAR lookup queries are just one line
                                    raw_queries = raw_query_file.readlines()
                                else:
                                    raw_queries = [raw_query_file.read()]

                                # Iterate over file that holds one or more raw queries.
                                for i, raw_query in enumerate(raw_queries):

                                    prefixes, raw_query = split_prefixes_query(raw_query)
                                    modifiers, raw_query = split_solution_modifiers_query(raw_query)
                                    queryCounter =  i if template_relative_path.split('/')[1] == 'ts' else k
                                    output_query = ""

                                    # Read template and create query string
                                    with open(os.path.join(sys.path[0]) +"/templates/" + template_relative_path + ".txt", 'r') as templateFile:
                                        template = templateFile.read()

                                        if policy == "cb_sr_ng":
                                            max_version_digits = len(str(query_set_versions))
                                            output_query = template.format(prefixes, 
                                                                           str(query_set_version).zfill(max_version_digits),
                                                                           raw_query, 
                                                                           modifiers)
                                        else:
                                            output_query = template.format(prefixes, 
                                                                           str(query_set_version),
                                                                           raw_query, 
                                                                           modifiers)


                                    # Write query string to file. For the RDF-star policies transform it to a
                                    # timestamp-based RDF-star query first.
                                    output_query_file_name = raw_query_name.split('.')[0] + "_q" + str(queryCounter) + "_v" + str(query_set_version) + ".txt"
                                    with open(output_queries_dir_path / output_query_file_name, 'w') as output_file:
                                        if mode is not None:
                                            start = time.time()
                                            timestamped_output_query = timestamp_query(output_query, vers_ts, mode=mode)
                                            end = time.time()
                                            rewriting_time = end - start

                                            with open(query_rewriting_measurements_path, 'a') as measure_file:
                                                measure_file.write("{0},{1},{2},{3},{4},{5},{6}\n".format(policy, triple_store, dataset, 
                                                    query_set_name, str(query_set_version), output_query_file_name, rewriting_time))
                                            output_file.write(timestamped_output_query[0])
                                        else:
                                            output_file.write(output_query)
                    # Only for tb_sr_rs policy
                    vers_ts = vers_ts + timedelta(seconds=1)
                vers_ts = init_version_timestamp      

    LOG.info("Finished generating queries.")

    # Count generated queries
    LOG.info("Counting generated queries.")
    _count_queries()
    LOG.info(f"Finished counting generated queries. File saved to {os.environ['RUN_DIR']}/output/measurements/queries_counts.csv")
    log_end("End Construct Queries", LOG)

if __name__ == "__main__":
    main()
