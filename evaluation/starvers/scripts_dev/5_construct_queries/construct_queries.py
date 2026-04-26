from pathlib import Path
import os
import sys
import time
from datetime import datetime, timezone, timedelta
import shutil
import logging
import re
import tomli

from starvers.starvers import timestamp_query, split_prefixes_query

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
if not os.path.exists(f'{os.environ["RUN_DIR"]}/output/logs/construct_queries'):
    os.makedirs(f'{os.environ["RUN_DIR"]}/output/logs/construct_queries')
with open(f'{os.environ["RUN_DIR"]}/output/logs/construct_queries/construct_queries.txt', "w") as log_file:
    log_file.write("")
logging.basicConfig(handlers=[logging.FileHandler(filename=f"{os.environ['RUN_DIR']}/output/logs/construct_queries/construct_queries.txt", 
                                                encoding='utf-8', mode='a+')],
                    format="%(asctime)s %(name)s:%(levelname)s:%(message)s", 
                    datefmt="%F %A %T", 
                    level=logging.INFO)
starvers_log = logging.getLogger("starvers.starvers")
starvers_log.setLevel(logging.ERROR)


# ---------------------------------------------------------------------------
# Environment / path constants
# ---------------------------------------------------------------------------
raw_queries_base=f"{os.environ['RUN_DIR']}/queries/raw_queries/"
output_queries_base=f"{os.environ['RUN_DIR']}/queries/final_queries/"
query_rewriting_measurements_path=f"{os.environ['RUN_DIR']}/output/measurements/query_rewriting_times.csv"

policies = os.environ.get("policies").split(" ")
datasets = os.environ.get("datasets").split(" ")

with open("/starvers_eval/configs/eval_setup.toml", mode="rb") as config_file:
    eval_setup = tomli.load(config_file)
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


def _count_queries():
    # Iterate recursively through f"{os.environ['RUN_DIR']}/queries/final_queries/"
    # Count the .txt files at the bottom of each branch and
    #  aggregate the counts on the third directory level,
    #  e.g. f"{os.environ['RUN_DIR']}/queries/final_queries/ic_sr_ng/orkg/complex
    # Save the counts for each policy, dataset, and query set in a csv file at f"{os.environ['RUN_DIR']}/output/logs/construct_queries/query_counts.csv" with columns: policy, dataset, query_set, query_count

    QUERIES_DIR = f"{os.environ['RUN_DIR']}/queries/final_queries/"
    query_count = 0
    with open(f"{os.environ['RUN_DIR']}/output/logs/construct_queries/query_counts.csv", 'w') as count_file:
        count_file.write("policy,dataset,query_set,query_count\n")
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
                    query_count = sum([len(files) for r, d, files in os.walk(query_set_path) if any(file.endswith('.txt') for file in files)])
                    count_file.write(f"{policy},{dataset},{query_set},{query_count}\n")


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------
def main():
    init_version_timestamp = datetime(2022,10,1,12,0,0,0,LOCAL_TIMEZONE)
    vers_ts = init_version_timestamp

    # Write measure file header
    with open(query_rewriting_measurements_path, 'w') as measure_file:
        measure_file.write("policy,dataset,query_set,snapshot,query,rewriting_time\n")

    # Generate queries  
    logging.info("Start generating queries.")
    for dataset, dataset_infos in eval_setup['datasets'].items():
        if dataset not in datasets:
            logging.info(f"Skipping dataset {dataset} as it is not in the list of requested datasets: {datasets}")
            continue

        logging.info(f"Generating queries for dataset: {dataset}")
        if 'query_sets' not in dataset_infos:
            logging.info("No query sets defined for dataset {0}, skipping query construction.".format(dataset))
            continue

        query_sets = dataset_infos['query_sets'].items()
        query_set_context = dataset_infos['superset']
        
        for query_set_name, query_set in query_sets:
            logging.info(f"Generating queries for dataset: {dataset} and query set {query_set_name}")

            policy_infos = query_set['policies'].items()
            raw_queries_dir_path = raw_queries_base + query_set_context + "/" + query_set_name

            for policy, infos in policy_infos:
                if policy not in policies:
                    logging.info("Queries for policy {0} will not be constructed as it is not one of the requested policies: {1}".format(policy, policies))
                    continue
                
                template_relative_path = infos['template']
                query_set_versions = infos['versions']

                logging.info(f"Generating queries for {query_set_versions} dataset versions of {dataset}, query set {query_set_name}, and policy {policy}")


                for query_set_version in range(query_set_versions):
                    logging.info(f"Generating queries for dataset: {dataset}, query set {query_set_name}, policy {policy}, and query set version {query_set_version}")
                    output_queries_dir_path = Path(output_queries_base + policy + "/" + dataset + "/" + query_set_name + "/" + str(query_set_version))
                    if output_queries_dir_path.exists():
                        shutil.rmtree(output_queries_dir_path)
                    
                    logging.info("Create directory {0}".format(output_queries_dir_path))
                    output_queries_dir_path.mkdir(parents=True, exist_ok=True)
                    
                    logging.info("Create queries and save to {0}".format(output_queries_dir_path))
                    logging.info(f"There are {len(os.listdir(raw_queries_dir_path))} raw queries.")
                    logging.info(f"The raw query diretory is: {raw_queries_dir_path}")
                    
                    for k, raw_query_name in enumerate(os.listdir(raw_queries_dir_path)):
                        if not raw_query_name.endswith(".txt"):
                            logging.info(f"Skipping file {raw_query_name} as it does not end with .txt")
                            continue

                        if not os.path.isfile(raw_queries_dir_path + "/" + raw_query_name):
                            logging.error("No such file: " + raw_queries_dir_path + "/" + raw_query_name)
                        
                        logging.info(f"Template relative path is: {template_relative_path}")
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
                                
                                
                                # Write query string to file. For tb_sr_rs transform it to a timestamp-based rdfstar query first
                                output_query_file_name = raw_query_name.split('.')[0] + "_q" + str(queryCounter) + "_v" + str(query_set_version) + ".txt"
                                with open(output_queries_dir_path / output_query_file_name, 'w') as output_file:
                                    if policy in ["tb_sr_rs"]:
                                        start = time.time()
                                        timestamped_output_query = timestamp_query(output_query, vers_ts)
                                        end = time.time()
                                        rewriting_time = end - start
                                        
                                        with open(query_rewriting_measurements_path, 'a') as measure_file:
                                            measure_file.write("{0},{1},{2},{3},{4},{5}\n".format(policy, dataset, 
                                                query_set_name, str(query_set_version), output_query_file_name, rewriting_time))
                                        output_file.write(timestamped_output_query[0])
                                    else:
                                        output_file.write(output_query) 
                    # Only for tb_sr_rs policy
                    vers_ts = vers_ts + timedelta(seconds=1)
                vers_ts = init_version_timestamp      

    logging.info("Finished generating queries.")

    # Count generated queries
    logging.info("Counting generated queries.")
    _count_queries()
    logging.info(f"Finished counting generated queries. File saved to {os.environ['RUN_DIR']}/output/logs/construct_queries/query_counts.csv")

if __name__ == "__main__":
    main()
