from pathlib import Path
import os
import sys
from datetime import datetime, timezone, timedelta
import shutil
import logging
import re
import tomli

from starvers.starvers import timestamp_query, split_prefixes_query

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



def main():
    ############################################# Parameters ################################################################
    raw_queries_base="/starvers_eval/queries/raw_queries/"
    output_queries_base="/starvers_eval/queries/final_queries/"

    policies_cmd = sys.argv[1]
    policies = policies_cmd.split(" ")
    with open("/starvers_eval/configs/eval_setup.toml", mode="rb") as config_file:
        eval_setup = tomli.load(config_file)
    LOCAL_TIMEZONE = datetime.now(timezone.utc).astimezone().tzinfo
    init_version_timestamp = datetime(2022,10,1,12,0,0,0,LOCAL_TIMEZONE)
    vers_ts = init_version_timestamp

    ############################################# Logging ###############################################################
    if not os.path.exists('/starvers_eval/output/logs/construct_queries'):
        os.makedirs('/starvers_eval/output/logs/construct_queries')
    with open('/starvers_eval/output/logs/construct_queries/construct_queries.txt', "w") as log_file:
        log_file.write("")
    logging.basicConfig(handlers=[logging.FileHandler(filename="/starvers_eval/output/logs/construct_queries/construct_queries.txt", 
                                                    encoding='utf-8', mode='a+')],
                        format="%(asctime)s %(name)s:%(levelname)s:%(message)s", 
                        datefmt="%F %A %T", 
                        level=logging.INFO)
    starvers_log = logging.getLogger("starvers.starvers")
    starvers_log.setLevel(logging.ERROR)

    ################################################## Generate queries ################################################# 
    for dataset, dataset_infos in eval_setup['datasets'].items():
        query_sets = dataset_infos['query_sets'].items()
        query_set_context = dataset_infos['superset']
        
        for query_set_name, query_set in query_sets:
            policy_infos = query_set['policies'].items()
            raw_queries_dir_path = raw_queries_base + query_set_context + "/" + query_set_name

            for policy, infos in policy_infos:
                if policy not in policies:
                    logging.info("Queries for policy {0} will not be constructed as it is not one of the requested policies: {1}".format(policy, policies))
                    continue

                template_relative_path = infos['template']
                query_set_versions = infos['versions']

                for query_set_version in range(query_set_versions):
                    output_queries_dir_path = Path(output_queries_base + policy + "/" + dataset + "/" + query_set_name + "/" + str(query_set_version))
                    if output_queries_dir_path.exists():
                        shutil.rmtree(output_queries_dir_path)
                    
                    logging.info("Create directory {0}".format(output_queries_dir_path))
                    output_queries_dir_path.mkdir(parents=True, exist_ok=True)
                    
                    logging.info("Create queries and save to {0}".format(output_queries_dir_path))
                    for k, raw_query_name in enumerate(os.listdir(raw_queries_dir_path)):
                        if not os.path.isfile(raw_queries_dir_path + "/" + raw_query_name):
                            logging.error("No such file: " + raw_queries_dir_path + "/" + raw_query_name)
                        
                        with open(raw_queries_dir_path + "/" + raw_query_name, 'r') as raw_query_file:
                            if template_relative_path.split('/')[1] == 'ts':
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
                                        output_query = template.format(prefixes, str(query_set_version).zfill(max_version_digits),
                                            raw_query, modifiers)
                                    else:
                                        output_query = template.format(prefixes, str(query_set_version),
                                            raw_query, modifiers)
                                
                                # Write query string to file. For tb_sr_rs transform it to a timestamp-based rdfstar query first
                                output_query_file_name = raw_query_name.split('.')[0] + "_q" + str(queryCounter) + "_v" + str(query_set_version) + ".txt"
                                with open(output_queries_dir_path / output_query_file_name, 'w') as output_file:
                                    if policy in ["tb_sr_rs"]:
                                        timestamped_output_query = timestamp_query(output_query, vers_ts)
                                        output_file.write(timestamped_output_query[0])
                                    else:
                                        output_file.write(output_query) 
                    # Only for tb_sr_rs policy
                    vers_ts = vers_ts + timedelta(seconds=1)
                vers_ts = init_version_timestamp           
                       

if __name__ == "__main__":
    main()
