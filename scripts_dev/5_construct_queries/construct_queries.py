from genericpath import isfile
from starvers.starvers import timestamp_query, split_prefixes_query
from pathlib import Path
import os
import sys
from datetime import datetime, timezone, timedelta
import shutil
import logging
import re
import tomli


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
    # Directory contained in docker image
    raw_queries_base="/starvers_eval/queries/raw_queries/"
    # Mounted directory
    output_queries_base="/starvers_eval/queries/final_queries/"

    policies_cmd = sys.argv[1]
    policies = policies_cmd.split(" ")
    queries={
        "ic_mr_tr":{
            "beara/high": {'output_dirs':{"beara/high": 1}, 'template': "ictr/ts"},
            "beara/low": {'output_dirs':{"beara/low": 1}, 'template': "ictr/ts"},
            "bearb/join": {'output_dirs':{"bearb_day/join": 1, "bearb_hour/join": 1}, 'template': "ic/bgp"},
            "bearb/lookup": {'output_dirs':{"bearb_day/lookup": 1, "bearb_hour/lookup": 1}, 'template': "ictr/ts"},
            "bearc": {'output_dirs':{ "bearc/complex": 1}, 'template': "ic/sparql"},
        },
        "cb_mr_tr":{
            "beara/high": {'output_dirs':{"beara/high": 1}, 'template': "ictr/ts"},
            "beara/low": {'output_dirs':{"beara/low": 1}, 'template': "ictr/ts"},
            "bearb/join": {'output_dirs':{"bearb_day/join": 1, "bearb_hour/join": 1}, 'template': "ic/bgp"},
            "bearb/lookup": {'output_dirs':{"bearb_day/lookup": 1, "bearb_hour/lookup": 1}, 'template': "ictr/ts"},
            "bearc": {'output_dirs':{"bearc/complex": 1}, 'template': "ic/sparql"},
        },
        "ic_sr_ng":{
            "beara/high": {'output_dirs':{"beara/high": 58}, 'template': "icng/ts"},
            "beara/low": {'output_dirs':{"beara/low": 58}, 'template': "icng/ts"},
            "bearb/join": {'output_dirs':{"bearb_day/join": 89, "bearb_hour/join": 1299}, 'template': "icng/bgp"},
            "bearb/lookup": {'output_dirs':{"bearb_day/lookup": 89, "bearb_hour/lookup": 1299}, 'template': "icng/ts"},
            "bearc": {'output_dirs':{"bearc/complex": 33}, 'template': "icng/sparql"},
        },
        "cb_sr_ng":{
            "beara/high": {'output_dirs':{"beara/high": 58}, 'template': "ictr/ts"},
            "beara/low": {'output_dirs':{"beara/low": 58}, 'template': "ictr/ts"},
            "bearb/join": {'output_dirs':{"bearb_day/join": 89, "bearb_hour/join": 1299}, 'template': "ictr/bgp"},
            "bearb/lookup": {'output_dirs':{"bearb_day/lookup": 89, "bearb_hour/lookup": 1299}, 'template': "ictr/ts"},
            "bearc": {'output_dirs':{"bearc/complex": 33}, 'template': "ictr/sparql"},
        },
        "tb_sr_ng":{
            "beara/high": {'output_dirs':{"beara/high": 58}, 'template': "tbng/ts"},
            "beara/low": {'output_dirs':{"beara/low": 58}, 'template': "tbng/ts"},
            "bearb/join": {'output_dirs':{"bearb_day/join": 89, "bearb_hour/join": 1299}, 'template': "tbng/bgp"},
            "bearb/lookup": {'output_dirs':{"bearb_day/lookup": 89, "bearb_hour/lookup": 1299}, 'template': "tbng/ts"},
            "bearc": {'output_dirs':{"bearc/complex": 33}, 'template': "tbng/sparql"},
        },
        "tb_sr_rs":{
            "beara/high": {'output_dirs':{"beara/high": 58}, 'template': "ictr/ts"},
            "beara/low": {'output_dirs':{"beara/low": 58}, 'template': "ictr/ts"},
            "bearb/join": {'output_dirs':{"bearb_day/join": 89, "bearb_hour/join": 1299}, 'template': "ictr/bgp"},
            "bearb/lookup": {'output_dirs':{"bearb_day/lookup": 89, "bearb_hour/lookup": 1299}, 'template': "ictr/ts"},
            "bearc": {'output_dirs':{"bearc/complex": 33}, 'template': "ictr/sparql"},
        }
    }
    # TODO: Use eval_setup.toml as the golden source instead of the "queries" dict.
    #with open("/starvers_eval/configs/eval_setup.toml", mode="rb") as config_file:
    #    queries_toml = tomli.load(config_file)
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

    # Create directories from toml config file
    # TODO: replace this out commented code with the code below once it is ready.
    #
    #for dataset, dataset_infos in queries_toml.items():
    #    query_sets = dataset_infos['query_sets'].items()
    #    for query_set_name, query_set in query_sets:
    #        policy_infos = query_set['policies'].items()
    #        for policy, infos in policy_infos:
    #            for query_set_version in range(infos['versions']:
    #                output_queries_dir_path = Path(output_queries_base + policy + "/" + dataset + "/" + query_set_name + "/" + str(query_set_version))
    #                if output_queries_dir_path.exists():
    #                    shutil.rmtree(output_queries_dir_path)
    #                logging.info("Create directory {0}".format(output_queries_dir_path))
    #                output_queries_dir_path.mkdir(parents=True, exist_ok=True)

    #                logging.info("Create queries in {0} for {1}, {2}, {3}".format(output_queries_dir_path, policy, dataset, query_set_name))
    #                raw_queries_dir_path = raw_queries_base + dataset + "/" + query_set_name
    #                for k, queriesFile in enumerate(os.listdir(raw_queries_base + dataset + "/" + query_set_name)):
    #                    if not os.path.isfile(raw_queries_dir_path + "/" + queriesFile):
    #                        logging.error("No such file: " + raw_queries_dir_path + "/" + queriesFile)
    #                    with open(raw_queries_dir_path + "/" + queriesFile, 'r') as file:
    #                        template_relative_path = infos['template']
    #                        if template_relative_path.split('/')[1] == 'ts':
    #                            raw_queries = file.readlines()
    #                        else:
    #                            raw_queries = [file.read()]    
    #                        # Iterate over file that holds multiple raw queries.
    #                        for i, raw_query in enumerate(raw_queries):
    #                            prefixes, raw_query = split_prefixes_query(raw_query)
    #                            modifiers, raw_query = split_solution_modifiers_query(raw_query)
    #                            queryCounter =  i if template_relative_path.split('/')[1] == 'ts' else k
    #                            output_query = ""    
    #                            # Read template and create query string
    #                            with open(os.path.join(sys.path[0]) +"/templates/" + template_relative_path + ".txt", 'r') as templateFile:
    #                                template = templateFile.read()
    #                                if policy == "cb_sr_ng":
    #                                    max_version_digits = len(str(query_versions))
    #                                    output_query = template.format(prefixes, str(query_version).zfill(max_version_digits),
    #                                        raw_query, modifiers)
    #                                else:
    #                                    output_query = template.format(prefixes, str(query_version),
    #                                        raw_query, modifiers)
    #                            # Write query string to file. For tb_sr_rs transform it to a timestamp-based rdfstar query first
    #                            with open(output_queries_base + policy + "/" + dataset + "/" + query_set_name + "/" + str(query_version) + "/" + queriesFile.split('.')[0] + "_q" + str(queryCounter) + "_v" + str(query_version) + ".txt", 'w') as output_file:
    #                                if policy in ["tb_sr_rs"]:
    #                                    timestamped_output_query = timestamp_query(output_query, vers_ts)
    #                                    output_file.write(timestamped_output_query[0])
    #                                    vers_ts = vers_ts + timedelta(seconds=1)
    #                                else:
    #                                    output_file.write(output_query)            
    #                   
    #                
    #

    for policy in policies:
        for querySet in queries[policy].keys():
            # create directories
            for output_dir, query_versions in queries[policy][querySet]['output_dirs'].items():
                logging.info("For {0}, {1} create {2} directories.".format(policy, querySet, query_versions))
                for query_version in range(query_versions):
                    query_dir = Path(output_queries_base + policy + "/" + output_dir + "/" + str(query_version))
                    if query_dir.exists():
                        shutil.rmtree(query_dir)
                    query_dir.mkdir(parents=True, exist_ok=True)

            # Create query files
            logging.info("Create queries for {0}, {1}".format(policy, querySet))
            pathToQueries = raw_queries_base + querySet
            for k, queriesFile in enumerate(os.listdir(raw_queries_base + querySet)):
                if not os.path.isfile(pathToQueries + "/" + queriesFile):
                    logging.warning("No such file: " + pathToQueries + "/" + queriesFile)
                    continue

                with open(pathToQueries + "/" + queriesFile, 'r') as file:
                    relativeTempLoc = queries[policy][querySet]['template']
                    if relativeTempLoc.split('/')[1] == 'ts':
                        raw_queries = file.readlines()
                    else:
                        raw_queries = [file.read()]
                    for i, raw_query in enumerate(raw_queries):
                        prefixes, raw_query = split_prefixes_query(raw_query)
                        modifiers, raw_query = split_solution_modifiers_query(raw_query)
                        queryCounter =  i if relativeTempLoc.split('/')[1] == 'ts' else k
                        output_query = ""
                        for output_dir, query_versions in queries[policy][querySet]['output_dirs'].items():
                            for query_version in range(query_versions):
                                with open(os.path.join(sys.path[0]) +"/templates/" + relativeTempLoc + ".txt", 'r') as templateFile:
                                    template = templateFile.read()
                                    if policy == "cb_sr_ng":
                                        max_version_digits = len(str(query_versions))
                                        output_query = template.format(prefixes, str(query_version).zfill(max_version_digits),
                                            raw_query, modifiers)
                                    else:
                                        output_query = template.format(prefixes, str(query_version),
                                            raw_query, modifiers)
                                with open(output_queries_base + policy + "/" + output_dir + "/" + str(query_version) + "/" + queriesFile.split('.')[0] + "_q" + str(queryCounter) + "_v" + str(query_version) + ".txt", 'w') as output_file:
                                    if policy in ["tb_sr_rs"]:
                                        timestamped_output_query = timestamp_query(output_query, vers_ts)
                                        output_file.write(timestamped_output_query[0])
                                        vers_ts = vers_ts + timedelta(seconds=1)
                                    else:
                                        output_file.write(output_query)
                            vers_ts = init_version_timestamp



if __name__ == "__main__":
    main()