from genericpath import isfile
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

def transform_query_line_for_basic_ostrich_query(line: str) -> str:
    # remove trailing dot
    line = re.sub(r'\s*\.\s*$', '', line.rstrip())

    # collapse variables like ?s, ?o to ?
    line = re.sub(r'\?[A-Za-z_][\w-]*', '?', line)
    return line

def construct_basic_ostrich_queries(raw_queries_base: str, output_queries_base: str, type: str):
    dst_dir = f"{output_queries_base}basic/"

    if not os.path.exists(dst_dir):
        logging.info("Create directory: " + dst_dir)
        os.makedirs(dst_dir)

    dst_lookup_queries_path = f"{dst_dir}lookup_queries_{type}.txt"
    src_lookup_queries_path = f"{raw_queries_base}bearb/lookup/lookup_queries_{type}.txt"

    with open(dst_lookup_queries_path, "w") as dst, open(src_lookup_queries_path, "r") as src:
        for line in (src):
            dst.write(transform_query_line_for_basic_ostrich_query(line) + "\n")


def main():
    ############################################# Parameters ################################################################
    raw_queries_base="/ostrich_eval/queries/raw_queries/"
    output_queries_base="/ostrich_eval/computed_queries/"

    with open("/ostrich_eval/configs", mode="rb") as config_file:
        eval_setup = tomli.load(config_file)
    LOCAL_TIMEZONE = datetime.now(timezone.utc).astimezone().tzinfo
    init_version_timestamp = datetime(2022,10,1,12,0,0,0,LOCAL_TIMEZONE)
    vers_ts = init_version_timestamp

    ############################################# Logging ###############################################################
    if not os.path.exists('/ostrich_eval/output/logs/construct_queries'):
        os.makedirs('/ostrich_eval/output/logs/construct_queries')
    with open('/ostrich_eval/output/logs/construct_queries/construct_queries.txt', "w") as log_file:
        log_file.write("")
    logging.basicConfig(handlers=[logging.FileHandler(filename="/ostrich_eval/output/logs/construct_queries/construct_queries.txt", 
                                                    encoding='utf-8', mode='a+')],
                        format="%(asctime)s %(name)s:%(levelname)s:%(message)s", 
                        datefmt="%F %A %T", 
                        level=logging.INFO)

    ################################################## Generate queries ################################################# 

    construct_basic_ostrich_queries(raw_queries_base, output_queries_base, "po")
    construct_basic_ostrich_queries(raw_queries_base, output_queries_base, "p")
    

if __name__ == "__main__":
    main()
