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

def split_prefixes_query(query: str):
    """
    Extract a contiguous block of PREFIX/BASE declarations from the *start* of the query.
    Returns (prefixes, query_without_prefixes).
    """
    # Matches one or more PREFIX/BASE lines at the very beginning (ignoring leading whitespace/comments)
    leading_ws_comments = r'(?:\s*#.*\n|\s*)*'
    prefix_line = r'(?:BASE\s+<[^>]+>|PREFIX\s+[A-Za-z_][\w-]*:\s*<[^>]+>)\s*'
    pat = re.compile(rf'^{leading_ws_comments}((?:{prefix_line})+)', re.IGNORECASE)
    m = pat.search(query)
    if not m:
        return "", query
    prefixes = m.group(1).strip()
    rest = query[m.end():].lstrip()
    return prefixes, rest

def split_solution_modifiers_query(query: str):
    """
    Extract trailing ORDER BY / LIMIT / OFFSET block (in any order) at the *end* of the query.
    Returns (modifiers, query_without_modifiers).
    """
    # Grab anything from the first ORDER/LIMIT/OFFSET that appears at the tail to the end.
    # This assumes solution modifiers are at the end (standard practice).
    pat = re.compile(
        r'(?is)'                       # DOTALL + case-insensitive
        r'(?:\s*#.*\n|\s*)*'           # trailing whitespace/comments
        r'('
        r'(?:ORDER\s+BY\b.*?(?=(?:LIMIT|OFFSET|$)))?'  # ORDER BY… up to LIMIT/OFFSET/end
        r'(?:\s*LIMIT\b.*?(?=(?:OFFSET|$)))?'          # LIMIT…
        r'(?:\s*OFFSET\b.*)?'                          # OFFSET…
        r')\s*\Z'
    )
    m = pat.search(query)
    if not m:
        return "", query
    modifiers = m.group(1).strip()
    if not modifiers:
        return "", query
    core = query[:m.start(1)].rstrip()
    return modifiers, core

def main():
    ############################################# Parameters ################################################################
    raw_queries_base="/ostrich_eval/queries/raw_queries/"
    output_queries_base="/ostrich_eval/computed_queries/"
    template_path="/ostrich_eval/scripts/5_construct_queries/templates/ostrich_template.txt"

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
    datasets = sys.argv[1].split(" ")
    
    construct_basic_ostrich_queries(raw_queries_base, output_queries_base, "po")
    construct_basic_ostrich_queries(raw_queries_base, output_queries_base, "p")

    for dataset in datasets:
        if dataset in ["bearb_day", "bearb_hour"]:
            context = "bearb"
            query_sets = ["join", "lookup"]
            if (dataset == "bearb_day"):
                versions = 89
            else:
                versions = 1299
        else:
            context = "bearc"
            query_sets = ["complex"]
            versions = 33

        for query_set in query_sets:
            print(f"Constructing queries for dataset:'{dataset}' and query_set:'{query_set}'")
            raw_queries_dir_path = raw_queries_base + context + "/" + query_set


            for version in range(versions):
                output_queries_dir_path = Path(output_queries_base + dataset + "/" + query_set + "/" + str(version))
                if output_queries_dir_path.exists():
                    shutil.rmtree(output_queries_dir_path)
                
                logging.info("Create directory {0}".format(output_queries_dir_path))
                output_queries_dir_path.mkdir(parents=True, exist_ok=True)
                
                logging.info("Create queries and save to {0}".format(output_queries_dir_path))
                for k, raw_query_name in enumerate(os.listdir(raw_queries_dir_path)):
                    if not os.path.isfile(raw_queries_dir_path + "/" + raw_query_name):
                        logging.error("No such file: " + raw_queries_dir_path + "/" + raw_query_name)
                    
                    with open(raw_queries_dir_path + "/" + raw_query_name, 'r') as raw_query_file:
                        if query_set == "lookup":
                            raw_queries = raw_query_file.readlines()
                        else:
                            raw_queries = [raw_query_file.read()]
                        # Iterate over file that holds one or more raw queries.
                        for i, raw_query in enumerate(raw_queries):
                            prefixes, q_wo_prefix = split_prefixes_query(raw_query)
                            modifiers, q_core = split_solution_modifiers_query(q_wo_prefix)

                            queryCounter =  i if query_set == "lookup" else k
    
                            with open(template_path, 'r') as templateFile:
                                template = templateFile.read()
                                output_query = template.format(prefixes, str(version), q_core.strip(), modifiers)
                            
                            # Write query string to file. For tb_sr_rs transform it to a timestamp-based rdfstar query first
                            output_query_file_name = raw_query_name.split('.')[0] + "_q" + str(queryCounter) + "_v" + str(version) + ".txt"
                            with open(output_queries_dir_path / output_query_file_name, 'w') as output_file:
                                output_file.write(output_query) 
        


if __name__ == "__main__":
    main()
