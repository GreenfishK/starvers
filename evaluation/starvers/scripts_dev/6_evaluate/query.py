from SPARQLWrapper import SPARQLWrapper, Wrapper, GET, POST, POSTDIRECTLY, JSON
from rdflib import Graph
from rdflib.term import URIRef, Literal, BNode
import pandas as pd
from typing import Tuple
import tomli
from pathlib import Path
import os
import sys
import time
import csv
import logging as logger
from datetime import datetime
from datetime import timedelta, timezone

##########################################################
# Logging
##########################################################
logger.basicConfig(handlers=[logger.FileHandler(filename="/starvers_eval/output/logs/evaluate/query.txt", 
                                                  encoding='utf-8', mode='a+')],
                    format="%(asctime)s %(name)s:%(levelname)s:%(message)s", 
                    datefmt="%F %A %T", 
                    level=logger.INFO)

##########################################################
# Parameters 
##########################################################
# Bash arguments and directory paths
triple_store = sys.argv[1]
logger.info(f"Parameter: triplestore={triple_store}")

policy = sys.argv[2]
logger.info(f"Parameter: policy={policy}")

dataset = sys.argv[3]
logger.info(f"Parameter: dataset={dataset}")

CONFIG_PATH = "/starvers_eval/configs/eval_setup.toml"

final_queries= "/starvers_eval/queries/final_queries"
result_sets_dir = "/starvers_eval/output/result_sets"
time_measurement_file = "/starvers_eval/output/measurements/time.csv"

# Global configurations for the SPARQL engine
timeout = 30

engine = SPARQLWrapper(endpoint="dummy")
engine.timeout = timeout
engine.setReturnFormat(JSON)
engine.setOnlyConneg(True)
engine.setMethod(POST)
engine.addCustomHttpHeader("Accept", "application/sparql-results+json")

LOCAL_TIMEZONE = datetime.now(timezone.utc).astimezone().tzinfo                    
init_version_timestamp = datetime(2022,10,1,12,0,0,0,LOCAL_TIMEZONE)

# ##########################################################
# Helper functions
# ##########################################################

def parse_results(result: Wrapper.QueryResult) -> list:
    """

    :param result:
    :return: Dataframe
    """
    
    results = result.convert()

    def format_value(res_value):
        value = res_value["value"]
        lang = res_value.get("xml:lang", None)
        datatype = res_value.get("datatype", None)
        if lang is not None:
            value += "@" + lang
        if datatype is not None:
            value += " [" + datatype + "]"
        return value

    header = []
    values = []

    if not "head" in results or not "vars" in results["head"]:
        return header

    if not "results" in results or not "bindings" in results["results"]:
        return values

    for var in results["head"]["vars"]:
        header.append(var)

    for r in results["results"]["bindings"]:
        row = []
        for col in results["head"]["vars"]:
            if col in r:
                result_value = format_value(r[col])
            else:
                result_value = None
            row.append(result_value)
        values.append(row)
    
    return [header] + values


def _set_endpoints(triple_store: str, dataset: str, policy: str, engine: SPARQLWrapper):
    with open(CONFIG_PATH, "rb") as f:
        CONFIG = tomli.load(f)

    engine.endpoint = CONFIG["rdf_stores"][triple_store]["get"].format(repo=f"{policy}_{dataset}")
    engine.updateEndpoint = CONFIG["rdf_stores"][triple_store]["post"].format(repo=f"{policy}_{dataset}")


##########################################################
# Pipeline
##########################################################
# Parse the relative locations of the query sets, number of repositories and number of query set versions 
with open(CONFIG_PATH, mode="rb") as config_file:
    eval_setup = tomli.load(config_file)
query_sets = [policy + "/" + dataset + "/" + query_set for query_set in eval_setup['datasets'][dataset]['query_sets'].keys()]
first_query_set = next(iter(eval_setup['datasets'][dataset]['query_sets']))
query_set_versions = eval_setup['datasets'][dataset]['query_sets'][first_query_set]['policies'][policy]['versions']
repositories = eval_setup['datasets'][dataset]['repositories'][policy]

# Set endpoints
_set_endpoints(triple_store, dataset, policy, engine)   

# Dry run and configuration of the SPARQL engine
dry_run_query = eval_setup["rdf_stores"][triple_store]["dry_run_query"]
engine.setQuery(dry_run_query)
try:
    logger.info(f"Execute simple SPARQL query against {engine.endpoint} to warm up the RDF store and prevent the initial hike during the evaluation.")
    logger.info(f"{dry_run_query}")
    dry_run_result = engine.query()
except Exception as e:
    logger.error(f"Dry run query failed against endpoint {engine.endpoint}. Exception: {e}")
    sys.exit(1)

# Evaluation 
logger.info(f"Evaluate {triple_store}, {policy}, {dataset} and query sets {query_sets} " +
f"with {query_set_versions} query set versions and {repositories} repositories on endpoint: {engine.endpoint}")
df_data = []

# Run queries
for query_set in query_sets:
    for query_version in range(query_set_versions):
        query_set_version = final_queries + "/" + query_set  +  "/" + str(query_version)
        snapshot_ts = init_version_timestamp + timedelta(seconds=query_version)
        current_query_version = query_version

        logger.info(f"Query set version: {query_set_version}")
        for query_file_name in os.listdir(query_set_version):
            execution_time = 0
            result_set_creation_time = 0
            yn_timeout = 0

            logger.info("Read query file {0} and pass it to the engine.".format(query_file_name))
            file = open(query_set_version + "/" + query_file_name, "r")
            query_text = file.read()
            engine.setQuery(query_text)
            file.close()

            _set_endpoints(triple_store, dataset, policy, engine)   
            try:
                logger.info("Querying SPARQL endpoint {0} with query {1}". format(engine.endpoint, query_file_name))
                start = time.time()
                result = engine.query()
                end = time.time()
                execution_time = end - start

                logger.info("Serializing results.")
                result_set_dir = result_sets_dir + "/" + triple_store + "/" + policy + "_" + dataset + "/" + query_set.split('/')[2] + "/" + str(query_version)
                Path(result_set_dir).mkdir(parents=True, exist_ok=True)
                file = open(result_set_dir + "/" + query_file_name.split('.')[0] + ".csv", 'w')
                write = csv.writer(file, delimiter=";")
                write.writerows(parse_results(result))
            except Exception as e:
                logger.error(e)
                logger.warning(f"The query execution {query_file_name} might have reached the timeout of {timeout} seconds. " +\
                "The execution_time will be set to -1. The results will not be serialized.")
                execution_time = -1
                yn_timeout = 1

            new_row = [triple_store, dataset, policy, query_set.split('/')[2], 
                       query_version, snapshot_ts, query_file_name, execution_time, 0, yn_timeout]
            df_data.append(new_row)

# Save measurements
logger.info("Writing performance measurements to disk ...")       

df = pd.DataFrame(data=df_data, 
                  columns=['triplestore', 'dataset', 'policy', 'query_set', 
                           'snapshot', 'snapshot_ts', 'query', 'execution_time',
                            'snapshot_creation_time', 'yn_timeout'])

df.to_csv(time_measurement_file, sep=";", index=False, mode='a', header=False)

