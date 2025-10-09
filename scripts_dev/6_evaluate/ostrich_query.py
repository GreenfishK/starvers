from SPARQLWrapper import SPARQLWrapper, Wrapper, POST, JSON
import pandas as pd
import tomli

from pathlib import Path
import os
import sys
import time
import csv
import logging as logger
from datetime import datetime
from datetime import timezone

############################################# Logging ###################################################################
logger.basicConfig(handlers=[logger.FileHandler(filename="/ostrich_eval/output/logs/evaluate/query.txt", 
                                                  encoding='utf-8', mode='a+')],
                    format="%(asctime)s %(name)s:%(levelname)s:%(message)s", 
                    datefmt="%F %A %T", 
                    level=logger.INFO)

###################################### Parameters ######################################
# Bash arguments and directory paths
final_queries= "/ostrich_eval/queries"
result_sets_dir = "/ostrich_eval/output/result_sets"

engine = SPARQLWrapper(endpoint="dummy")
engine.timeout = None
engine.setReturnFormat(JSON)
engine.setOnlyConneg(True)
engine.setMethod(POST)
engine.addCustomHttpHeader("Accept", "application/sparql-results+json")
engine.addCustomHttpHeader("Content-Type", "application/sparql-query")

endpoint_url = os.environ.get("ENDPOINT_URL", "http://ostrich-endpoint:42564/sparql")

LOCAL_TIMEZONE = datetime.now(timezone.utc).astimezone().tzinfo                    
init_version_timestamp = datetime(2022,10,1,12,0,0,0,LOCAL_TIMEZONE)

# Parse the relative locations of the query sets, number of repositories and number of query set versions 
with open("/ostrich_eval/configs", mode="rb") as config_file:
    eval_setup = tomli.load(config_file)

###################################### Helper functions ######################################
def to_list(result: Wrapper.QueryResult) -> list:
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


def _set_endpoint(endpoint: dict, engine: SPARQLWrapper, dataset):
    engine.endpoint = endpoint['get'].format(hostname=f"ostrich-{dataset}", port="3000")
    engine.updateEndpoint = endpoint['post'].format(hostname=f"ostrich-{dataset}", port="3000")


###################################### Evaluation ######################################
datasets = sys.argv[1].split(" ")


for dataset in datasets:
    if dataset in ["bearb_day", "bearb_hour"]:
        query_sets = ["join", "lookup"]
        query_set_versions = 89 if dataset == "bearb_day" else 1299
    else:
        query_sets = ["complex"]
        query_set_versions = 33


    logger.info(f"Evaluate {dataset} and query sets {query_sets}")
    engine.endpoint = endpoint_url
    engine.updateEndpoint = endpoint_url
    engine.setMethod('GET')
    
    logger.info(f"Execute simple SPARQL query for dataset {dataset} to warm up the RDF store and prevent the initial hike during the evaluation.")
    dry_run_query = "SELECT * WHERE { GRAPH <version:0> { ?s ?p ?o . } } LIMIT 10"
    engine.setQuery(dry_run_query)
    dry_run_result = engine.query()

    
    failed = False
    df = pd.DataFrame(columns=['dataset', 'query_set', 'snapshot', 'query', 'execution_time'])
    for query_set in query_sets:
        failed = False
        for query_version in range(query_set_versions):
            if failed:
                break
            query_set_version = final_queries + "/" + dataset + "/" + query_set  +  "/" + str(query_version)
            current_query_version = query_version

            logger.info(f"Query set version: {query_set_version}")
            for query_file_name in os.listdir(query_set_version):
                print(f"Querying query:{query_file_name} for query_set:{query_set} in dataset:{dataset}")
                execution_time = 0
                result_set_creation_time = 0

                logger.info("Read query file {0} and pass it to the engine.".format(query_file_name))
                file = open(query_set_version + "/" + query_file_name, "r")
                query_text = file.read()
                engine.setQuery(query_text)
                file.close()

                logger.info("Querying SPARQL endpoint {0} with query {1}". format(engine.endpoint, query_file_name))
                try:
                    start = time.time()
                    result = engine.query()
                    end = time.time()
                    execution_time = end - start
                    time.sleep(0.05)

                    logger.info("Serializing results.")
                    result_set_dir = result_sets_dir + "/" + dataset + "/" + query_set + "/" + str(query_version)
                    Path(result_set_dir).mkdir(parents=True, exist_ok=True)
                    file = open(result_set_dir + "/" + query_file_name.split('.')[0] + ".csv", 'w')
                    write = csv.writer(file, delimiter=";")
                    write.writerows(to_list(result))
                except Exception as e:
                    logger.error(e)
                    logger.warning(f"Query has failed"
                                    "The execution_time will be set to -1. The results will not be serialized.")
                    execution_time = -1
                    failed = True

                df = df.append(pd.Series([dataset, query_set, query_version, query_file_name, execution_time], index=df.columns), ignore_index=True)
        

    
                        
logger.info("Writing performance measurements to disk ...")            
df.to_csv("/ostrich_eval/output/measurements/time.csv", sep=";", index=False, mode='a', header=True)

