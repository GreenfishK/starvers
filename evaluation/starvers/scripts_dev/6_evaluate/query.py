from SPARQLWrapper import SPARQLWrapper, Wrapper, POST, JSON
from rdflib import Graph
from rdflib.term import URIRef, Literal, BNode
import pandas as pd
from typing import Tuple
import tomli
from pathlib import Path
import os
import sys
import time
import multiprocessing
import shutil
import csv
import logging as logger
from datetime import datetime
from datetime import timedelta, timezone

############################################# Logging ###################################################################
logger.basicConfig(handlers=[logger.FileHandler(filename="/starvers_eval/output/logs/evaluate/query.txt", 
                                                  encoding='utf-8', mode='a+')],
                    format="%(asctime)s %(name)s:%(levelname)s:%(message)s", 
                    datefmt="%F %A %T", 
                    level=logger.INFO)

###################################### Parameters ######################################
# Bash arguments and directory paths
triple_store = sys.argv[1]
policy = sys.argv[2]
dataset = sys.argv[3]
port = sys.argv[4]
final_queries= "/starvers_eval/queries/final_queries"
result_sets_dir = "/starvers_eval/output/result_sets"

# Global configurations for the SPARQL engine
if triple_store == "jenatdb2" and policy == "tb_sr_rs" and dataset == "bearc":
    timeout = 1
else:
    timeout = None
engine = SPARQLWrapper(endpoint="dummy")
engine.timeout = timeout
engine.setReturnFormat(JSON)
engine.setOnlyConneg(True)
engine.setMethod(POST)

endpoints = {'graphdb': {'get': 'http://{hostname}:{port}/repositories/{repository_name}',
                        'post': 'http://{hostname}:{port}/repositories/{repository_name}/statements'},
            'jenatdb2': {'get': 'http://{hostname}:{port}/{repository_name}/sparql',
                        'post': 'http://{hostname}:{port}/{repository_name}/update'}}    
LOCAL_TIMEZONE = datetime.now(timezone.utc).astimezone().tzinfo                    
init_version_timestamp = datetime(2022,10,1,12,0,0,0,LOCAL_TIMEZONE)

# Parse the relative locations of the query sets, number of repositories and number of query set versions 
with open("/starvers_eval/configs/eval_setup.toml", mode="rb") as config_file:
    eval_setup = tomli.load(config_file)
query_sets = [policy + "/" + dataset + "/" + query_set for query_set in eval_setup['datasets'][dataset]['query_sets'].keys()]
# The query set versions are the same for every query set within a dataset. There is just some redundancy in the eval_setup.toml
# E.g. there are 58 versions for beara and ic_sr_ng for both - high and low query sets
first_query_set = next(iter(eval_setup['datasets'][dataset]['query_sets']))
query_set_versions = eval_setup['datasets'][dataset]['query_sets'][first_query_set]['policies'][policy]['versions']
repositories = eval_setup['datasets'][dataset]['repositories'][policy]

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


def _set_endpoints(dataset: str, policy: str, endpoints: dict, engine: SPARQLWrapper, repository_suffix: str = None):
    if repository_suffix:
        # For multi repository (mr) storage management policy
        repository_name = "{policy}_{dataset}_{repository_suffix}".format(policy=policy, dataset=dataset,
            repository_suffix=repository_suffix)
    else:
        # For single repository (sr) storage management policy
        repository_name = "{policy}_{dataset}".format(policy=policy, dataset=dataset)
    engine.endpoint = endpoints[triple_store]['get'].format(hostname="Starvers", port=port, repository_name=repository_name)
    engine.updateEndpoint = endpoints[triple_store]['post'].format(hostname="Starvers", port=port, repository_name=repository_name)

###################################### Dry run ########################################
_set_endpoints(dataset, policy, endpoints, engine)   
dry_run_query = "select ?s ?p ?o {?s ?p ?o .} limit 10"
engine.setQuery(dry_run_query)
try:
    logger.info(f"Execute simple SPARQL query against {engine.endpoint} to warm up the RDF store and prevent the initial hike during the evaluation.")
    dry_run_result = engine.query()
except Exception as e:
    logger.error(f"Dry run query failed against endpoint {engine.endpoint}. Exception: {e}")
    sys.exit(1)

###################################### Evaluation ######################################
logger.info(f"Evaluate {triple_store}, {policy}, {dataset} and query sets {query_sets} " +
f"with {query_set_versions} query set versions and {repositories} repositories on port: {port}")
df_data = []

for query_set in query_sets:
    for query_version in range(query_set_versions):
        query_set_version = final_queries + "/" + query_set  +  "/" + str(query_version)
        snapshot_ts = init_version_timestamp + timedelta(seconds=query_version)
        current_query_version = query_version

        logger.info(f"Query set version: {query_set_version}")
        for query_file_name in os.listdir(query_set_version):
            execution_time = 0
            result_set_creation_time = 0

            logger.info("Read query file {0} and pass it to the engine.".format(query_file_name))
            file = open(query_set_version + "/" + query_file_name, "r")
            query_text = file.read()
            engine.setQuery(query_text)
            file.close()

            if policy in ["ic_sr_ng", "tb_sr_ng", "tb_sr_rs"]:
                _set_endpoints(dataset, policy, endpoints, engine)   

                logger.info("Querying SPARQL endpoint {0} with query {1}". format(engine.endpoint, query_file_name))
                try:
                    start = time.time()
                    result = engine.query()
                    end = time.time()
                    execution_time = end - start

                    logger.info("Serializing results.")
                    result_set_dir = result_sets_dir + "/" + triple_store + "/" + policy + "_" + dataset + "/" + query_set.split('/')[2] + "/" + str(query_version)
                    Path(result_set_dir).mkdir(parents=True, exist_ok=True)
                    file = open(result_set_dir + "/" + query_file_name.split('.')[0] + ".csv", 'w')
                    write = csv.writer(file, delimiter=";")
                    write.writerows(to_list(result))
                except Exception as e:
                    logger.error(e)
                    logger.warning(f"The query execution {query_file_name} might have reached the timeout of {timeout} seconds. " +\
                    "The execution_time will be set to -1. The results will not be serialized.")
                    execution_time = -1

                new_row = [triple_store, dataset, policy, query_set.split('/')[2], query_version, snapshot_ts, query_file_name, execution_time, 0]
                df_data.append(new_row)
            
            elif policy == "cb_sr_ng":
                _set_endpoints(dataset, policy, endpoints, engine)   

                def build_snapshot(snapshot: Graph):
                    """
                    Build the snapshot at version :query_version by populating :snapshot with triples from the add-set 
                    to :snapshot and depopulating :snapshot by removing triples that match the ones in the del-set, consecutively.
                    """
                    def parse_triple(row) -> Tuple[object, object, object]:
                        # parse subject
                        if row['s']['type'] == "uri":
                            s = URIRef(row['s']['value'])
                        else:
                            s = BNode(row['s']['value'])

                        # parse predicate
                        p = URIRef(row['p']['value'])

                        # parse object
                        if row['o']['type']  == "uri":
                            o = URIRef(row['o']['value'])
                        elif row['o']['type'] == "blank":
                            o = BNode(row['o']['value'])
                        else:
                            value = row['o']["value"]
                            lang = row['o'].get("xml:lang", None)
                            datatype = row['o'].get("datatype", None)
                            o = Literal(value, lang=lang, datatype=datatype)
                        return (s, p, o)

                    for cs_version in range(query_version + 1):
                        add_sets_v = """ Select ?graph ?s ?p ?o WHERE {{
                                graph <http://starvers_eval/v{0}/added> 
                                {{
                                    ?s ?p ?o .
                                }}
                            }} """.format(str(cs_version).zfill(len(str(query_set_versions))))
                        engine.setQuery(add_sets_v)
                        add_set = engine.query().convert()
                        for r in add_set["results"]["bindings"]:                        
                            snapshot.add(parse_triple(r))

                        del_sets_v = """ Select ?graph ?s ?p ?o WHERE {{
                            graph <http://starvers_eval/v{0}/deleted> 
                            {{
                                ?s ?p ?o .
                            }}
                        }} """.format(str(cs_version).zfill(len(str(query_set_versions))))
                        engine.setQuery(del_sets_v)
                        del_set = engine.query().convert()
                        for r in del_set["results"]["bindings"]:                        
                            snapshot.remove(parse_triple(r))
                
                # Query all changesets from the triplestore until version :query_version 
                # ordered by change set versions
                if current_query_version == query_version:
                    logger.info("Build snapshot version {0} from endpoint {1}". format(str(query_version).zfill(len(str(query_set_versions))),
                                                           engine.endpoint))
                    start = time.time()
                    snapshot_g = Graph()
                    build_snapshot(snapshot_g)
                    end = time.time()
                    snapshot_creation_time = end - start
                    current_query_version = None

                logger.info("Querying snapshot (rdflib graph object) with query {0}". format(query_file_name))
                try:
                    start = time.time()
                    query_result = snapshot_g.query(query_text)
                    end = time.time()
                    execution_time = end - start

                    if query_text.startswith("# Exclude"):
                        logger.info("Dont serialize query results due to issue in rdflib's serializer with this query: {0}". format(query_file_name))
                    else:
                        logger.info("Serializing results.")
                        result_set_dir = result_sets_dir + "/" + triple_store + "/" + policy + "_" + dataset + "/" + query_set.split('/')[2] + "/" + str(query_version)
                        Path(result_set_dir).mkdir(parents=True, exist_ok=True)
                        query_result.serialize(result_set_dir + "/" + query_file_name.split('.')[0] + ".csv", format="csv")
                except Exception as e:
                    logger.warning(f"The query execution {query_file_name} reached the timeout of {timeout}s. " + \
                    "The execution_time will be set to -1. The results will not be serialized.")
                    execution_time = -1
                
                new_row = [triple_store, dataset, policy, query_set.split('/')[2], query_version, snapshot_ts, query_file_name, execution_time, snapshot_creation_time]
                df_data.append(new_row)

            elif policy == "ic_mr_tr":
                for repository in range(1, int(repositories)+1):
                    _set_endpoints(dataset, policy, endpoints, engine, str(repository))

                    logger.info("Querying SPARQL endpoint {0} with query {1}". format(engine.endpoint, query_file_name))
                    try:
                        start = time.time()
                        result = engine.query()
                        end = time.time()
                        execution_time = end - start
                        
                        logger.info("Serializing results.")
                        result_set_dir = result_sets_dir + "/" + triple_store + "/" + policy + "_" + dataset + "/" + query_set.split('/')[2] + "/" + str(repository)
                        Path(result_set_dir).mkdir(parents=True, exist_ok=True)
                        file = open(result_set_dir + "/" + query_file_name.split('.')[0], 'w')
                        write = csv.writer(file, delimiter=";")
                        write.writerows(to_list(result))
                    except Exception as e:
                        logger.warning(f"The query execution {query_file_name} reached the timeout of {timeout}s. " + \
                        "The execution_time will be set to -1. The results will not be serialized.")
                        execution_time = -1
                    snapshot_ts = init_version_timestamp + timedelta(seconds=repository-1)
                    
                    new_row = [triple_store, dataset, policy, query_set.split('/')[2], repository, snapshot_ts, query_file_name, execution_time, 0]
                    df_data.append(new_row)

            elif policy == "cb_mr_tr":
                logger.info("To be implemented")
                """manager = multiprocessing.Manager()
                for repository in range(0, int(repositories/2)):
                    list_result = []     
                    header = []
                    engine.setQuery("Select * where { ?s ?p ?o . }")
                    result_set_creation_time = 0
                    execution_time = 0
                    logger.info("Recreating snapshot: " + str(repository))

                    def query_add(execution_time_add: float, result_set_creation_time_add: float, cs_add: list):
                        engine.endpoint = endpoints[triple_store]['get'].format(hostname="Starvers", port=port, repository_name=repository_name_add)
                        engine.updateEndpoint = endpoints[triple_store]['post'].format(hostname="Starvers", port=port, repository_name=repository_name_add)
                        
                        logger.info("Querying SPARQL endpoint {0} with query {1}". format(engine.endpoint, query_file_name))
                        start = time.time()
                        result = engine.query()
                        end = time.time()
                        execution_time_add.value = end - start

                        start = time.time()
                        cs_add.extend(to_list(result))
                        end = time.time()
                        result_set_creation_time_add.value = end - start


                    def query_del(execution_time_del: float, result_set_creation_time_del: float, cs_del: list):
                        engine.endpoint = endpoints[triple_store]['get'].format(hostname="Starvers", port=port, repository_name=repository_name_del)
                        engine.updateEndpoint = endpoints[triple_store]['post'].format(hostname="Starvers", port=port, repository_name=repository_name_del)
                        
                        logger.info("Querying SPARQL endpoint {0} with query {1}". format(engine.endpoint, query_file_name))
                        start = time.time()
                        result = engine.query()
                        end = time.time()
                        execution_time_del.value = end - start

                        start = time.time()
                        cs_del.extend(to_list(result))
                        end = time.time()
                        result_set_creation_time_del.value = end - start

                    for cs in range(0, repository+1):
                        if cs == 0:
                            repository_name_add = "{policy}_{dataset}_ic1".format(policy=policy, dataset=dataset)
                            repository_name_del = "{policy}_{dataset}_empty".format(policy=policy, dataset=dataset)

                            # Query empty repository just to get header                 
                            engine.endpoint = endpoints[triple_store]['get'].format(hostname="Starvers", port=port, repository_name=repository_name_del)
                            result = engine.query()
                            header = to_list(result)[0:1]
                        else:
                            repository_name_add = "{policy}_{dataset}_add_{v}-{ve}".format(policy=policy, dataset=dataset, v=cs, ve=cs+1)
                            repository_name_del = "{policy}_{dataset}_del_{v}-{ve}".format(policy=policy, dataset=dataset, v=cs, ve=cs+1)

                        # Registering shared values between processes
                        execution_time_add = multiprocessing.Value("f", 0, lock=False)
                        execution_time_del = multiprocessing.Value("f", 0, lock=False)
                        result_set_creation_time_add = multiprocessing.Value("f", 0, lock=False)
                        result_set_creation_time_del = multiprocessing.Value("f", 0, lock=False)
                        cs_add = manager.list()
                        cs_del = manager.list()

                        # Creating and starting processes
                        p1 = multiprocessing.Process(target=query_add, args=[execution_time_add, result_set_creation_time_add, cs_add])
                        p2 = multiprocessing.Process(target=query_del, args=[execution_time_del, result_set_creation_time_del, cs_del])
                        p1.start()
                        p2.start()
                        p1.join()
                        p2.join()

                        start = time.time()
                        list_result.extend(list(cs_add))
                        cum_result = np.array(list_result)
                        cs_del_arr = np.array(list(cs_del))
                        a1_rows = cum_result.view([('', cum_result.dtype)] * cum_result.shape[1]) # if len(list_result)>1 else [[()]]
                        a2_rows = cs_del_arr.view([('', cs_del_arr.dtype)] * cs_del_arr.shape[1]) # if len(list(cs_del))>1 else [[()]]
                        list_result = np.setdiff1d(a1_rows, a2_rows).view(cum_result.dtype).reshape(-1, cum_result.shape[1]).tolist()
                        end = time.time()

                        execution_time = execution_time + execution_time_add.value + execution_time_del.value
                        result_set_creation_time = result_set_creation_time + result_set_creation_time_add.value + result_set_creation_time_del.value + (end - start)
                
                    # Create output directory and save result set
                    # TODO: format output
                    result_set_dir = result_sets_dir + "/" + triple_store + "/" + policy + "_" + dataset + "/" + query_set.split('/')[2] + "/" + str(repository)
                    Path(result_set_dir).mkdir(parents=True, exist_ok=True)
                    file = open(result_set_dir + "/" + query_file_name.split('.')[0] + ".csv", 'w')
                    write = csv.writer(file, delimiter=";")
                    write.writerows(header)
                    write.writerows(list_result)
                    file.close()

                    new_row = [triple_store, dataset, policy, query_set.split('/')[2], repository, query_file_name, execution_time, result_set_creation_time]
                    df_data.append(new_row)"""

logger.info("Writing performance measurements to disk ...")       

df = pd.DataFrame(data=df_data, 
                  columns=['triplestore', 'dataset', 'policy', 'query_set', 
                           'snapshot', 'snapshot_ts', 'query', 'execution_time', 'snapshot_creation_time'])

df.to_csv("/starvers_eval/output/measurements/time.csv", sep=";", index=False, mode='a', header=False)

