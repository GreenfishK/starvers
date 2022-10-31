from SPARQLWrapper import SPARQLWrapper, Wrapper, POST, DIGEST, GET, JSON
from pathlib import Path
import os
import pandas as pd
import sys
import time
import multiprocessing
from rdflib import Graph
import shutil
import csv
import numpy as np
import logging

logger = logging.getLogger("evaluate")
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s [%(name)-12s] %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

###################################### Parameters ######################################
final_queries= "/starvers_eval/queries/final_queries"
result_sets_dir = "/starvers_eval/output/result_sets"
ds_queries_map={'ic': {
                    'beara': {'query_versions': 1, 'repositories': 58, 'query_sets': ['ic/queries_beara/high',
                                                            'ic/queries_beara/low']}, 
                    'bearb_day': {'query_versions': 1,'repositories': 89, 'query_sets': ['ic/queries_bearb/lookup',
                                                                'ic/queries_bearb/join']}, 
                    'bearb_hour': {'query_versions': 1, 'repositories': 1299, 'query_sets': ['ic/queries_bearb/lookup',
                                                                'ic/queries_bearb/join']},
                    'bearc': {'query_versions': 1, 'repositories': 32, 'query_sets': ['ic/queries_bearc/complex']}       
                },    
                'cb': {
                    'beara': {'query_versions': 1, 'repositories': 116, 'query_sets': ['cb/queries_beara/high',
                                                            'cb/queries_beara/low']}, 
                    'bearb_day': {'query_versions': 1, 'repositories': 178, 'query_sets': ['cb/queries_bearb/lookup',
                                                                'cb/queries_bearb/join']}, 
                    'bearb_hour': {'query_versions': 1, 'repositories': 2598, 'query_sets': ['cb/queries_bearb/lookup',
                                                                'cb/queries_bearb/join']},
                    'bearc': {'query_versions': 1, 'repositories': 64, 'query_sets': ['cb/queries_bearc/complex']}       
                },        
                'tb':{
                    'beara': {'query_versions': 58, 'repositories': 1, 'query_sets': ['tb/queries_beara/high',
                                                            'tb/queries_beara/low']}, 
                    'bearb_day': {'query_versions': 89, 'repositories': 1, 'query_sets': ['tb/queries_bearb_day/lookup',
                                                                'tb/queries_bearb_day/join']}, 
                    'bearb_hour': {'query_versions': 1299, 'repositories': 1, 'query_sets': ['tb/queries_bearb_hour/lookup',
                                                                'tb/queries_bearb_hour/join']},
                    'bearc': {'query_versions': 32, 'repositories': 1, 'query_sets': ['tb/queries_bearc/complex']}       
                },
                'tbsf': {
                    'beara': {'query_versions': 58, 'repositories': 1,'query_sets': ['tbsf/queries_beara/high',
                                                            'tbsf/queries_beara/low']}, 
                    'bearb_day': {'query_versions': 89, 'repositories': 1, 'query_sets': ['tbsf/queries_bearb_day/lookup',
                                                                'tbsf/queries_bearb_day/join']}, 
                    'bearb_hour': {'query_versions': 1299, 'repositories': 1, 'query_sets': ['tbsf/queries_bearb_hour/lookup',
                                                                'tbsf/queries_bearb_hour/join']},
                    'bearc': {'query_versions': 32, 'repositories': 1, 'query_sets': ['tbsf/queries_bearc/complex']}       
                },
                'tbsh': {
                    'beara': {'query_versions': 58, 'repositories': 1, 'query_sets': ['tbsh/queries_beara/high',
                                                            'tbsh/queries_beara/low']}, 
                    'bearb_day': {'query_versions': 89, 'repositories': 1, 'query_sets': ['tbsh/queries_bearb_day/lookup',
                                                                'tbsh/queries_bearb_day/join']}, 
                    'bearb_hour': {'query_versions': 1299, 'repositories': 1, 'query_sets': ['tbsh/queries_bearb_day/lookup',
                                                                'tbsh/queries_bearb_day/join']},
                    'bearc': {'query_versions': 32, 'repositories': 1, 'query_sets': ['tbsh/queries_bearc/complex']}       
                }
}

endpoints = {'graphdb': {'get': 'http://{hostname}:{port}/repositories/{repository_name}',
                        'post': 'http://{hostname}:{port}/repositories/{repository_name}/statements'},
            'jenatdb2': {'get': 'http://{hostname}:{port}/{repository_name}/sparql',
                        'post': 'http://{hostname}:{port}/{repository_name}/update'}}                        

###################################### Evaluation ######################################
# header: tripleStore,snapshot,min,mean,max,stddev,count,sum
# aggregation on tripleStore and version level



def to_list(result: Wrapper.QueryResult) -> list:
    """

    :param result:
    :return: Dataframe
    """
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)

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
    
def query_dataset(triple_store: str, policy: str, ds: str, port: int):
    df = pd.DataFrame(columns=['triplestore', 'dataset', 'policy', 'query_set', 'snapshot', 'query', 'execution_time', 'result_set_creation_time'])

    query_sets = ds_queries_map[policy][ds]['query_sets']
    query_versions = ds_queries_map[policy][ds]['query_versions']
    repositories = ds_queries_map[policy][ds]['repositories']

    for query_set in query_sets:
        for query_version in range(query_versions):
            query_set_version = final_queries + "/" + query_set  +  "/" + str(query_version)
            for query_file_name in os.listdir(query_set_version):
                print("Processing query {0}".format(query_file_name))
                execution_time = 0
                result_set_creation_time = 0

                if policy in ["tbsh", "tbsf", "tb"]:
                    repository_name = "{policy}_{dataset}".format(policy=policy, dataset=ds)
                    getEndpoint = endpoints[triple_store]['get'].format(hostname="Starvers", port=port, repository_name=repository_name)
                    postEndpoint = endpoints[triple_store]['post'].format(hostname="Starvers", port=port, repository_name=repository_name)
                    engine = SPARQLWrapper(endpoint=getEndpoint, updateEndpoint=postEndpoint)

                    with open(query_set_version + "/" + query_file_name, "r") as file:
                        query_text = file.read()
                        file.close()
                    engine.setQuery(query_text)
                    engine.setReturnFormat(JSON)
                    engine.setOnlyConneg(True)
                    print("Querying SPARQL endpoint {0} with query {1}". format(getEndpoint, query_file_name))
                    start = time.time()
                    result = engine.query()
                    end = time.time()
                    execution_time = end - start
                        
                    start = time.time()
                    list_result = to_list(result)
                    end = time.time()
                    result_set_creation_time = end - start

                    # Create output directory and save result set
                    result_set_dir = result_sets_dir + "/" + triple_store + "_" + policy + "_" + ds + "/" + query_set.split('/')[2] + "/" + str(query_version + 1)
                    Path(result_set_dir).mkdir(parents=True, exist_ok=True)
                    file = open(result_set_dir + "/" + query_file_name.split('.')[0], 'w')
                    write = csv.writer(file, delimiter=";")
                    write.writerows(list_result)

                    df = df.append(pd.Series([triple_store, ds, policy, query_set.split('/')[2], query_version, query_file_name, execution_time, result_set_creation_time], index=df.columns), ignore_index=True)

                elif policy == "ic":
                    with open(query_set_version + "/" + query_file_name, "r") as file:
                        query_text = file.read()
                        file.close()
                    for repository in range(1, int(repositories)+1):
                        repository_name = "{policy}_{dataset}_{repository}".format(policy=policy, dataset=ds, repository=repository)
                        getEndpoint = endpoints[triple_store]['get'].format(hostname="Starvers", port=port, repository_name=repository_name)
                        postEndpoint = endpoints[triple_store]['post'].format(hostname="Starvers", port=port, repository_name=repository_name)
                        engine = SPARQLWrapper(endpoint=getEndpoint, updateEndpoint=postEndpoint)
                        engine.setQuery(query_text)
                        engine.setReturnFormat(JSON)
                        engine.setOnlyConneg(True)
                        
                        print("Querying SPARQL endpoint {0} with query {1}". format(getEndpoint, query_file_name))
                        start = time.time()
                        result = engine.query()
                        end = time.time()
                        execution_time = end - start
                        
                        start = time.time()
                        list_result = to_list(result)
                        end = time.time()
                        result_set_creation_time = end - start

                        # Create output directory and save result set
                        result_set_dir = result_sets_dir + "/" + triple_store + "_" + policy + "_" + ds + "/" + query_set.split('/')[2] + "/" + str(repository)
                        Path(result_set_dir).mkdir(parents=True, exist_ok=True)
                        file = open(result_set_dir + "/" + query_file_name.split('.')[0], 'w')
                        write = csv.writer(file, delimiter=";")
                        write.writerows(list_result)

                        df = df.append(pd.Series([triple_store, ds, policy, query_set.split('/')[2], repository, query_file_name, execution_time, result_set_creation_time], index=df.columns), ignore_index=True)
                    

                elif policy == "cb":
                    file = open(query_set_version + "/" + query_file_name, "r")
                    query_text = file.read()
                    file.close()
                    manager = multiprocessing.Manager()

                    for repository in range(0, int(repositories/2)):
                        list_result = []     
                        result_set_creation_time = 0
                        logger.info("Recreating snapshot: " + str(repository))
                        for cs in range(0, repository+1):
                            print(cs)
                            if cs == 0:
                                repository_name_add = "{policy}_{dataset}_ic1".format(policy=policy, dataset=ds)
                                repository_name_del = "{policy}_{dataset}_empty".format(policy=policy, dataset=ds)
                            else:
                                repository_name_add = "{policy}_{dataset}_add_{v}-{ve}".format(policy=policy, dataset=ds, v=cs, ve=cs+1)
                                repository_name_del = "{policy}_{dataset}_del_{v}-{ve}".format(policy=policy, dataset=ds, v=cs, ve=cs+1)
                            
                            def query_add(execution_time_add: float, result_set_creation_time_add: float, cs_add: list):
                                getEndpoint = endpoints[triple_store]['get'].format(hostname="Starvers", port=port, repository_name=repository_name_add)
                                postEndpoint = endpoints[triple_store]['post'].format(hostname="Starvers", port=port, repository_name=repository_name_add)
                                engine = SPARQLWrapper(endpoint=getEndpoint, updateEndpoint=postEndpoint)
                                engine.setQuery(query_text)
                                engine.setReturnFormat(JSON)
                                engine.setOnlyConneg(True)
                                
                                print("Querying SPARQL endpoint {0} with query {1}". format(getEndpoint, query_file_name))
                                start = time.time()
                                result = engine.query()
                                end = time.time()
                                execution_time_add.value = end - start

                                start = time.time()
                                cs_add.extend(to_list(result))
                                end = time.time()
                                result_set_creation_time_add.value = end - start


                            def query_del(execution_time_del: float, result_set_creation_time_del: float, cs_del: list):
                                getEndpoint = endpoints[triple_store]['get'].format(hostname="Starvers", port=port, repository_name=repository_name_del)
                                postEndpoint = endpoints[triple_store]['post'].format(hostname="Starvers", port=port, repository_name=repository_name_del)
                                engine = SPARQLWrapper(endpoint=getEndpoint, updateEndpoint=postEndpoint)
                                engine.setQuery(query_text)
                                engine.setReturnFormat(JSON)
                                engine.setOnlyConneg(True)
                                
                                print("Querying SPARQL endpoint {0} with query {1}". format(getEndpoint, query_file_name))
                                start = time.time()
                                result = engine.query()
                                end = time.time()
                                execution_time_del.value = end - start

                                start = time.time()
                                cs_del.extend(to_list(result))
                                end = time.time()
                                result_set_creation_time_del.value = end - start

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

                            a1_rows = cum_result.view([('', cum_result.dtype)] * cum_result.shape[1]) if len(list_result)>1 else [[()]]
                            a2_rows = cs_del_arr.view([('', cs_del_arr.dtype)] * cs_del_arr.shape[1]) if len(list(cs_del))>1 else [[()]]
                            list_result = np.setdiff1d(a1_rows, a2_rows).view(cum_result.dtype).reshape(-1, cum_result.shape[1]).tolist()
                            end = time.time()

                            execution_time = execution_time + execution_time_add.value + execution_time_del.value
                            result_set_creation_time = result_set_creation_time + result_set_creation_time_add.value + result_set_creation_time_del.value + (end - start)
                    
                        # Create output directory and save result set
                        # TODO: format output
                        result_set_dir = result_sets_dir + "/" + triple_store + "_" + policy + "_" + ds + "/" + query_set.split('/')[2] + "/" + str(repository)
                        Path(result_set_dir).mkdir(parents=True, exist_ok=True)
                        file = open(result_set_dir + "/" + query_file_name.split('.')[0], 'w')
                        write = csv.writer(file, delimiter=";")
                        write.writerows(list_result)
                        df = df.append(pd.Series([triple_store, ds, policy, query_set.split('/')[2], repository, query_file_name, execution_time, result_set_creation_time], index=df.columns), ignore_index=True)
                
    df.to_csv("/starvers_eval/output/measurements/time.csv", sep=";", index=False)


def query():
    triple_store = sys.argv[1]
    policy = sys.argv[2]
    dataset = sys.argv[3]
    port = sys.argv[4]

    print("Query " + triple_store + ", " + policy + ", " + dataset + " on port: " + port)
    query_dataset(triple_store, policy, dataset, port)

# Clean outputs
# shutil.rmtree(result_sets_dir, ignore_errors=True)
# Start evaluation
query()


