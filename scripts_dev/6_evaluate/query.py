from SPARQLWrapper import SPARQLWrapper, Wrapper, POST, JSON
from rdflib import Graph
from rdflib.term import URIRef, Literal, BNode
from rdflib.query import Result
import pandas as pd

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
triple_store = sys.argv[1]
policy = sys.argv[2]
dataset = sys.argv[3]
port = sys.argv[4]
final_queries= "/starvers_eval/queries/final_queries"
result_sets_dir = "/starvers_eval/output/result_sets"
ds_queries_map={'ic_mr_tr': {
                    'beara': {'query_versions': 1, 'repositories': 58, 'query_sets': ['ic_mr_tr/beara/high',
                                                            'ic_mr_tr/beara/low']}, 
                    'bearb_day': {'query_versions': 1,'repositories': 89, 'query_sets': ['ic_mr_tr/bearb_day/lookup',
                                                                'ic_mr_tr/bearb_day/join']}, 
                    'bearb_hour': {'query_versions': 1, 'repositories': 1299, 'query_sets': ['ic_mr_tr/bearb_hour/lookup',
                                                                'ic_mr_tr/bearb_hour/join']},
                    'bearc': {'query_versions': 1, 'repositories': 33, 'query_sets': ['ic_mr_tr/bearc/complex']}       
                }, 
                'cb_mr_tr': {
                    'beara': {'query_versions': 1, 'repositories': 116, 'query_sets': ['cb_mr_tr/beara/high',
                                                            'cb_mr_tr/beara/low']}, 
                    'bearb_day': {'query_versions': 1, 'repositories': 178, 'query_sets': ['cb_mr_tr/bearb_day/lookup',
                                                                'cb_mr_tr/bearb_day/join']}, 
                    'bearb_hour': {'query_versions': 1, 'repositories': 2598, 'query_sets': ['cb_mr_tr/bearb_hour/lookup',
                                                                'cb_mr_tr/bearb_hour/join']},
                    'bearc': {'query_versions': 1, 'repositories': 66, 'query_sets': ['cb_mr_tr/bearc/complex']}       
                },     
                'ic_sr_ng': {
                    'beara': {'query_versions': 58, 'repositories': 1, 'query_sets': ['ic_sr_ng/beara/high',
                                                            'ic_sr_ng/beara/low']}, 
                    'bearb_day': {'query_versions': 89,'repositories': 1, 'query_sets': ['ic_sr_ng/bearb_day/lookup',
                                                                'ic_sr_ng/bearb_day/join']}, 
                    'bearb_hour': {'query_versions': 1299, 'repositories': 1, 'query_sets': ['ic_sr_ng/bearb_hour/lookup',
                                                                'ic_sr_ng/bearb_hour/join']},
                    'bearc': {'query_versions': 33, 'repositories': 1, 'query_sets': ['ic_sr_ng/bearc/complex']}       
                },  
                'cb_sr_ng': {
                    'beara': {'query_versions': 58, 'repositories': 1, 'query_sets': ['cb_sr_ng/beara/high',
                                                            'cb_sr_ng/beara/low']}, 
                    'bearb_day': {'query_versions': 89, 'repositories': 1, 'query_sets': ['cb_sr_ng/bearb_day/lookup',
                                                                'cb_sr_ng/bearb_day/join']}, 
                    'bearb_hour': {'query_versions': 1299, 'repositories': 1, 'query_sets': ['cb_sr_ng/bearb_hour/lookup',
                                                                'cb_sr_ng/bearb_hour/join']},
                    'bearc': {'query_versions': 33, 'repositories': 1, 'query_sets': ['cb_sr_ng/bearc/complex']}       
                },     
                'tb_sr_ng':{
                    'beara': {'query_versions': 58, 'repositories': 1, 'query_sets': ['tb_sr_ng/beara/high',
                                                            'tb_sr_ng/beara/low']}, 
                    'bearb_day': {'query_versions': 89, 'repositories': 1, 'query_sets': ['tb_sr_ng/bearb_day/lookup',
                                                                'tb_sr_ng/bearb_day/join']}, 
                    'bearb_hour': {'query_versions': 1299, 'repositories': 1, 'query_sets': ['tb_sr_ng/bearb_hour/lookup',
                                                                'tb_sr_ng/bearb_hour/join']},
                    'bearc': {'query_versions': 33, 'repositories': 1, 'query_sets': ['tb_sr_ng/bearc/complex']}       
                },
                'tb_sr_rs': {
                    'beara': {'query_versions': 58, 'repositories': 1, 'query_sets': ['tb_sr_rs/beara/high',
                                                            'tb_sr_rs/beara/low']}, 
                    'bearb_day': {'query_versions': 89, 'repositories': 1, 'query_sets': ['tb_sr_rs/bearb_day/lookup',
                                                                'tb_sr_rs/bearb_day/join']}, 
                    'bearb_hour': {'query_versions': 1299, 'repositories': 1, 'query_sets': ['tb_sr_rs/bearb_day/lookup',
                                                                'tb_sr_rs/bearb_day/join']},
                    'bearc': {'query_versions': 33, 'repositories': 1, 'query_sets': ['tb_sr_rs/bearc/complex']}       
                }
}

endpoints = {'graphdb': {'get': 'http://{hostname}:{port}/repositories/{repository_name}',
                        'post': 'http://{hostname}:{port}/repositories/{repository_name}/statements'},
            'jenatdb2': {'get': 'http://{hostname}:{port}/{repository_name}/sparql',
                        'post': 'http://{hostname}:{port}/{repository_name}/update'}}    
LOCAL_TIMEZONE = datetime.now(timezone.utc).astimezone().tzinfo                    
init_version_timestamp = datetime(2022,10,1,12,0,0,0,LOCAL_TIMEZONE)

###################################### Evaluation ######################################
# header: tripleStore,snapshot,min,mean,max,stddev,count,sum
# aggregation on tripleStore and version level
logger.info("Evaluate " + triple_store + ", " + policy + ", " + dataset + " on port: " + port)

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


df = pd.DataFrame(columns=['triplestore', 'dataset', 'policy', 'query_set', 'snapshot', 'snapshot_ts', 'query', 'execution_time', 'snapshot_creation_time'])

query_sets = ds_queries_map[policy][dataset]['query_sets']
query_versions = ds_queries_map[policy][dataset]['query_versions']
repositories = ds_queries_map[policy][dataset]['repositories']

engine = SPARQLWrapper(endpoint="dummy")
engine.setReturnFormat(JSON)
engine.setOnlyConneg(True)
engine.setMethod(POST)

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

for query_set in query_sets:
    for query_version in range(query_versions):
        query_set_version = final_queries + "/" + query_set  +  "/" + str(query_version)
        snapshot_ts = init_version_timestamp + timedelta(seconds=query_version)
        current_query_version = query_version

        for query_file_name in os.listdir(query_set_version):
            logger.info("Processing query {0}".format(query_file_name))
            execution_time = 0
            result_set_creation_time = 0

            file = open(query_set_version + "/" + query_file_name, "r")
            query_text = file.read()
            engine.setQuery(query_text)
            file.close()

            if policy in ["ic_sr_ng", "tb_sr_ng", "tb_sr_rs"]:
                _set_endpoints(dataset, policy, endpoints, engine)   

                logger.info("Querying SPARQL endpoint {0} with query {1}". format(engine.endpoint, query_file_name))
                start = time.time()
                result = engine.query()
                end = time.time()
                execution_time = end - start
                    
                # Create output directory and save result set
                result_set_dir = result_sets_dir + "/" + triple_store + "/" + policy + "_" + dataset + "/" + query_set.split('/')[2] + "/" + str(query_version)
                Path(result_set_dir).mkdir(parents=True, exist_ok=True)
                file = open(result_set_dir + "/" + query_file_name.split('.')[0] + ".csv", 'w')
                write = csv.writer(file, delimiter=";")
                write.writerows(to_list(result))

                df = df.append(pd.Series([triple_store, dataset, policy, query_set.split('/')[2], query_version, snapshot_ts, query_file_name, execution_time, 0], index=df.columns), ignore_index=True)
            
            elif policy == "cb_sr_ng":
                _set_endpoints(dataset, policy, endpoints, engine)   

                change_sets_until_v = """ Select ?graph ?s ?p ?o WHERE {{
                                        graph ?graph 
                                        {{
                                            ?s ?p ?o .
                                            filter (str(?graph) <= "http://starvers_eval/v{0}/added" || str(?graph) <= "http://starvers_eval/v{0}/deleted")
                                        }}
                                    }} order by ?graph""".format(str(query_version).zfill(len(str(query_versions))))
                engine.setQuery(change_sets_until_v)
                
                def build_snapshot(change_sets: Wrapper.QueryResult) -> Graph: 
                    """
                    Build the snapshot at version :query_version by adding triples from the add-set 
                    and deleting triples from the del-set, consecutively.
                    """
                    graph = Graph()

                    if not "head" in change_sets or not "vars" in change_sets["head"]:
                        return graph

                    if not "results" in change_sets or not "bindings" in change_sets["results"]:
                        return graph

                    for r in change_sets["results"]["bindings"]:
                        # parse subject
                        if r['s']['type'] == "uri":
                            s = URIRef(r['s']['value'])
                        else:
                            s = BNode(r['s']['value'])

                        # parse predicate
                        p = URIRef(r['p']['value'])

                        # parse object
                        if r['o']['type']  == "uri":
                            o = URIRef(r['o']['value'])
                        elif r['o']['type'] == "blank":
                            o = BNode(r['o']['value'])
                        else:
                            value = r['o']["value"]
                            lang = r['o'].get("xml:lang", None)
                            datatype = r['o'].get("datatype", None)
                            o = Literal(value, lang=lang, datatype=datatype)
                        
                        # Add or remove from graph, depending on which change set the triple comes from
                        if r['graph']['value'].split('/')[-1].startswith('added'):
                            graph.add((s, p, o))
                        else:
                            graph.remove((s, p, o))

                    return graph
                
                # Query all changesets from the triplestore until version :query_version 
                # ordered by change set versions
                if current_query_version == query_version:
                    logger.info("Build snapshot version {0}". format(str(query_version).zfill(len(str(query_versions)))))
                    start = time.time()
                    result = engine.query()
                    snapshot_g = build_snapshot(change_sets=result.convert())
                    end = time.time()
                    snapshot_creation_time = end - start
                    current_query_version = None

                # Query from in-memory snapshot at version :query_version
                logger.info("Querying SPARQL endpoint {0} with query {1}". format(engine.endpoint, query_file_name))
                start = time.time()
                query_result = snapshot_g.query(query_text)
                end = time.time()
                execution_time = end - start

                # Create output directory and save result set
                result_set_dir = result_sets_dir + "/" + triple_store + "/" + policy + "_" + dataset + "/" + query_set.split('/')[2] + "/" + str(query_version)
                Path(result_set_dir).mkdir(parents=True, exist_ok=True)
                query_result.serialize(result_set_dir + "/" + query_file_name.split('.')[0] + ".csv", format="csv")
                
                df = df.append(pd.Series([triple_store, dataset, policy, query_set.split('/')[2], query_version, snapshot_ts, query_file_name, execution_time, snapshot_creation_time], index=df.columns), ignore_index=True)
            
            elif policy == "ic_mr_tr":
                for repository in range(1, int(repositories)+1):
                    _set_endpoints(dataset, policy, endpoints, engine, str(repository))

                    logger.info("Querying SPARQL endpoint {0} with query {1}". format(engine.endpoint, query_file_name))
                    start = time.time()
                    result = engine.query()
                    end = time.time()
                    execution_time = end - start
                    
                    # Create output directory and save result set
                    result_set_dir = result_sets_dir + "/" + triple_store + "/" + policy + "_" + dataset + "/" + query_set.split('/')[2] + "/" + str(repository)
                    Path(result_set_dir).mkdir(parents=True, exist_ok=True)
                    file = open(result_set_dir + "/" + query_file_name.split('.')[0], 'w')
                    write = csv.writer(file, delimiter=";")
                    write.writerows(to_list(result))

                    snapshot_ts = init_version_timestamp + timedelta(seconds=repository-1)
                    df = df.append(pd.Series([triple_store, dataset, policy, query_set.split('/')[2], repository, snapshot_ts, query_file_name, execution_time, 0], index=df.columns), ignore_index=True)

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
                    df = df.append(pd.Series([triple_store, dataset, policy, query_set.split('/')[2], repository, query_file_name, execution_time, result_set_creation_time], index=df.columns), ignore_index=True)"""

logger.info("Writing performance measurements to disk ...")            
df.drop_duplicates(inplace=True)
df.to_csv("/starvers_eval/output/measurements/time.csv", sep=";", index=False, mode='a')


