from SPARQLWrapper import SPARQLWrapper, POST, DIGEST, GET, JSON
from pathlib import Path
import os
import pandas as pd
import sys
import time

###################################### Parameters ######################################
final_queries= "/starvers_eval/queries/final_queries"
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
df = pd.DataFrame(columns=['triplestore', 'dataset', 'policy', 'query_set', 'query_version', 'query', 'execution_time'])

def query_dataset(triple_store: str, policy: str, ds: str, port: int):
    query_sets = ds_queries_map[policy][ds]['query_sets']
    query_versions = ds_queries_map[policy][ds]['query_versions']
    repositories = ds_queries_map[policy][ds]['repositories']

    for repository in range(1, repositories+1):
        if repositories==1:
            repository_name = "{policy}_{dataset}".format(triple_store=triple_store, policy=policy, dataset=ds)
        else:
            repository_name = "{policy}_{dataset}_{snapshot}".format(triple_store=triple_store, policy=policy, dataset=ds, snapshot=repository)
        getEndpoint = endpoints[triple_store]['get'].format(hostname="Starvers", port=port, repository_name=repository_name)
        postEndpoint = endpoints[triple_store]['post'].format(hostname="Starvers", port=port, repository_name=repository_name)
        engine = SPARQLWrapper(endpoint=getEndpoint, updateEndpoint=postEndpoint)

        for query_version in range(query_versions):
            for query_set in query_sets:
                query_set_version = final_queries + "/" + query_set  +  "/" + str(query_version)
                for query_file_name in os.listdir(query_set_version):
                    print("Processing query {0}".format(query_file_name))
                    if policy in ["ic", "tbsh", "tbsf", "tb"]:
                        with open(query_set_version + "/" + query_file_name, "r") as file:
                            query_text = file.read()
                            engine.setQuery(query_text)
                            print("Querying SPARQL endpoint {0} with query {1}". format(getEndpoint, query_file_name))
                            start = time.time()
                            engine.query()
                            end = time.time()
                            execution_time = end - start
                            df.add([triple_store, ds, policy, query_set.split('/')[2], query_version, query_file_name, execution_time])
                            file.close()
                    elif policy == "cb":
                        print("Not yet implemented")
                        # Execute queries against repositories starting from v0 (initial snapshot) up until version v x
                        # Save result sets for every add and del repository
                        # Initialize final result set
                        # Iterate over all changeset results until version v
                        # Add add_result_sets to final set and then remove del_result_sets from final set
    
    df.to_csv("/starvers_eval/output/measurements/time.csv", sep=";")


def query():
    triple_store = sys.argv[1]
    policy = sys.argv[2]
    dataset = sys.argv[3]
    port = sys.argv[4]

    print("Query " + triple_store + ", " + policy + ", " + dataset + " on port: " + port)
    query_dataset(triple_store, policy, dataset, port)

query()


