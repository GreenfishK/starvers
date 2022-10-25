from SPARQLWrapper import SPARQLWrapper, POST, DIGEST, GET, JSON
from pathlib import Path
import os
import pandas as pd
import sys


###################################### Parameters ######################################
policies=['ic', 'cb'] # ["ic", "cb", "tb", "tbsf", "tbsh"]
datasets=['bearb-day'] # ['beara', 'bearb-hour', 'bearb-day', 'bearc']
triple_stores=['jenatdb2'] # ['graphdb', 'jenatdb2']
final_queries= "/opt/starvers/eval/queries"
ds_queries_map={'ic': {
                    'beara': {'query_versions': 1, 'repositories': 58, 'queries': ['ic/queries_beara/high',
                                                            'ic/queries_beara/low']}, 
                    'bearb-day': {'query_versions': 1,'repositories': 89, 'queries': ['ic/queries_bearb/lookup',
                                                                'ic/queries_bearb/join']}, 
                    'bearb-hour': {'query_versions': 1, 'repositories': 1299, 'queries': ['ic/queries_bearb/lookup',
                                                                'ic/queries_bearb/join']},
                    'bearc': {'query_versions': 1, 'repositories': 32, 'queries': ['ic/queries_bearc']}       
                },    
                'cb': {
                    'beara': {'query_versions': 1, 'repositories': 116, 'queries': ['cb/queries_beara/high',
                                                            'cb/queries_beara/low']}, 
                    'bearb-day': {'query_versions': 1, 'repositories': 178, 'queries': ['cb/queries_bearb/lookup',
                                                                'cb/queries_bearb/join']}, 
                    'bearb-hour': {'query_versions': 1, 'repositories': 2598, 'queries': ['cb/queries_bearb/lookup',
                                                                'cb/queries_bearb/join']},
                    'bearc': {'query_versions': 1, 'repositories': 64, 'queries': ['cb/queries_bearc']}       
                },        
                'tb':{
                    'beara': {'query_versions': 58, 'repositories': 1, 'queries': ['tb/queries_beara/high',
                                                            'tb/queries_beara/low']}, 
                    'bearb-day': {'query_versions': 89, 'repositories': 1, 'queries': ['tb/queries_bearb-day/lookup',
                                                                'tb/queries_bearb-day/join']}, 
                    'bearb-hour': {'query_versions': 1299, 'repositories': 1, 'queries': ['tb/queries_bearb-hour/lookup',
                                                                'tb/queries_bearb-hour/join']},
                    'bearc': {'query_versions': 32, 'repositories': 1, 'queries': ['tb/queries_bearc']}       
                },
                'tbsf': {
                    'beara': {'query_versions': 58, 'repositories': 1,'queries': ['tbsf/queries_beara/high',
                                                            'tbsf/queries_beara/low']}, 
                    'bearb-day': {'query_versions': 89, 'repositories': 1, 'queries': ['tbsf/queries_bearb-day/lookup',
                                                                'tbsf/queries_bearb-day/join']}, 
                    'bearb-hour': {'query_versions': 1299, 'repositories': 1, 'queries': ['tbsf/queries_bearb-hour/lookup',
                                                                'tbsf/queries_bearb-hour/join']},
                    'bearc': {'query_versions': 32, 'repositories': 1, 'queries': ['tbsf/queries_bearc']}       
                },
                'tbsh': {
                    'beara': {'query_versions': 58, 'repositories': 1, 'queries': ['tbsh/queries_beara/high',
                                                            'tbsh/queries_beara/low']}, 
                    'bearb-day': {'query_versions': 89, 'repositories': 1, 'queries': ['tbsh/queries_bearb-day/lookup',
                                                                'tbsh/queries_bearb-day/join']}, 
                    'bearb-hour': {'query_versions': 1299, 'repositories': 1, 'queries': ['tbsh/queries_bearb-day/lookup',
                                                                'tbsh/queries_bearb-day/join']},
                    'bearc': {'query_versions': 32, 'repositories': 1, 'queries': ['tbsh/queries_bearc']}       
                }
}

endpoints = {'graphdb': {'get': 'http://{hostname}:{port}/repositories/{repository_name}',
                        'post': 'http://{hostname}:{port}/repositories/{repository_name}/statements'},
            'jenatdb2': {'get': 'http://{hostname}:{port}/{repository_name}/sparql',
                        'post': 'http://{hostname}:{port}/{repository_name}/update'}}

###################################### Evaluation ######################################
# header: tripleStore,snapshot,min,mean,max,stddev,count,sum
# aggregation on tripleStore and version level
df = pd.DataFrame(columns=['triplestore', 'dataset', 'policy', 'version', 'query_set', 'query', 'execution_time'])

def query_dataset(triple_store: str, policy: str, ds: str, port: int):
    query_sets = ds_queries_map[policy][ds]['queries']
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
                    if policy == "ic":
                        with open(query_set_version + "/" + query_file_name, "r") as file:
                            query_text = file.read()
                            engine.setQuery(query_text)
                            print("Querying SPARQL endpoint {0} with query {1}". format(getEndpoint, query_file_name))
                            result = engine.query()
                            print(result)
                            file.close()
                    elif policy == "cb":
                        print("Not yet implemented")
                        # Execute queries against repositories starting from v0 (initial snapshot) up until version v x
                        # Save result sets for every add and del repository
                        # Initialize final result set
                        # Iterate over all changeset results until version v
                        # Add add_result_sets to final set and then remove del_result_sets from final set
                    elif policy in ["tbsh", "tbsf", "tb"]:
                        print("Not yet implemented")

def query():
    triple_store = sys.argv[1]
    policy = sys.argv[2]
    dataset = sys.argv[3]
    port = sys.argv[4]

    print("Query " + triple_store + " " + policy + " " + dataset + "on port" + port)
    query_dataset(triple_store, policy, dataset, port)

query()


