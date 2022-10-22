from SPARQLWrapper import SPARQLWrapper, POST, DIGEST, GET, JSON
import multiprocessing
from pathlib import Path
import os
import logging
import pandas as pd
import pexpect

# header: tripleStore,snapshot,min,mean,max,stddev,count,sum
# aggregation on tripleStore and version level

# Run 4 containers in parallel - one for each dataset, each with a different port

# Execute all queries against every snapshot and measure the execution time
# Aggreagte execution time on triple store and snapshot level

# Specific for CB
# Execute queries against repositories starting from v0 (initial snapshot) up until version v x
# Save result sets for every add and del repository
# Initialize final result set
# Iterate over all changeset results until version v
# Add add_result_sets to final set and then remove del_result_sets from final set

# save dataset

###################################### Parameters ######################################
policies=['IC'] # ["IC", "CB", "TB", "TBSF", "TBSH"]
datasets=['bearb-day'] # ['beara', 'bearb-hour', 'bearb-day', 'bearc']
triple_stores=['jenatdb2'] # ['graphdb', 'jenatdb2']
# On host machine
# final_queries=str(Path.home()) + "/.BEAR/queries/final_queries" 
# In docker container
final_queries= "/opt/starvers/eval/queries"
ds_queries_map={'IC': {
                    'beara': {'query_versions': 1, 'repositories': 58, 'queries': ['IC/queries_beara/high',
                                                            'IC/queries_beara/low']}, 
                    'bearb-day': {'query_versions': 1,'repositories': 89, 'queries': ['IC/queries_bearb/lookup',
                                                                'IC/queries_bearb/join']}, 
                    'bearb-hour': {'query_versions': 1, 'repositories': 1299, 'queries': ['IC/queries_bearb/lookup',
                                                                'IC/queries_bearb/join']},
                    'bearc': {'query_versions': 1, 'repositories': 32, 'queries': ['IC/queries_bearc']}       
                },    
                'CB': {
                    'beara': {'query_versions': 1, 'repositories': 116, 'queries': ['CB/queries_beara/high',
                                                            'CB/queries_beara/low']}, 
                    'bearb-day': {'query_versions': 1, 'repositories': 178, 'queries': ['CB/queries_bearb/lookup',
                                                                'CB/queries_bearb/join']}, 
                    'bearb-hour': {'query_versions': 1, 'repositories': 2598, 'queries': ['CB/queries_bearb/lookup',
                                                                'CB/queries_bearb/join']},
                    'bearc': {'query_versions': 1, 'repositories': 64, 'queries': ['CB/queries_bearc']}       
                },        
                'TB':{
                    'beara': {'query_versions': 58, 'repositories': 1, 'queries': ['TB/queries_beara/high',
                                                            'TB/queries_beara/low']}, 
                    'bearb-day': {'query_versions': 89, 'repositories': 1, 'queries': ['TB/queries_bearb-day/lookup',
                                                                'TB/queries_bearb-day/join']}, 
                    'bearb-hour': {'query_versions': 1299, 'repositories': 1, 'queries': ['TB/queries_bearb-hour/lookup',
                                                                'TB/queries_bearb-hour/join']},
                    'bearc': {'query_versions': 32, 'repositories': 1, 'queries': ['TB/queries_bearc']}       
                },
                'TBSF': {
                    'beara': {'query_versions': 58, 'repositories': 1,'queries': ['TBSF/queries_beara/high',
                                                            'TBSF/queries_beara/low']}, 
                    'bearb-day': {'query_versions': 89, 'repositories': 1, 'queries': ['TBSF/queries_bearb-day/lookup',
                                                                'TBSF/queries_bearb-day/join']}, 
                    'bearb-hour': {'query_versions': 1299, 'repositories': 1, 'queries': ['TBSF/queries_bearb-hour/lookup',
                                                                'TBSF/queries_bearb-hour/join']},
                    'bearc': {'query_versions': 32, 'repositories': 1, 'queries': ['TBSF/queries_bearc']}       
                },
                'TBSH': {
                    'beara': {'query_versions': 58, 'repositories': 1, 'queries': ['TBSH/queries_beara/high',
                                                            'TBSH/queries_beara/low']}, 
                    'bearb-day': {'query_versions': 89, 'repositories': 1, 'queries': ['TBSH/queries_bearb-day/lookup',
                                                                'TBSH/queries_bearb-day/join']}, 
                    'bearb-hour': {'query_versions': 1299, 'repositories': 1, 'queries': ['TBSH/queries_bearb-day/lookup',
                                                                'TBSH/queries_bearb-day/join']},
                    'bearc': {'query_versions': 32, 'repositories': 1, 'queries': ['TBSH/queries_bearc']}       
                }
}

endpoints = {'graphdb': {'get': 'http://{hostname}:{port}/repositories/{repository_name}',
                        'post': 'http://{hostname}:{port}/repositories/{repository_name}/statements'},
            'jenatdb2': {'get': 'http://{hostname}:{port}/{repository_name}/sparql',
                        'post': 'http://{hostname}:{port}/{repository_name}/update'}}
ports = {}
with open(".env") as myfile:
    for line in myfile:
        name, var = line.partition("=")[::2]
        if name.endswith("_port"):
            ports[name.strip()] = int(var.strip())
###################################### Evaluation ######################################

df = pd.DataFrame(columns=['triplestore', 'dataset', 'policy', 'version', 'query_set', 'query', 'execution_time'])

def query_dataset(triple_store: str, policy: str, ds: str, port: int):
    query_sets = ds_queries_map[policy][ds]['queries']
    query_versions = ds_queries_map[policy][ds]['query_versions']
    repositories = ds_queries_map[policy][ds]['repositories']

    for query_version in range(query_versions):
        for query in query_sets:
            query_set_version_dir = final_queries + "/" + query  +  "/" + str(query_version)
            for query_file_name in os.listdir(query_set_version_dir):
                if policy == "IC":
                    for repository in range(1, repositories+1):
                        repository_name = "{policy}_{dataset}_{snapshot}".format(triple_store=triple_store, policy=policy.lower(), dataset=ds, snapshot=repository)
                        getEndpoint = endpoints[triple_store]['get'].format(hostname="Starvers", port=port, repository_name=repository_name)
                        postEndpoint = endpoints[triple_store]['post'].format(hostname="Starvers", port=port, repository_name=repository_name)
                        engine = SPARQLWrapper(endpoint=getEndpoint, updateEndpoint=postEndpoint)
                        with open(query_set_version_dir + "/" + query_file_name, "r") as file:
                            query_text = file.read()
                            engine.setQuery(query_text)
                            print("Querying SPARQL endpoint {0} with query {1}". format(getEndpoint, query_file_name))
                            result = engine.query()
                            print(result)
                            file.close()
                elif policy == "CB":
                    print("Not yet implemented")
                elif policy in ["TBSH", "TBSF", "TB"]:
                    print("Not yet implemented")


def bulk_query():
    pass
    for triple_store in triple_stores:
        for policy in policies:
            for dataset in datasets:
                print(triple_store + " " + policy + " " + dataset)
                port = ports[triple_store + "_" + policy.lower() + "_" + "_".join(dataset.split('-')) + "_port"]
                query_dataset(triple_store, policy, dataset, port)
                print("Done!")

bulk_query()


# If you want to add parallel processing:
#t1 = multiprocessing.Process(target=query_dataset, args=(triple_store, policy, datasets[0], 7200)) # 3030 for jenatdb2
#t2 = multiprocessing.Process(target=query_dataset, args=(triple_store, policy, datasets[1], 7210)) # 3040 for jenatdb2
#t3 = multiprocessing.Process(target=query_dataset, args=(triple_store, policy, datasets[2], 7220)) # 3050 for jenatdb2
#t4 = multiprocessing.Process(target=query_dataset, args=(triple_store, policy, datasets[3], 7230)) # 3060 for jenatdb2

#t1.start()
#t2.start()
#t3.start()
#t4.start()

# wait until threads are completely executed
#t1.join()
#t2.join()
#t3.join()
#t4.join()
