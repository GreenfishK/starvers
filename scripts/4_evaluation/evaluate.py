from SPARQLWrapper import SPARQLWrapper, POST, DIGEST, GET, JSON
import multiprocessing
from pathlib import Path
import os
import logging

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

policies=["ic", "cb", "tb", "tbsf", "tbsh"]
datasets=['beara', 'bearb-hour', 'bearb-day', 'bearc']
triple_stores=['graphdb', 'jenatdb2']
final_queries=str(Path.home()) + "/.BEAR/queries/final_queries"
ds_queries_map={'ic': {
                    'beara': {'query_versions': 1, 'repositories': 58, 'queries': ['tb/queries_beara/high',
                                                            'tb/queries_beara/low']}, 
                    'bearb-day': {'query_versions': 1,'repositories': 89, 'queries': ['tb/queries_bearb-day/lookup',
                                                                'tb/queries_bearb-day/join']}, 
                    'bearb-hour': {'query_versions': 1, 'repositories': 1299, 'queries': ['tb/queries_bearb-day/lookup',
                                                                'tb/queries_bearb-day/join']},
                    'bearc': {'query_versions': 1, 'repositories': 32, 'queries': ['tb/queries_bearb-day/lookup',
                                                            'tb/queries_bearb-day/join']}       
                },    
                'cb': {
                    'beara': {'query_versions': 1, 'repositories': 116, 'queries': ['tb/queries_beara/high',
                                                            'tb/queries_beara/low']}, 
                    'bearb-day': {'query_versions': 1, 'repositories': 178, 'queries': ['tb/queries_bearb-day/lookup',
                                                                'tb/queries_bearb-day/join']}, 
                    'bearb-hour': {'query_versions': 1, 'repositories': 2598, 'queries': ['tb/queries_bearb-day/lookup',
                                                                'tb/queries_bearb-day/join']},
                    'bearc': {'query_versions': 1, 'repositories': 64, 'queries': ['tb/queries_bearb-day/lookup',
                                                            'tb/queries_bearb-day/join']}       
                },        
                'tb':{
                    'beara': {'query_versions': 58, 'repositories': 1, 'queries': ['tb/queries_beara/high',
                                                            'tb/queries_beara/low']}, 
                    'bearb-day': {'query_versions': 89, 'repositories': 1, 'queries': ['tb/queries_bearb-day/lookup',
                                                                'tb/queries_bearb-day/join']}, 
                    'bearb-hour': {'query_versions': 1299, 'repositories': 1, 'queries': ['tb/queries_bearb-day/lookup',
                                                                'tb/queries_bearb-day/join']},
                    'bearc': {'query_versions': 32, 'repositories': 1, 'queries': ['tb/queries_bearb-day/lookup',
                                                            'tb/queries_bearb-day/join']}       
                },
                'tbsf': {
                    'beara': {'query_versions': 58, 'repositories': 1,'queries': ['tb/queries_beara/high',
                                                            'tb/queries_beara/low']}, 
                    'bearb-day': {'query_versions': 89, 'repositories': 1, 'queries': ['tb/queries_bearb-day/lookup',
                                                                'tb/queries_bearb-day/join']}, 
                    'bearb-hour': {'query_versions': 1299, 'repositories': 1, 'queries': ['tb/queries_bearb-day/lookup',
                                                                'tb/queries_bearb-day/join']},
                    'bearc': {'query_versions': 32, 'repositories': 1, 'queries': ['tb/queries_bearb-day/lookup',
                                                            'tb/queries_bearb-day/join']}       
                },
                'tbsh': {
                    'beara': {'query_versions': 58, 'repositories': 1, 'queries': ['tb/queries_beara/high',
                                                            'tb/queries_beara/low']}, 
                    'bearb-day': {'query_versions': 89, 'repositories': 1, 'queries': ['tb/queries_bearb-day/lookup',
                                                                'tb/queries_bearb-day/join']}, 
                    'bearb-hour': {'query_versions': 1299, 'repositories': 1, 'queries': ['tb/queries_bearb-day/lookup',
                                                                'tb/queries_bearb-day/join']},
                    'bearc': {'query_versions': 32, 'repositories': 1, 'queries': ['tb/queries_bearb-day/lookup',
                                                            'tb/queries_bearb-day/join']}       
                }
}

def query_dataset(triple_store: str, policy: str, ds: str, port: int):
    queries_to_execute = ds_queries_map[policy][ds]['queries']
    query_versions = ds_queries_map[policy][ds]['query_versions']
    repositories = ds_queries_map[policy][ds]['repositories']

    if triple_store == "graphdb":      
        for query_version in range(query_versions):
            for query in queries_to_execute:
                query_dir = final_queries + "/" + query  +  "/" + str(query_version)
                for query_file_name in os.listdir(query_dir):
                    if policy == "ic":
                        for repository in range(1, repositories):
                            repository_name = "{triple_store}_{policy}_{dataset}_{snapshot}".format(triple_store=triple_store, policy=policy, dataset=ds, snapshot=repository)
                            getEndpoint = "http://{hostname}:{port}/repositories/{repository_name}".format(hostname="Starvers", port=port, repository_name=repository_name)
                            postEndpoint = getEndpoint + "/statements"
                            engine = SPARQLWrapper(endpoint=getEndpoint, updateEndpoint=postEndpoint)
                            with open(query_dir + "/" + query_file_name, "r") as file:
                                query_text = file.read()
                                engine.setQuery(query_text)
                                logging.info("Querying repository {0}, on port {1} with query {2}". format(repository_name, port, query_file_name))
                                result = engine.query()
                                print(result)
                                file.close()
                    elif policy == "cb":
                        logging.info("Not yet implemented")
                    elif policy in ["tbsh", "tbsf", "tb"]:
                        logging.info("Not yet implemented")
    elif triple_store == "jenatdb2":
        logging.info("Not yet implemented")
        # TODO:  Run docker-compose



def bulk_query():
    pass
    for triple_store in triple_stores:
        for policy in policies:
            t1 = multiprocessing.Process(target=query_dataset, args=(triple_store, policy, datasets[0], 7200))
            t2 = multiprocessing.Process(target=query_dataset, args=(triple_store, policy, datasets[1], 7300))
            t3 = multiprocessing.Process(target=query_dataset, args=(triple_store, policy, datasets[2], 7400))
            t4 = multiprocessing.Process(target=query_dataset, args=(triple_store, policy, datasets[3], 7500))

            t1.start()
            t2.start()
            t3.start()
            t4.start()

            # wait until threads are completely executed
            t1.join()
            t2.join()
            t3.join()
            t4.join()

            print("Done!")

    