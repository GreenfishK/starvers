from genericpath import isfile
import starvers.starvers
from pathlib import Path
import os
import sys

raw_queries_dir=str(Path.home()) + "/.BEAR/queries/raw_queries/"
output_queries_dir=str(Path.home()) + "/.BEAR/queries/final_queries/"
queries={
    "ic":{
        "beara/high":[1, "ic/ts"],
        "beara/low": [1, "ic/ts"],
        "beara": [1, "ic/ts"],
        "bearb/join": [1, "ic/bgp"],
        "bearb/lookup": [1, "ic/ts"],
        "bearc":[1, None]},
    "cb":{
        "beara/high":[1, "cb/ts"],
        "beara/low": [1, "cb/ts"],
        "beara": [1, "ic/ts"],
        "bearb/join": [1, "cb/bgp"],
        "bearb/lookup": [1, "cb/ts"],
        "bearc":[1, None]},
    "tb":{
        "beara/high":[58, "tb/ts"],
        "beara/low": [58, "tb/ts"],
        "beara": [58, "tb/ts"],
        "bearb/join": [1299, "tb/bgp"],
        "bearb/lookup": [1299, "tb/ts"],
        "bearc":[32, None]},
    "tbsf":{
        "beara/high":[58, "ic/ts"],
        "beara/low": [58, "ic/ts"],
        "beara": [58, "ic/ts"],
        "bearb/join": [1299, "ic/bgp"],
        "bearb/lookup": [1299, "ic/ts"],
        "bearc":[32, None]},
    "tbsh":{
        "beara/high":[58, "ic/ts"],
        "beara/low": [58, "ic/ts"],
        "beara": [58, "ic/ts"],
        "bearb/join": [1299, "ic/bgp"],
        "bearb/lookup": [1299, "ic/ts"],
        "bearc":[32, None]},
    }
policies=["ic", "cb", "tb", "tbsf", "tbsh"]
input_representations=["ts", "bgp", "sparql"]

# Create directories
for policy in policies:
    for querySet in queries[policy].keys():
        Path(output_queries_dir + str.upper(policy) + "/queries_" + querySet).mkdir(parents=True, exist_ok=True)


# Create queries
for policy in policies:
    for querySet in queries[policy].keys():
        pathToQueries = raw_queries_dir + "queries_" + querySet
        for queriesFile in os.listdir(raw_queries_dir + "queries_" + querySet):
            if os.path.isfile(pathToQueries + "/" + queriesFile):
                with open(pathToQueries + "/" + queriesFile, 'r') as file:
                    lines = file.readlines()
                    for i, tripleStatment in enumerate(lines):
                        output_query = ""
                        with open(os.path.join(sys.path[0]) +"/templates/" + queries[policy][querySet][1] + ".txt", 'r') as templateFile:
                            template = templateFile.read()
                            output_query = template.format(tripleStatment)
                            templateFile.close()
                        with open(output_queries_dir + str.upper(policy) + "/queries_" + querySet + "/" + str(i) + "_" + queriesFile, 'w') as output_file:
                            output_file.write(output_query)
                            output_file.close()
                file.close()


