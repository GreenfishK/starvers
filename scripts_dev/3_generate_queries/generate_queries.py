from genericpath import isfile
from starvers.starvers import timestamp_query, split_prefixes_query
from pathlib import Path
import os
import sys
from datetime import datetime, timezone, timedelta
import shutil

raw_queries_dir="/starvers_eval/queries/raw_queries/"
output_queries_dir="/starvers_eval/queries/final_queries/"
policies_cmd = sys.argv[1]
policies = policies_cmd.split(" ")

queries={
    "ic":{
        "beara/high": {'output_dirs':{"beara/high": 1}, 'template': "ic/ts"},
        "beara/low": {'output_dirs':{"beara/low": 1}, 'template': "ic/ts"},
        "bearb/join": {'output_dirs':{ "bearb/join": 1}, 'template': "ic/bgp"},
        "bearb/lookup": {'output_dirs':{"bearb/lookup": 1}, 'template': "ic/ts"},
        "bearc": {'output_dirs':{ "bearc/complex": 1}, 'template': "ic/sparql"},
    },
    "icng":{
        "beara/high": {'output_dirs':{"beara/high": 58}, 'template': "icng/ts"},
        "beara/low": {'output_dirs':{"beara/low": 58}, 'template': "icng/ts"},
        "bearb/join": {'output_dirs':{"bearb_day/join": 89, "bearb_hour/join": 1299}, 'template': "icng/bgp"},
        "bearb/lookup": {'output_dirs':{"bearb_day/lookup": 89, "bearb_hour/lookup": 1299}, 'template': "icng/ts"},
        "bearc": {'output_dirs':{"bearc/complex": 32}, 'template': "icng/sparql"},
    },
    "cb":{
        "beara/high": {'output_dirs':{"beara/high": 1}, 'template': "cb/ts"},
        "beara/low": {'output_dirs':{"beara/low": 1}, 'template': "cb/ts"},
        "bearb/join": {'output_dirs':{ "bearb/join": 1}, 'template': "cb/bgp"},
        "bearb/lookup": {'output_dirs':{ "bearb/lookup": 1}, 'template': "cb/ts"},
        "bearc": {'output_dirs':{"bearc/complex": 1}, 'template': "cb/sparql"},
    },
    "cbng":{
        "beara/high": {'output_dirs':{"beara/high": 58}, 'template': "cbng/ts"},
        "beara/low": {'output_dirs':{"beara/low": 58}, 'template': "cbng/ts"},
        "bearb/join": {'output_dirs':{"bearb_day/join": 89, "bearb_hour/join": 1299}, 'template': "cbng/bgp"},
        "bearb/lookup": {'output_dirs':{"bearb_day/lookup": 89, "bearb_hour/lookup": 1299}, 'template': "cbng/ts"},
        "bearc": {'output_dirs':{"bearc/complex": 32}, 'template': "cbng/sparql"},
    },
    "tb":{
        "beara/high": {'output_dirs':{"beara/high": 58}, 'template': "tb/ts"},
        "beara/low": {'output_dirs':{"beara/low": 58}, 'template': "tb/ts"},
        "bearb/join": {'output_dirs':{"bearb_day/join": 89, "bearb_hour/join": 1299}, 'template': "tb/bgp"},
        "bearb/lookup": {'output_dirs':{"bearb_day/lookup": 89, "bearb_hour/lookup": 1299}, 'template': "tb/ts"},
        "bearc": {'output_dirs':{"bearc/complex": 32}, 'template': "tb/sparql"},
    },
    "tbsf":{
        "beara/high": {'output_dirs':{"beara/high": 58}, 'template': "ic/ts"},
        "beara/low": {'output_dirs':{"beara/low": 58}, 'template': "ic/ts"},
        "bearb/join": {'output_dirs':{"bearb_day/join": 89, "bearb_hour/join": 1299}, 'template': "ic/bgp"},
        "bearb/lookup": {'output_dirs':{"bearb_day/lookup": 89, "bearb_hour/lookup": 1299}, 'template': "ic/ts"},
        "bearc": {'output_dirs':{"bearc/complex": 32}, 'template': "ic/sparql"},
    },
    "tbsh":{
        "beara/high": {'output_dirs':{"beara/high": 58}, 'template': "ic/ts"},
        "beara/low": {'output_dirs':{"beara/low": 58}, 'template': "ic/ts"},
        "bearb/join": {'output_dirs':{"bearb_day/join": 89, "bearb_hour/join": 1299}, 'template': "ic/bgp"},
        "bearb/lookup": {'output_dirs':{"bearb_day/lookup": 89, "bearb_hour/lookup": 1299}, 'template': "ic/ts"},
        "bearc": {'output_dirs':{"bearc/complex": 32}, 'template': "ic/sparql"},
    }
}
LOCAL_TIMEZONE = datetime.now(timezone.utc).astimezone().tzinfo
init_version_timestamp = datetime(2022,10,1,12,0,0,0,LOCAL_TIMEZONE)
vers_ts = init_version_timestamp

# Create queries
for policy in policies:
    for querySet in queries[policy].keys():
        # create directories
        for output_dir, query_versions in queries[policy][querySet]['output_dirs'].items():
            for query_version in range(query_versions):
                query_dir = Path(output_queries_dir + policy + "/queries_" + output_dir + "/" + str(query_version))
                if query_dir.exists():
                    shutil.rmtree(query_dir)
                query_dir.mkdir(parents=True, exist_ok=True)

        # Create query files
        pathToQueries = raw_queries_dir + "queries_" + querySet
        for k, queriesFile in enumerate(os.listdir(raw_queries_dir + "queries_" + querySet)):
            if os.path.isfile(pathToQueries + "/" + queriesFile):
                with open(pathToQueries + "/" + queriesFile, 'r') as file:
                    relativeTempLoc = queries[policy][querySet]['template']
                    print("Create queries for {0} and {1}".format(policy, querySet))
                    if relativeTempLoc.split('/')[1] == 'ts':
                        raw_queries = file.readlines()
                    else:
                        raw_queries = [file.read()]
                    for i, raw_query in enumerate(raw_queries):
                        prefixes, raw_query = split_prefixes_query(raw_query)
                        queryCounter =  i if relativeTempLoc.split('/')[1] == 'ts' else k
                        output_query = ""
                        for output_dir, query_versions in queries[policy][querySet]['output_dirs'].items():
                            for query_version in range(query_versions):
                                with open(os.path.join(sys.path[0]) +"/templates/" + relativeTempLoc + ".txt", 'r') as templateFile:
                                    template = templateFile.read()
                                    if policy == "cbng":
                                        max_version_digits = len(str(query_versions))
                                        output_query = template.format(prefixes, str(query_version).zfill(max_version_digits), raw_query)
                                    else:
                                        output_query = template.format(prefixes, str(query_version), raw_query)
                                    templateFile.close()
                                with open(output_queries_dir + policy + "/queries_" + output_dir + "/" + str(query_version) + "/" + queriesFile.split('.')[0] + "_q" + str(queryCounter) + "_v" + str(query_version) + ".txt", 'w') as output_file:
                                    if policy in ["tbsf", "tbsh"]:
                                        timestamped_output_query = timestamp_query(output_query, vers_ts)
                                        output_file.write(timestamped_output_query[0])
                                        vers_ts = vers_ts + timedelta(seconds=1)
                                    else:
                                        output_file.write(output_query)
                                    output_file.close()
                            vers_ts = init_version_timestamp

                file.close()

