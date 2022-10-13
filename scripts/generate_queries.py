import starvers.starvers
from pathlib import Path

raw_queries_dir=str(Path.home()) + "/.BEAR/queries/raw_queries/"
output_queries_dir=str(Path.home()) + "/.BEAR/queries/final_queries/"
datasets={
    "beara/high":58,"beara/low":58,
    "bearb/join":1299,"bearb/lookup":1299,
    "bearc":32}
policies=["ic","cb", "tb", "tbsf", "tbsh"]
input_representations=["ts", "bgp", "sparql"]

# Create directories
for policy in policies:
    for dataset in datasets.keys():
        Path(output_queries_dir + str.upper(policy) + "/queries_" + dataset).mkdir(parents=True, exist_ok=True)


# Create queries

