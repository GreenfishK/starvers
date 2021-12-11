import pandas as pd
from pathlib import Path


ic0_ds_path = str(Path.home()) + "/.BEAR/rawdata-bearb/hour/alldata.IC.nt/000001.nt"
ic1_ds_path = str(Path.home()) + "/.BEAR/rawdata-bearb/hour/alldata.IC.nt/000002.nt"

ic0_df = pd.DataFrame({'s': [], 'p': [], 'o': []})
ic0_list = []
with open(ic0_ds_path, "r") as ic0:
    for triple in ic0:
        tr_array = triple[:-2].split(" ", 2)
        ic0_list.append(tr_array)

ic0_df = pd.DataFrame(ic0_list, columns=['s', 'p', 'o'])
print(ic0_df)