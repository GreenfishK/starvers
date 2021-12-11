import pandas as pd
from pathlib import Path


ic0_ds_path = str(Path.home()) + "/.BEAR/rawdata-bearb/hour/alldata.IC.nt/000001.nt"
ic1_ds_path = str(Path.home()) + "/.BEAR/rawdata-bearb/hour/alldata.IC.nt/000002.nt"


def number_of_triples(version: int):
    ic0_ds_path = str(Path.home()) + "/.BEAR/rawdata-bearb/hour/alldata.IC.nt/00{0}.nt".format(str(version).zfill(4))
    ic0_list = []
    with open(ic0_ds_path, "r") as ic0:
        for triple in ic0:
            tr_array = triple[:-2].split(" ", 2)
            if len(tr_array) == 3:
                ic0_list.append(tr_array)

    ic0_df = pd.DataFrame(ic0_list, columns=['s', 'p', 'o'])
    print(len(ic0_df))


number_of_triples(1299)