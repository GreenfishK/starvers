import csv

import pandas as pd
from pathlib import Path
import os


def diff_set(version1: int, version2: int):
    ic1_ds_path = str(Path.home()) + "/.BEAR/rawdata-bearb/hour/alldata.IC.nt/00{0}.nt".format(str(version1).zfill(4))
    ic2_ds_path = str(Path.home()) + "/.BEAR/rawdata-bearb/hour/alldata.IC.nt/00{0}.nt".format(str(version2).zfill(4))

    ic1_list = []
    with open(ic1_ds_path, "r") as ic0:
        for triple in ic0:
            tr_array = triple[:-2].split(" ", 2)
            if len(tr_array) == 3:
                ic1_list.append(tr_array)

    ic1_df = pd.DataFrame(ic1_list, columns=['s1', 'p1', 'o1'])

    ic2_list = []
    with open(ic2_ds_path, "r") as ic0:
        for triple in ic0:
            tr_array = triple[:-2].split(" ", 2)
            if len(tr_array) == 3:
                ic2_list.append(tr_array)

    ic2_df = pd.DataFrame(ic2_list, columns=['s1', 'p1', 'o1'])

    df_del = ic1_df.merge(ic2_df, indicator='i', how='left').query('i == "left_only"').drop('i', 1)
    df_del['change_type'] = 'd'
    df_add = ic1_df.merge(ic2_df, indicator='i', how='right').query('i == "right_only"').drop('i', 1)
    df_add['change_type'] = 'a'
    df = pd.concat([df_del, df_add])
    df['dot'] = "."
    return df


cb_comp_dir = str(Path.home()) + "/.BEAR/rawdata-bearb/hour/alldata.CB_computed.nt"
if not os.path.exists(cb_comp_dir):
    os.makedirs(cb_comp_dir)

for i in range(1, 1300):
    output = diff_set(i, i+1)
    cs_added = output.query("change_type == 'a'")
    assert isinstance(cs_added, pd.DataFrame)
    cs_deleted = output.query("change_type == 'd'")
    assert isinstance(cs_deleted, pd.DataFrame)

    cs_added[['s1', 'p1', 'o1', 'dot']].to_csv(cb_comp_dir + "/" + "data-added_{0}-{1}.nt".format(i, i+1),
                                               index=False, sep=" ", header=False, quoting=csv.QUOTE_NONE,
                                               quotechar="", escapechar=' ')
    cs_deleted[['s1', 'p1', 'o1', 'dot']].to_csv(cb_comp_dir + "/" + "data-deleted_{0}-{1}.nt".format(i, i+1),
                                                 index=False, sep=" ", header=False, quoting=csv.QUOTE_NONE,
                                                 quotechar="", escapechar=' ')

