import csv
import pandas as pd
from pathlib import Path
import os
from rdflib import Graph


def diff_set(version1: int, version2: int):
    ic1_ds_path = str(Path.home()) + "/.BEAR/rawdata-bearb/hour/alldata.IC.nt/00{0}.nt".format(str(version1).zfill(4))
    ic2_ds_path = str(Path.home()) + "/.BEAR/rawdata-bearb/hour/alldata.IC.nt/00{0}.nt".format(str(version2).zfill(4))

    ic1 = Graph()
    ic1.parse(ic1_ds_path)
    ic1_list = []
    for s, p, o in ic1:
        ic1_list.append([s.n3(), p.n3(), o.n3()])
    ic1_df = pd.DataFrame(ic1_list, columns=['s1', 'p1', 'o1'])

    ic2 = Graph()
    ic2.parse(ic2_ds_path)
    ic2_list = []
    for s, p, o in ic2:
        ic2_list.append([s.n3(), p.n3(), o.n3()])
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

for i in range(1, 1299):
    output = diff_set(i, i+1)
    cs_added = output.query("change_type == 'a'")
    assert isinstance(cs_added, pd.DataFrame)
    cs_deleted = output.query("change_type == 'd'")
    assert isinstance(cs_deleted, pd.DataFrame)

    print("Create and load data-added_{0}-{1}.nt".format(i, i+1))
    f = open(cb_comp_dir + "/" + "data-added_{0}-{1}.nt".format(i, i+1), "w")
    f.write("")
    f.close()
    with open(cb_comp_dir + "/" + "data-added_{0}-{1}.nt".format(i, i+1), "a") as output_tb_ds:
        for index, row in cs_added.iterrows():
            output_tb_ds.write("{0} {1} {2} .\n".format(row['s1'], row['p1'], row['o1']))
        output_tb_ds.close()

    print("Create and load data-deleted_{0}-{1}.nt".format(i, i+1))
    f = open(cb_comp_dir + "/" + "data-deleted_{0}-{1}.nt".format(i, i+1), "w")
    f.write("")
    f.close()
    with open(cb_comp_dir + "/" + "data-deleted_{0}-{1}.nt".format(i, i+1), "a") as output_tb_ds:
        for index, row in cs_deleted.iterrows():
            output_tb_ds.write("{0} {1} {2} .\n".format(row['s1'], row['p1'], row['o1']))
        output_tb_ds.close()

