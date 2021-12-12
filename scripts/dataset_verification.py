import pandas as pd
from pathlib import Path

desired_width = 600
pd.set_option('display.width', desired_width)
pd.set_option("display.max_columns", 10)


def number_of_triples(version: int):
    ic0_ds_path = str(Path.home()) + "/.BEAR/rawdata-bearb/hour/alldata.IC.nt/00{0}.nt".format(str(version).zfill(4))
    ic0_list = []
    with open(ic0_ds_path, "r") as ic0:
        for triple in ic0:
            tr_array = triple[:-2].split(" ", 2)
            if len(tr_array) == 3:
                ic0_list.append(tr_array)

    ic0_df = pd.DataFrame(ic0_list, columns=['s', 'p', 'o'])
    return len(ic0_df)


def cb_to_df(version: int, added_or_deleted: str = "added"):
    """
    version: the actual version. E.g. for triples added between v1 and v2 (data-added_1-2.nt) 2 would
    be the parameter value.
    """

    version_prev = version - 1
    if version == 1:
        return "This is the initial version. There is no change set previous to this version. Choose are version " \
               "higher than 1 and lower than 1300."
    cb0_ds_path = str(Path.home()) + "/.BEAR/rawdata-bearb/hour/alldata.CB.nt/data-{0}_{1}-{2}.nt".\
        format(added_or_deleted, version_prev, version)
    cb0_list = []
    with open(cb0_ds_path, "r") as ic0:
        for triple in ic0:
            tr_array = triple[:-2].split(" ", 2)
            if len(tr_array) == 3:
                cb0_list.append(tr_array)

    cb0_df = pd.DataFrame(cb0_list, columns=['s', 'p', 'o'])
    return cb0_df


def ic_to_df(version: int):
    ic0_ds_path = str(Path.home()) + "/.BEAR/rawdata-bearb/hour/alldata.IC.nt/00{0}.nt".format(str(version).zfill(4))
    ic0_list = []
    with open(ic0_ds_path, "r") as ic0:
        for triple in ic0:
            tr_array = triple[:-2].split(" ", 2)
            if len(tr_array) == 3:
                ic0_list.append(tr_array)

    ic0_df = pd.DataFrame(ic0_list, columns=['s', 'p', 'o'])
    return ic0_df


def print_stats(version: int):
    print("Number of triples in IC version {0}: {1}".format(version, len(ic_to_df(version))))
    print("Number of triples in previous IC version {0}: {1}".format(version-1, len(ic_to_df(version-1))))
    print("Number of added triples in version {0} compared to previous version: {1}".
          format(version, len(cb_to_df(version))))
    print("Number of deleted triples in version {0} compared to previous version: {1}".
          format(version, len(cb_to_df(version, "deleted"))))

    check_flag = False
    if len(ic_to_df(version-1)) + len(cb_to_df(version)) - len(cb_to_df(version, "deleted")) == len(ic_to_df(version)):
        check_flag = True

    print("Check whether the change numbers reflect the difference between two ICs: {0} + {1} - {2} = {3}: Equation {4}".
          format(len(ic_to_df(version-1)), len(cb_to_df(version)), len(cb_to_df(version, "deleted")),
                 len(ic_to_df(version)), check_flag))

    df1 = ic_to_df(version-1)
    cb_add = cb_to_df(version)
    cb_del = cb_to_df(version, "deleted")
    df_diff1 = cb_add.merge(cb_del, on=['s', 'p', 'o'], how="inner")
    df1 = df1.merge(cb_add, on=['s', 'p', 'o'], how="inner")
    assert len(df1) == len(df_diff1)

    print("Number of triples that have been deleted and added again (or vice versa) "
          "in version {0} compared to previous version: {1}".format(version, len(df_diff)))


print("Verify that the number of triples in version 1 is 33502")
assert number_of_triples(1) == 33502

print("Verify that the number of triples in version 1299 is 43907")
assert number_of_triples(1299) == 43907

print("Verify that all added triples between version 1 and 2 are included in version 2")
v2_df = ic_to_df(2)
v1_v2_cb = cb_to_df(2)
df = v2_df.merge(v1_v2_cb, on=['s', 'p', 'o'], how="inner")
assert len(df) == len(v1_v2_cb)


print("Verify that all triples that are included in version 1 and version 2 are also reflected in the "
      "intersection of the added and deleted change sets. Thus, they must have been deleted and then added again.")
# There are some triples that have been deleted and then added again between version 1 and 2
v1_df = ic_to_df(1)
v1_v2_cb = cb_to_df(2)
v1_v2_cb_del = cb_to_df(2, "deleted")
df_diff = v1_v2_cb.merge(v1_v2_cb_del, on=['s', 'p', 'o'], how="inner")
df = v1_df.merge(v1_v2_cb, on=['s', 'p', 'o'], how="inner")
assert len(df) == len(df_diff)


print("Verify that the deleted triples between version 1 and version 2 are included in version 1")
v1_df = ic_to_df(1)
v1_v2_cb_del = cb_to_df(2, "deleted")
df = v1_df.merge(v1_v2_cb_del, on=['s', 'p', 'o'], how="inner")
assert len(df) == len(v1_v2_cb_del)

# diff set between v1 and v2
# v1_df = ic_to_df(1)
# v2_df = ic_to_df(2)
# df_diff = pd.concat([v1_df, v2_df]).drop_duplicates(keep=False)
# df_1 = df_diff.merge(v1_v2_cb, on=['s', 'p', 'o'], how="inner")
# df_2 = df_diff.merge(v1_v2_cb_del, on=['s', 'p', 'o'], how="inner")

print()
print_stats(6)