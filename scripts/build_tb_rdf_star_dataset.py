from pathlib import Path
from datetime import datetime
from datetime import timedelta
import os
import numpy as np
import pandas as pd
from pandas import IndexSlice as idx

valid_from_predicate = "<https://github.com/GreenfishK/DataCitation/versioning/valid_from>"
valid_until_predicate = "<https://github.com/GreenfishK/DataCitation/versioning/valid_until>"
alldata_versioned_path = str(Path.home()) + "/.BEAR/rawdata-bearb/hour/alldata.TB_star.ttl"
sys_ts = datetime.now()

desired_width=320
pd.set_option('display.width', desired_width)
# np.set_printoption(linewidth=desired_width)
pd.set_option('display.max_columns',10)


def annotate_initial_set():
    """
    Annotates all triples with the system timestamp as the start date the 31.12.9999 as the end date.
    :return:
    """

    ic0_ds_path = str(Path.home()) + "/.BEAR/rawdata-bearb/hour/alldata.IC.nt/000001.nt"
    tz_offset = "+02:00"
    sys_ts_formatted = datetime.strftime(sys_ts, "%Y-%m-%dT%H:%M:%S.%f")[:-3]

    with open(ic0_ds_path, "r") as ic0:
        for triple in ic0:
            # Remove dot from statement
            triple_trimmed = triple[:-2]

            ic0_ds_versioned = open(alldata_versioned_path, "a")
            ic0_ds_versioned.write('<<{triple}>> {vers_p} "{ts}{tz_offset}"^^xsd:dateTime .\n'.format(
                triple=triple_trimmed, ts=sys_ts_formatted, vers_p=valid_from_predicate, tz_offset=tz_offset))
            ic0_ds_versioned.write('<<{triple}>> {vers_p} "9999-12-31T00:00:00.000+02:00"^^xsd:dateTime .\n'.format(
                triple=triple_trimmed, vers_p=valid_until_predicate))
            ic0_ds_versioned.close()


def construct_tb_star_ds(cb_rel_path: str):
    """
    :param: cb_rel_path: The name of the directory where the change sets are stored. This is not the absolute
    but only the relative path to "/.BEAR/rawdata-bearb/hour/

    :return:
    """

    """ Annotation of initial version triples """
    ic0_ds_path = str(Path.home()) + "/.BEAR/rawdata-bearb/hour/alldata.IC.nt/000001.nt"
    tz_offset = "+02:00"
    valid_ufn_ts = '9999-12-31T00:00:00.000'
    sys_ts_formatted = datetime.strftime(sys_ts, "%Y-%m-%dT%H:%M:%S.%f")[:-3]

    init_ds = []
    with open(ic0_ds_path, "r") as ic0:
        for triple in ic0:
            # Remove dot from statement
            triple_trimmed = triple[:-2]
            triple_tuple = triple_trimmed.split(" ")
            init_ds.append(["<<", triple_tuple[0], triple_tuple[1], triple_tuple[2],
                            ">>", valid_from_predicate, '"{ts}{tz_offset}"^^xsd:dateTime'.format(
                                                           ts=sys_ts_formatted, tz_offset=tz_offset),
                            ".", "initial"])
            init_ds.append(["<<", triple_tuple[0], triple_tuple[1], triple_tuple[2],
                            ">>", valid_until_predicate, '"{ts}{tz_offset}"^^xsd:dateTime'.format(
                                                           ts=valid_ufn_ts, tz_offset=tz_offset),
                            ".", "initial"])

    df_tb_set = pd.DataFrame(init_ds, columns=['open_pointy_brackets', 's', 'p', 'o', 'closing_pointy_brackets',
                                                    'vers_predicate', 'timestamp', 'dot', 'change_type'])

    """ Loading change set files """
    # build list (version, filename_added, filename_deleted, version_timestamp)
    change_sets_path = str(Path.home()) + "/.BEAR/rawdata-bearb/hour/{0}".format(cb_rel_path)
    new_triples = {}
    deleted_triples = {}
    change_sets = []

    for filename in os.listdir(change_sets_path):
        version = filename.split('-')[2].split('.')[0].zfill(4)

        if filename.startswith("data-added"):
            new_triples[version] = filename
        if filename.startswith("data-deleted"):
            deleted_triples[version] = filename

    vers_ts = sys_ts
    for vers, new_trpls in sorted(new_triples.items()):
        vers_ts = vers_ts + timedelta(seconds=1)
        change_sets.append((vers, new_trpls, deleted_triples[vers], datetime.strftime(vers_ts,
                                                                                      "%Y-%m-%dT%H:%M:%S.%f")[:-3]))

    """ Annotation of change set triples """

    """ Annotate added triples using rdf* syntax """
    for t in change_sets:
        print("Change set between version {0} and {1} processing. ".format(int(t[0])-1, int(t[0])))
        with open(change_sets_path + "/" + t[1], "r") as cs:
            for triple in cs:
                # Remove dot from statement
                triple_trimmed = triple[:-2]
                triple_tuple = triple_trimmed.split(" ")
                df_tb_set.loc[len(df_tb_set)] = ["<<", triple_tuple[0], triple_tuple[1], triple_tuple[2],
                                                           ">>", valid_from_predicate,
                                                           '"{ts}{tz_offset}"^^xsd:dateTime'.format(
                                                               ts=t[3], tz_offset=tz_offset),
                                                           ".", "added"]
                df_tb_set.loc[len(df_tb_set)] = ["<<", triple_tuple[0], triple_tuple[1], triple_tuple[2],
                                                           ">>", valid_until_predicate,
                                                           '"{ts}{tz_offset}"^^xsd:dateTime'.format(
                                                               ts=valid_ufn_ts,
                                                               tz_offset=tz_offset), ".", "added"]

        df_tb_set.set_index(['s', 'p', 'o', 'vers_predicate', 'timestamp'], drop=False, inplace=True)

        """ Annotate deleted triples using rdf* syntax """
        with open(change_sets_path + "/" + t[2], "r") as cs:
            alldata_versioned = open(alldata_versioned_path, "r")
            alldata_versioned_new = alldata_versioned.read()
            for triple in cs:
                # Remove dot from statement
                triple_trimmed = triple[:-2]
                triple_tuple = triple_trimmed.split(" ")
                df_tb_set.loc[(triple_tuple[0], triple_tuple[1], triple_tuple[2],
                                valid_until_predicate,
                                '"{ts}{tz_offset}"^^xsd:dateTime'.format(ts=valid_ufn_ts,
                                                                         tz_offset=tz_offset)), 'timestamp'] = \
                    '"{ts}{tz_offset}"^^xsd:dateTime'.format(ts=t[3], tz_offset=tz_offset)
                # df_tb_set.loc[[triple_tuple[0], triple_tuple[1], triple_tuple[2]], :]['timestamp'] = 'test'

        print("Number of triples: {0}" .format(len(df_tb_set.query('timestamp == \'"{0}{1}"^^xsd:dateTime\''.format(valid_ufn_ts, tz_offset)))))


# annotate_initial_set()
# Take the change sets that were manually computed from the ICs by compute_change_sets.py
construct_tb_star_ds("alldata.CB_computed.nt")
