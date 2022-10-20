from pathlib import Path
from datetime import datetime
from datetime import timedelta, timezone
import os
import pandas as pd
from rdflib import Graph
from enum import Enum
from typing import Union

import data_corrections

desired_width = 320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', 10)


class AnnotationStyle(Enum):
    HIERARCHICAL = 1
    FLAT = 2


def diff_set(dataset_dir: str, version1: int, version2: int, format: str, zf: int) -> Union[Graph, Graph]:
    print("Calculating changeset between version {0} and {1}".format(version1, version2))
    ic1_ds_path = "{0}/alldata.IC.nt/{1}.nt".format(dataset_dir, str(version1).zfill(zf))
    ic2_ds_path = "{0}/alldata.IC.nt/{1}.nt".format(dataset_dir, str(version2).zfill(zf))

    ic1 = Graph()
    ic1.parse(ic1_ds_path, format=format)
    ic2 = Graph()
    ic2.parse(ic2_ds_path, format=format)

    cs_add = Graph()
    cs_add.parse(ic2_ds_path, format=format)
    cs_add.__isub__(ic1)

    cs_del = Graph()
    cs_del.parse(ic1_ds_path, format=format)
    cs_del.__isub__(ic2)

    return cs_add, cs_del


def construct_change_sets(dataset_dir: str, end_vers: int, format: str, zf: int):
    """
    end_vers: The last version that should be built. Can only build as many versions as there are snapshots provided
    in the dataset_dir.
    format: ttl or nt.

    """
    cb_comp_dir = dataset_dir + "/alldata.CB_computed." + format
    if not os.path.exists(cb_comp_dir):
        os.makedirs(cb_comp_dir)

    for i in range(1, end_vers):
        output = diff_set(dataset_dir, i, i + 1, format, zf)
        cs_added = output[0]
        assert isinstance(cs_added, Graph)
        cs_deleted = output[1]
        assert isinstance(cs_deleted, Graph)

        print("Create data-added_{0}-{1}.nt with {2} triples.".format(i, i + 1, len(cs_added)))
        cs_added.serialize(destination=cb_comp_dir + "/" + "data-added_{0}-{1}.{2}".format(i, i + 1, format),
                           format=format)
        print("Create data-deleted_{0}-{1}.nt with {2} triples.".format(i, i + 1, len(cs_deleted)))
        cs_deleted.serialize(destination=cb_comp_dir + "/" + "data-deleted_{0}-{1}.{2}".format(i, i + 1, format),
                             format=format)


def construct_tb_star_ds(source_ic0, source_cs: str, destination: str, last_version: int, init_timestamp: datetime,
                         annotation_style: AnnotationStyle = AnnotationStyle.FLAT):
    """
    :param: cb_rel_path: The name of the directory where the change sets are stored. This is not the absolute
    but only the relative path to "/.BEAR/rawdata/<dataset>/

    :return: initial timestamp. This is only returned for some necessary corrections that need the initial timestamp
    from which one can get to the desired version timestamp.
    """

    valid_from_predicate = "<https://github.com/GreenfishK/DataCitation/versioning/valid_from>"
    valid_until_predicate = "<https://github.com/GreenfishK/DataCitation/versioning/valid_until>"
    xsd_datetime = "<http://www.w3.org/2001/XMLSchema#dateTime>"
    tz_offset = "+02:00"
    valid_ufn_ts_res = '"9999-12-31T00:00:00.000{tz_offset}"^^{datetimeref}'.format(tz_offset=tz_offset,
                                                                                    datetimeref=xsd_datetime)
    # sys_ts = datetime.now()
    sys_ts_formatted = datetime.strftime(init_timestamp, "%Y-%m-%dT%H:%M:%S.%f")[:-3]
    init_ts_res = '"{ts}{tz_offset}"^^{datetimeref}'.format(ts=sys_ts_formatted, tz_offset=tz_offset,
                                                            datetimeref=xsd_datetime)

    """ Annotation of initial version triples """
    print("Building version 1. ")

    ic0 = Graph()
    ic0.parse(source_ic0)

    init_ds = []
    if annotation_style == AnnotationStyle.HIERARCHICAL:
        for s, p, o in ic0:
            init_ds.append([s.n3(), p.n3(), o.n3(), valid_from_predicate, init_ts_res,
                            valid_until_predicate, valid_ufn_ts_res])
        df_tb_set = pd.DataFrame(init_ds, columns=['s', 'p', 'o', 'valid_from_predicate', 'valid_from_timestamp',
                                                   'valid_until_predicate', 'valid_until_timestamp'])
        print("Number of data triples: {0}".format(
            len(df_tb_set.query("valid_until_timestamp == '{0}'".format(valid_ufn_ts_res)))))
    else:
        assert(annotation_style == AnnotationStyle.FLAT)
        for s, p, o in ic0:
            init_ds.append([s.n3(), p.n3(), o.n3(), valid_from_predicate, init_ts_res])
            init_ds.append([s.n3(), p.n3(), o.n3(), valid_until_predicate, valid_ufn_ts_res])
        df_tb_set = pd.DataFrame(init_ds, columns=['s', 'p', 'o', 'vers_predicate', 'timestamp'])
        print("Number of data triples: {0}".format(
            len(df_tb_set.query("timestamp == '{0}'".format(valid_ufn_ts_res)))))

    """ Loading change set files """
    # build list (version, filename_added, filename_deleted, version_timestamp)
    new_triples = {}
    deleted_triples = {}
    change_sets = []

    if not os.path.exists(source_cs):
        os.makedirs(source_cs)

    for filename in os.listdir(source_cs):
        version = filename.split('-')[2].split('.')[0].zfill(4)
        if filename.startswith("data-added"):
            new_triples[version] = filename
        if filename.startswith("data-deleted"):
            deleted_triples[version] = filename
    print("{0} change sets are in directory {1}".format(len(new_triples), source_cs))

    vers_ts = init_timestamp
    for vers, new_trpls in sorted(new_triples.items()):
        vers_ts = vers_ts + timedelta(seconds=1)
        change_sets.append((vers, new_trpls, deleted_triples[vers], datetime.strftime(vers_ts,
                                                                                      "%Y-%m-%dT%H:%M:%S.%f")[:-3]))

    """ Annotation of change set triples """
    assert last_version - 1 <= len(change_sets)
    for t in change_sets[0:last_version-1]:
        print("Building version {0}. ".format(int(t[0])))
        
        """ Annotate added triples using rdf* syntax """
        cs_add = Graph()
        cs_add.parse(source_cs + "/" + t[1])
        valid_from_ts_res = '"{ts}{tz_offset}"^^{datetimeref}'.format(ts=t[3], tz_offset=tz_offset,
                                                                      datetimeref=xsd_datetime)
        for s, p, o in cs_add:
            if annotation_style == AnnotationStyle.FLAT:
                df_tb_set.loc[len(df_tb_set)] = [s.n3(), p.n3(), o.n3(), valid_from_predicate, valid_from_ts_res]
                df_tb_set.loc[len(df_tb_set)] = [s.n3(), p.n3(), o.n3(), valid_until_predicate, valid_ufn_ts_res]
            if annotation_style == AnnotationStyle.HIERARCHICAL:
                df_tb_set.loc[len(df_tb_set)] = [s.n3(), p.n3(), o.n3(), valid_from_predicate, valid_from_ts_res,
                                                 valid_until_predicate, valid_ufn_ts_res]

        """ Annotate deleted triples using rdf* syntax """
        cs_del = Graph()
        cs_del.parse(source_cs + "/" + t[2])
        if annotation_style == AnnotationStyle.FLAT:
            df_tb_set.set_index(['s', 'p', 'o', 'vers_predicate'], drop=False, inplace=True)
            for s, p, o in cs_del:
                df_tb_set.loc[(s.n3(), p.n3(), o.n3(), valid_until_predicate), 'timestamp'] = valid_from_ts_res
            print("Number of data triples: {0}".format(len(df_tb_set.query("timestamp == '{0}'".format(valid_ufn_ts_res)))))
        if annotation_style == AnnotationStyle.HIERARCHICAL:
            df_tb_set.set_index(['s', 'p', 'o'], drop=False, inplace=True)
            for s, p, o in cs_del:
                df_tb_set.loc[(s.n3(), p.n3(), o.n3()), 'valid_until_timestamp'] = valid_from_ts_res
            print("Number of data triples: {0}".format(
                len(df_tb_set.query("valid_until_timestamp == '{0}'".format(valid_ufn_ts_res)))))

    """ Export dataset by reading out each line. Pandas does so far not provide any function 
    to serialize in ttl oder n3 format"""
    print("Export data set.")
    f = open(destination, "w")
    f.write("")
    f.close()
    with open(destination, "a") as output_tb_ds:
        if annotation_style == AnnotationStyle.FLAT:
            for index, row in df_tb_set.iterrows():
                triple = "{0} {1} {2}".format(row['s'], row['p'], row['o'])
                output_tb_ds.write("<<{triple}>> {vers_p} {ts} .\n".format(triple=triple,
                                                                           vers_p=row['vers_predicate'],
                                                                           ts=row['timestamp']))
        if annotation_style == AnnotationStyle.HIERARCHICAL:
            for index, row in df_tb_set.iterrows():
                triple = "{0} {1} {2}".format(row['s'], row['p'], row['o'])
                output_tb_ds.write("<<<<{triple}>> {vf_ts_p} {vf_ts}>>"
                                   " {vu_ts_p} {vu_ts} .\n".format(triple=triple,
                                                                   vf_ts_p=row['valid_from_predicate'],
                                                                   vf_ts=row['valid_from_timestamp'],
                                                                   vu_ts_p=row['valid_until_predicate'],
                                                                   vu_ts=row['valid_until_timestamp']))
        output_tb_ds.close()
        return init_timestamp


""" Parameters and function calls """
in_frm = "ttl"
out_frm = "ttl"
LOCAL_TIMEZONE = datetime.now(timezone.utc).astimezone().tzinfo
init_version_timestamp = datetime(2022,10,1,12,0,0,0,LOCAL_TIMEZONE)

datasets = {'bearc': 32} #'beara':58, 'bearb-day':89, 'bearb-hour':1299, 
ic_zfills = {'beara': 1, 'bearb-hour': 6, 'bearb-day': 6, 'bearc': 1}

for dataset, totalVersions in datasets.items():
    data_dir = str(Path.home()) + "/.BEAR/rawdata/" + dataset

    construct_change_sets(dataset_dir=data_dir, end_vers=totalVersions, format=out_frm, zf=ic_zfills[dataset])
    construct_tb_star_ds(source_ic0=data_dir + "/alldata.IC.nt/" + "1".zfill(ic_zfills[dataset])  + ".nt",
                        source_cs=data_dir + "/alldata.CB_computed." + in_frm,
                        destination=data_dir + "/alldata.TB_star_hierarchical." + out_frm,
                        last_version=totalVersions,
                        init_timestamp=init_version_timestamp,
                        annotation_style=AnnotationStyle.HIERARCHICAL)
    construct_tb_star_ds(source_ic0=data_dir + "/alldata.IC.nt/" + "1".zfill(ic_zfills[dataset])  + ".nt",
                        source_cs=data_dir + "/alldata.CB_computed." + in_frm,
                        destination=data_dir + "/alldata.TB_star_flat." + out_frm,
                        last_version=totalVersions,
                        init_timestamp=init_version_timestamp,
                        annotation_style=AnnotationStyle.FLAT)
    if dataset == 'bearb-hour':
        data_corrections.correct("rdf_star_hierarchical", data_dir + "/alldata.TB_star_hierarchical." + out_frm, init_ts=init_version_timestamp)
        data_corrections.correct("rdf_star_flat", data_dir + "/alldata.TB_star_flat." + out_frm, init_ts=init_version_timestamp)

