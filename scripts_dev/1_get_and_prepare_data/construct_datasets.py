from pathlib import Path
from datetime import datetime
from datetime import timedelta, timezone
import os
import pandas as pd
from rdflib import Graph, compare
from enum import Enum
from typing import Union
import re
import sys

desired_width = 320
pd.set_option('display.width', desired_width)
pd.set_option('display.max_columns', 10)


class AnnotationStyle(Enum):
    HIERARCHICAL = 1
    FLAT = 2


def construct_change_sets(dataset_dir: str, end_vers: int, format: str, zf: int):
    """
    end_vers: The last version that should be built. Can only build as many versions as there are snapshots provided
    in the dataset_dir.
    format: ttl or nt.

    """
    print("Constructing changesets.")
    cb_comp_dir = dataset_dir + "/alldata.CB_computed." + format
    if not os.path.exists(cb_comp_dir):
        print("Create directory: " + cb_comp_dir)
        os.makedirs(cb_comp_dir)

    for i in range(1, end_vers):
        print("Calculating changeset between version {0} and {1}".format(i, i+1))
        ic1_ds_path = "{0}/alldata.IC.nt/{1}.nt".format(dataset_dir, str(i).zfill(zf))
        ic2_ds_path = "{0}/alldata.IC.nt/{1}.nt".format(dataset_dir, str(i+1).zfill(zf))

        with open(ic1_ds_path, "r") as ic1_file, open(ic2_ds_path, "r") as ic2_file:
            ic1 = set(ic1_file.read().splitlines())
            ic2 = set(ic2_file.read().splitlines())

        cs_added = ic2.difference(ic1)
        cs_deleted = ic1.difference(ic2)
        assert len(ic2) - len(ic1) == len(cs_added) - len(cs_deleted)

        print("Create data-added_{0}-{1}.nt with {2} triples.".format(i, i + 1, len(cs_added)))
        cs_added_str = "\n".join(triple for triple in cs_added)
        with open(cb_comp_dir + "/" + "data-added_{0}-{1}.{2}".format(i, i + 1, format), "w") as cs_added_file:
            cs_added_file.write(cs_added_str)
        cs_added, cs_added_str = None, None

        print("Create data-deleted_{0}-{1}.nt with {2} triples.".format(i, i + 1, len(cs_deleted)))
        cs_deleted_str = "\n".join(triple for triple in cs_deleted)
        with open(cb_comp_dir + "/" + "data-deleted_{0}-{1}.{2}".format(i, i + 1, format), "w") as cs_deleted_file:
            cs_deleted_file.write(cs_deleted_str)
        cs_deleted, cs_deleted_str = None, None


def construct_tb_star_ds(source_ic0, source_cs: str, destination: str, last_version: int, init_timestamp: datetime,
                         annotation_style: AnnotationStyle = AnnotationStyle.FLAT):
    """
    :param: cb_rel_path: The name of the directory where the change sets are stored. This is not the absolute
    but only the relative path to "/starvers/rawdata/<dataset>/

    :return: initial timestamp. This is only returned for some necessary corrections that need the initial timestamp
    from which one can get to the desired version timestamp.
    """
    print("Constructing RDF-star dataset with the {0} annotation style from ICs and changesets.".format(annotation_style))

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

    # Annotation of initial version triples 
    print("Building version 0. ")

    ic0 = Graph()
    ic0.parse(source_ic0)
    open(destination, "w").write("")
    rdf_star_ds = open(destination, "a")

    if annotation_style == AnnotationStyle.HIERARCHICAL:
        for s, p, o in ic0:
            rdf_star_ds.write("<< << {0} {1} {2} >> {3} {4} >> {5} {6} .\n".format(s.n3(), p.n3(), o.n3(),
            valid_from_predicate, init_ts_res, valid_until_predicate, valid_ufn_ts_res))
        print("Number of data triples: {0}".format(len(ic0)))
    else:
        for s, p, o in ic0:
            rdf_star_ds.write("<< << {0} {1} {2} >> {3} {4} .\n".format(s.n3(), p.n3(), o.n3(),
            valid_from_predicate, init_ts_res))
            rdf_star_ds.write("<< << {0} {1} {2} >> {3} {4} .\n".format(s.n3(), p.n3(), o.n3(),
            valid_until_predicate, valid_ufn_ts_res))
        print("Number of data triples: {0}".format(len(ic0)/2))

    # Load all change set file names into a dict 
    cs_add_files = {}
    cs_del_files = {}

    if not os.path.exists(source_cs):
        print("There are is no changeset directory " + source_cs)
        return

    for filename in os.listdir(source_cs):
        version = filename.split('-')[2].split('.')[0].zfill(4)
        if filename.startswith("data-added"):
            cs_add_files[version] = filename
        if filename.startswith("data-deleted"):
            cs_del_files[version] = filename
    print("{0} change sets are in directory {1}".format(len(cs_add_files), source_cs))

    # Transforming triples/lines from all loaded change sets into their rdf-star representations
    # and write then to the final rdf star dataset 
    vers_ts = init_timestamp
    for vers, cs_add_file in sorted(cs_add_files.items()):#
        vers_ts = vers_ts + timedelta(seconds=1)
        vers_ts_str = '"{ts}{tz_offset}"^^{datetimeref}'.format(
            ts=datetime.strftime(vers_ts, "%Y-%m-%dT%H:%M:%S.%f")[:-3], 
            tz_offset=tz_offset, 
            datetimeref=xsd_datetime)
        if annotation_style == AnnotationStyle.HIERARCHICAL:
            with open(source_cs + "/" + cs_add_file) as cs_add:
                cnt = 0
                for triple in cs_add.readline():
                    cnt = cnt + 1
                    rdf_star_ds.write("<< << {0} >> {1} {2} >> {3} {4} .\n".format(triple, 
                    valid_from_predicate, vers_ts_str, 
                    valid_until_predicate, valid_ufn_ts_res))
                print(source_cs + "/" + cs_add_file + ": " + str(cnt))
            with open(source_cs + "/" + cs_del_files[vers]) as cs_del:
                cnt = 0
                for triple in cs_del.readline():
                    rdf_star_ds.write("<< << {0} >> {1} {2} >> {3} {4} .\n".format(triple, 
                    valid_from_predicate, vers_ts_str, 
                    valid_until_predicate, valid_ufn_ts_res))  
                print(source_cs + "/" + cs_del_files[vers] + ": " + str(cnt))

        else:
            with open(source_cs + "/" + cs_add_file) as cs_add:
                for triple in cs_add.readline():
                    rdf_star_ds.write("<< {0} >> {1} {2} .\n".format(triple, 
                    valid_from_predicate, vers_ts_str))
                    rdf_star_ds.write("<< {0} >> {1} {2} .\n".format(triple, 
                    valid_until_predicate, valid_ufn_ts_res))
            with open(source_cs + "/" + cs_del_files[vers]) as cs_del:
                for triple in cs_del.readline():
                    rdf_star_ds.write("<< {0} >> {1} {2} .\n".format(triple, 
                    valid_from_predicate, vers_ts_str))
                    rdf_star_ds.write("<< {0} >> {1} {2} .\n".format(triple, 
                    valid_until_predicate, valid_ufn_ts_res))        
    rdf_star_ds.close()              


def construct_cbng_ds(source_ic0, source_cs: str, destination: str, last_version: int):
    print("Constructing change-based datasets with the initial IC and changesets as named graphs.")

    def split_prefixes_dataset(dataset: str) -> list:
        """
        Separates the prologue (prefixes at the beginning of the query) from the dataset. 
        If there is no prolog, the prefixes variable will be an empty string.

        :param query: A dataset as string with or without prologue.
        :return: A list with the prefixes as the first element and the actual query string as the second element.
        """
        pattern = "@prefix\\s*([a-zA-Z0-9_-]*):\\s*(<.*>)\\s*\."

        prefixes_list = re.findall(pattern, dataset, re.MULTILINE)
        dataset_without_prefixes = re.sub(pattern, "", dataset)

        return [prefixes_list, dataset_without_prefixes]


    print("Building version {0}. ".format(str(0)))
    cbng_dataset = ""
    prefixes = {}
    ns_cnt = 1
    ic0_raw = open(source_ic0, "r").read()
    sub_prefixes, ic0 = split_prefixes_dataset(ic0_raw)
    max_version_digits = len(str(last_version))

    template = open("/starvers_eval/scripts/1_get_and_prepare_data/templates/cbng.txt", "r").read()
    cbng_dataset = cbng_dataset + template.format(str(0).zfill(max_version_digits), ic0, "")

    # build list (version, filename_added, filename_deleted)
    cs_add_files = {}
    cs_del_files = {}
    change_sets = []

    if not os.path.exists(source_cs):
        os.makedirs(source_cs)

    for filename in os.listdir(source_cs):
        version = int(filename.split('-')[2].split('.')[0].zfill(4)) - 1
        if filename.startswith("data-added"):
            cs_add_files[version] = filename
        if filename.startswith("data-deleted"):
            cs_del_files[version] = filename
    print("{0} change sets are in directory {1}".format(len(cs_add_files), source_cs))

    for vers, cs_add_file in sorted(cs_add_files.items()):
        change_sets.append((vers, cs_add_file, cs_del_files[vers]))

    assert last_version - 1 <= len(change_sets)
    for i, t in enumerate(change_sets[0:last_version-1]):
        print("Building version {0}. ".format(int(t[0])))
        cs_add_raw = open(source_cs + "/" + t[1], "r").read()
        cs_del_raw = open(source_cs + "/" + t[2], "r").read()

        sub_prefixes_add, cs_add = split_prefixes_dataset(cs_add_raw)
        sub_prefixes_del, cs_del = split_prefixes_dataset(cs_del_raw)

        for prefix_iri_tuple in sub_prefixes_add:
            ns = prefix_iri_tuple[0]
            iri = prefix_iri_tuple[1]
            if ns in prefixes.keys():
                new_ns =   "new_ns" + str(ns_cnt)
                # TODO: just for more elegancy: replace with regex by matching the prefix pattern in the data
                cs_add = cs_add.replace(ns + ":", new_ns + ":")
                ns_cnt = ns_cnt + 1
                prefixes[new_ns] = iri
            else:
                prefixes[ns] = iri

        for prefix_iri_tuple in sub_prefixes_del:
            ns = prefix_iri_tuple[0]
            iri = prefix_iri_tuple[1]
            if iri in prefixes.keys():
                new_ns =  "new_ns" + str(ns_cnt)
                # TODO: just for more elegancy: replace with regex by matching the prefix pattern in the data
                cs_add = cs_del.replace(ns + ":", new_ns + ":")
                ns_cnt = ns_cnt + 1
                prefixes[new_ns] = iri
            else:
                prefixes[ns] = iri

        cbng_dataset = cbng_dataset + template.format(str(i+1).zfill(max_version_digits), cs_add, cs_del)

    
    print("Export data set.")
    f = open(destination, "w")
    f.write("\n".join(["@prefix " + key + ":" + value + " ." for key, value in prefixes.items()]) + "\n" + cbng_dataset)
    f.close()


def construct_icng_ds(source: str, destination: str, last_version: int):
    print("Constructing the ICNG dataset with ICs as named graphs.")

    template = open("/starvers_eval/scripts/1_get_and_prepare_data/templates/icng.txt", "r").read()

    if not os.path.exists(source):
        os.makedirs(source)

    print("Create empty dataset.")
    f = open(destination, "w")
    f.write("")
    f.close()

    for i in range(last_version):
        print("Building version {0}. ".format(str(i+1)))
        ic = open(source + "/" + str(i+1).zfill(ic_zfills[dataset])  + ".nt", "r").read()
    
        print("Write ic {} to data set.".format(str(i+1)))
        f = open(destination, "a")
        f.write(template.format(str(i), ic) + "\n")
        f.close()



""" Parameters and function calls """
in_frm = "nt"
LOCAL_TIMEZONE = datetime.now(timezone.utc).astimezone().tzinfo
init_version_timestamp = datetime(2022,10,1,12,0,0,0,LOCAL_TIMEZONE)
datasets_cmd = sys.argv[1]
datasets = datasets_cmd.split(" ")
dataset_versions = {'beara':58, 'bearb_day':89, 'bearb_hour':1299, 'bearc':32}
ic_zfills = {'beara': 1, 'bearb_hour': 6, 'bearb_day': 6, 'bearc': 1}

for dataset in datasets:
    if dataset not in ['beara', 'bearb_day', 'bearb_hour', 'bearc']:
        print("Dataset must be one of: ", ['beara', 'bearb_day', 'bearb_hour', 'bearc'])
        break

    data_dir = "/starvers_eval/rawdata/" + dataset
    total_versions = dataset_versions[dataset]
    print("Constructing datasets for {0}".format(dataset))

    # CB
    construct_change_sets(dataset_dir=data_dir, end_vers=total_versions, format=in_frm, zf=ic_zfills[dataset])

    # TBSH
    construct_tb_star_ds(source_ic0=data_dir + "/alldata.IC.nt/" + "1".zfill(ic_zfills[dataset])  + ".nt",
                        source_cs=data_dir + "/alldata.CB_computed." + in_frm,
                        destination=data_dir + "/alldata.TB_star_hierarchical" + ".ttls",
                        last_version=total_versions,
                        init_timestamp=init_version_timestamp,
                        annotation_style=AnnotationStyle.HIERARCHICAL)
    
    # TBSF
    # construct_tb_star_ds(source_ic0=data_dir + "/alldata.IC.nt/" + "1".zfill(ic_zfills[dataset])  + ".nt",
    #                    source_cs=data_dir + "/alldata.CB_computed." + in_frm,
    #                    destination=data_dir + "/alldata.TB_star_flat." + ".ttls",
    #                    last_version=total_versions,
    #                    init_timestamp=init_version_timestamp,
    #                    annotation_style=AnnotationStyle.FLAT)
    
    # CBNG
    construct_cbng_ds(source_ic0=data_dir + "/alldata.IC.nt/" + "1".zfill(ic_zfills[dataset])  + ".nt",
                      source_cs=data_dir + "/alldata.CB_computed." + in_frm,
                      destination=data_dir + "/alldata.CBNG.trig",
                      last_version=total_versions)

    # ICNG
    construct_icng_ds(source=data_dir + "/alldata.IC.nt",
                      destination=data_dir + "/alldata.ICNG.trig",
                      last_version=total_versions)
