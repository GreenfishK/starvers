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
import re
import csv

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

        cs_added_str = "\n".join(triple for triple in cs_added if not triple.startswith("#"))
        print("Create data-added_{0}-{1}.nt with {2} triples.".format(i, i + 1, cs_added_str.count("\n") + 1))
        with open(cb_comp_dir + "/" + "data-added_{0}-{1}.{2}".format(i, i + 1, format), "w") as cs_added_file:
            cs_added_file.write(cs_added_str)
        cs_added, cs_added_str = None, None

        cs_deleted_str = "\n".join(triple for triple in cs_deleted if not triple.startswith("#"))
        print("Create data-deleted_{0}-{1}.nt with {2} triples.".format(i, i + 1, cs_deleted_str.count("\n") + 1))
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
    sys_ts_formatted = datetime.strftime(init_timestamp, "%Y-%m-%dT%H:%M:%S.%f")[:-3]
    init_ts_res = '"{ts}{tz_offset}"^^{datetimeref}'.format(ts=sys_ts_formatted, tz_offset=tz_offset,
                                                            datetimeref=xsd_datetime)

    print("Write initial snapshot {0} to final dataset ".format(source_ic0))
    result_set = []

    # transform all triples in the list to their starvers RDF-star representations
    added_triples_raw = open(source_ic0, "r").read().splitlines()
    added_triples_raw = list(filter(None, added_triples_raw))
    result_set += list(map(list, zip(["<< <<"] * len(added_triples_raw),
                             added_triples_raw, 
                             [">>"] * len(added_triples_raw), 
                             [valid_from_predicate] * len(added_triples_raw),
                             [init_ts_res] * len(added_triples_raw), 
                             [">>"] * len(added_triples_raw),
                             [valid_until_predicate] * len(added_triples_raw),
                             [valid_ufn_ts_res] * len(added_triples_raw),
                             ['.'] * len(added_triples_raw))))
    for filename in sorted(os.listdir(source_cs)):
        version = filename.split('-')[1][-1:] #.zfill(4)
        vers_ts = init_timestamp + timedelta(seconds=int(version))
        vers_ts_str = '"{ts}{tz_offset}"^^{datetimeref}'.format(ts=datetime.strftime(vers_ts, "%Y-%m-%dT%H:%M:%S.%f")[:-3], tz_offset=tz_offset, datetimeref=xsd_datetime)            
        
        if filename.startswith("data-added"):
            print("Read changeset {0} from filesystem and add it to the result set.".format(filename))
            added_triples_raw = open(source_cs + "/" + filename, "r").read().splitlines()
            added_triples_raw = list(filter(None, added_triples_raw))
            result_set += list(map(list, zip(["<< <<"] * len(added_triples_raw),
                                      added_triples_raw, 
                                      [">>"] * len(added_triples_raw), 
                                      [valid_from_predicate] * len(added_triples_raw),
                                      [vers_ts_str] * len(added_triples_raw), 
                                      [">>"] * len(added_triples_raw),
                                      [valid_until_predicate] * len(added_triples_raw),
                                      [valid_ufn_ts_res] * len(added_triples_raw),
                                      ['.'] * len(added_triples_raw))))
        if filename.startswith("data-deleted"):
            print("Read changeset {0} from filesystem and remove all the triples from the result set that match with the triples in {0}.".format(filename))
            deleted_triples_raw = open(source_cs + "/" + filename, "r").read().splitlines()
            for i, triple in enumerate(result_set):
                if len(deleted_triples_raw) == 0:
                    break
                if triple[1] == deleted_triples_raw[0] and triple[7] == valid_ufn_ts_res:
                    result_set[i][7] = vers_ts_str
                    deleted_triples_raw.pop(0)
        
    print("Write result string to file.")
    rdf_star_ds_str = ""
    for rdf_star_triple_list in result_set:
        assert rdf_star_triple_list[1][-2:] == " ."
        rdf_star_triple_list[1] = rdf_star_triple_list[1][:-2]
        rdf_star_ds_str += " ".join(rdf_star_triple_list) + "\n"
    with open(destination, "w") as rdf_star_ds_file:
        rdf_star_ds_file.write(rdf_star_ds_str)

  
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
dataset_versions = {'beara':58, 'bearb_day':89, 'bearb_hour':1299, 'bearc':32, 'beart': 4}
ic_zfills = {'beara': 1, 'bearb_hour': 6, 'bearb_day': 6, 'bearc': 1, 'beart': 6}

for dataset in datasets:
    if dataset not in ['beara', 'bearb_day', 'bearb_hour', 'bearc', 'beart']:
        print("Dataset must be one of: ", ['beara', 'bearb_day', 'bearb_hour', 'bearc', 'beart'])
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
