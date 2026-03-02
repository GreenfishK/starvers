from datetime import datetime,  timedelta, timezone
import os
from enum import Enum
import sys
import re
import numpy as np
import time
import logging
import subprocess
import shlex
import pandas as pd
import shutil
from SPARQLWrapper import SPARQLWrapper, JSON
from rdflib.term import URIRef
import tomli
from itertools import product, takewhile
from functools import partial
from urllib.error import HTTPError
import psutil

from starvers.starvers import TripleStoreEngine
from starvers._helper import versioning_timestamp_format

##########################################################################################
# Logging 
##########################################################################################
if not os.path.exists('/starvers_eval/output/logs/construct_datasets'):
    os.makedirs('/starvers_eval/output/logs/construct_datasets')
with open('/starvers_eval/output/logs/construct_datasets/construct_datasets.txt', "w") as log_file:
    log_file.write("")
logging.basicConfig(handlers=[logging.FileHandler(filename="/starvers_eval/output/logs/construct_datasets/construct_datasets.txt", 
                                                  encoding='utf-8', mode='a+')],
                    format="%(asctime)s %(name)s:%(levelname)s:%(message)s", 
                    datefmt="%F %A %T", 
                    level=logging.INFO)

LOG_FILE = "/starvers_eval/output/logs/construct_datasets/construct_datasets.txt"
##########################################################################################
# Parameters 
##########################################################################################
datasets = sys.argv[1].split(" ")
skip_change_sets = sys.argv[2]
skip_tb_star_ds = sys.argv[3]
skip_cbng_ds = sys.argv[4]
skip_icng_ds = sys.argv[5]
skip_update_measurement = sys.argv[6]

CONFIG_TMPL_DIR="/starvers_eval/scripts/3_construct_datasets/configs"
CONFIG_DIR="/starvers_eval/configs/construct_datasets"

in_frm = "nt"
LOCAL_TIMEZONE = datetime.now(timezone.utc).astimezone().tzinfo
init_version_timestamp = datetime(2022,10,1,12,0,0,0,LOCAL_TIMEZONE)

with open("/starvers_eval/configs/eval_setup.toml", mode="rb") as config_file:
    eval_setup = tomli.load(config_file)

dataset_versions = {dataset: infos['snapshot_versions'] for dataset, infos in eval_setup['datasets'].items()}
ic_basename_lengths = {dataset: infos['ic_basename_length'] for dataset, infos in eval_setup['datasets'].items()}
allowed_datasets = list(dataset_versions.keys())
snapshot_dir = eval_setup['general']['snapshot_dir']
change_sets_dir = eval_setup['general']['change_sets_dir']
databases_dir = "/starvers_eval/databases/construct_datasets"

class TripleStore(Enum):
    GRAPHDB = 1
    JENATDB2 = 2
   

def construct_change_sets(snapshots_dir: str, change_sets_dir: str, end_vers: int, format: str, basename_length: int):
    """
    end_vers: The last version that should be built. Can only build as many versions as there are snapshots provided
    in the dataset_dir.
    format: ttl or nt.

    """

    logging.info("Constructing changesets.")
    cb_comp_dir = f"{change_sets_dir}.{format}"
    if not os.path.exists(cb_comp_dir):
        logging.info("Create directory: " + cb_comp_dir)
        os.makedirs(cb_comp_dir)

    cnt_net_triples_added = 0    
    cnt_triples_rdf_star = 0
    cnt_valid_triples_last_ic = 0

    for i in range(1, end_vers):
        ic1_ds_path = "{0}/{1}.nt".format(snapshots_dir, str(i).zfill(basename_length))
        ic2_ds_path = "{0}/{1}.nt".format(snapshots_dir, str(i+1).zfill(basename_length))
        logging.info("Calculating changesets between snapshots {0}.nt and {1}.nt".format(str(i).zfill(basename_length), str(i+1).zfill(basename_length)))


        with open(ic1_ds_path, "r") as ic1_file, open(ic2_ds_path, "r") as ic2_file:
            ic1 = ic1_file.read().splitlines()
            ic2 = ic2_file.read().splitlines()
        
        ic1 = set([line for line in ic1 if not (line.startswith("#") or len(line) == 0)])
        ic2 = set([line for line in ic2 if not (line.startswith("#") or len(line) == 0)])

        cs_added = ic2.difference(ic1)
        cs_deleted = ic1.difference(ic2)
        assert len(ic2) - len(ic1) == len(cs_added) - len(cs_deleted)

        cs_added_str = "\n".join(triple for triple in cs_added) 
        cnt_net_triples_added += len(cs_added)
        cnt_triples_rdf_star += len(cs_added) + (len(ic1) if i == 1 else 0)
        cnt_valid_triples_last_ic = len(ic2) if i == end_vers - 1 else 0
        logging.info("Create data-added_{0}-{1}.nt with {2} triples.".format(i, i + 1, len(cs_added)))
        with open(cb_comp_dir + "/" + "data-added_{0}-{1}.{2}".format(i, i + 1, format), "w") as cs_added_file:
            cs_added_file.write(cs_added_str)
        cs_added, cs_added_str = None, None

        cs_deleted_str = "\n".join(triple for triple in cs_deleted)
        cnt_net_triples_added -= len(cs_deleted)
        logging.info("Create data-deleted_{0}-{1}.nt with {2} triples.".format(i, i + 1, len(cs_deleted)))
        with open(cb_comp_dir + "/" + "data-deleted_{0}-{1}.{2}".format(i, i + 1, format), "w") as cs_deleted_file:
            cs_deleted_file.write(cs_deleted_str)
        cs_deleted, cs_deleted_str = None, None
    
    logging.info("Assertion: From the first to the last snapshot {1} triples were added (net)".format(end_vers, cnt_net_triples_added))        
    logging.info("Assertion: The rdf-star dataset created with function construct_tb_star_ds should have {1} triples".format(end_vers, cnt_triples_rdf_star))
    logging.info("Assertion: Triples that are still valid with the latest snapshot: {0}".format(cnt_valid_triples_last_ic))




def insert_ic0_and_cbs(triple_store: TripleStore, chunk_size: int, dataset: str,
                        source_ic0: str, source_cs: str, last_version: int, init_timestamp: datetime):
    triple_store_name = triple_store.name.lower()
    logging.info(f"Constructing timestamped RDF-star dataset from ICs and changesets triple store {triple_store} and chunk size {chunk_size}.")
    policy = "tb_rs_sr"

    repository = policy + "_" + dataset
    database_dir = f"{databases_dir}/{triple_store_name}"
    mgmt_script = eval_setup["rdf_stores"][triple_store_name]["mgmt_script"]

    logging.info("Create GraphDB directories and environment")
    logging.info(f"\nDatabase directory {database_dir}\nConfig dirctory:{CONFIG_DIR}\nConfig Template directory:{CONFIG_TMPL_DIR}")
    subprocess.call(shlex.split(f"{mgmt_script} --log-file {LOG_FILE} create_env {policy} {dataset} {database_dir} {CONFIG_TMPL_DIR} {CONFIG_DIR}"))

    logging.info(f"Ingest empty file into {repository} repository and start {triple_store_name}.")
    subprocess.call(shlex.split(f"{mgmt_script} --log-file {LOG_FILE} ingest_empty {database_dir} {policy} {dataset} {CONFIG_DIR}"))

    logging.info("Startup GraphDB engine")
    subprocess.call(shlex.split(f"{mgmt_script} --log-file {LOG_FILE} startup {database_dir} {policy} {dataset}"))

    logging.info("Read initial snapshot {0} into memory.".format(source_ic0))
    added_triples_raw = open(source_ic0, "r").read().splitlines()
    added_triples_raw = list(filter(None, added_triples_raw))
    added_triples_raw = list(filter(lambda x: not x.startswith("# "), added_triples_raw))

    logging.info("Add triples from initial snapshot {0} as nested triples into the RDF-star dataset.".format(source_ic0))
    
    query_endpoint = eval_setup["rdf_stores"][triple_store_name]["get"].format(repo=f"{policy}_{dataset}")
    update_endpoint = eval_setup["rdf_stores"][triple_store_name]["post"].format(repo=f"{policy}_{dataset}")
    rdf_star_engine = TripleStoreEngine(query_endpoint, update_endpoint)
    try:
        start = time.time()
        rdf_star_engine.insert(triples=added_triples_raw, timestamp=init_timestamp, chunk_size=chunk_size)
        end = time.time()
    except HTTPError:
        logging.info("Too many triples transfered over HTTP. No measures for this chunk size setting will be recorded")
        return False
    execution_time_insert = end - start
    
    df = pd.DataFrame(columns=['triplestore', 'dataset', 'batch', 'cnt_batch_trpls', 'chunk_size', 'execution_time'],
                        data=[[triple_store.name, dataset, 'snapshot_0', len(added_triples_raw), chunk_size, execution_time_insert]])

    # Map versions to files in chronological orders
    change_sets = {}
    for filename in sorted(os.listdir(source_cs)):
        if not (filename.startswith("data-added") or filename.startswith("data-deleted")):
            continue 
        version = int(filename.split('-')[2].split('.')[0].zfill(len(str(last_version)))) - 1
        change_sets[filename] = version

    # Apply changesets to RDF-star dataset
    for filename, version in sorted(change_sets.items(), key=lambda item: item[1]):
        vers_ts = init_timestamp + timedelta(seconds=version)

        mem_in_usage = psutil.virtual_memory().percent
        logging.info(f"Memory in usage: {mem_in_usage}%")
        if mem_in_usage > 85:
            # Reboot to free up main memory
            subprocess.call(shlex.split(f"{mgmt_script} --log-file {LOG_FILE} shutdown"))
            subprocess.call(shlex.split(f"{mgmt_script} --log-file {LOG_FILE} startup {database_dir} {policy} {dataset}"))
        
        if filename.startswith("data-added"):
            logging.info("Read positive changeset {0} into memory.".format(filename))
            added_triples_raw = open(source_cs + "/" + filename, "r").read().splitlines()
            added_triples_raw = list(filter(None, added_triples_raw))
            cnt_trpls = len(added_triples_raw)

            logging.info(f"Add {cnt_trpls} triples from changeset {filename} as nested triples into the RDF-star dataset.")
            start = time.time()
            rdf_star_engine.insert(triples=added_triples_raw, timestamp=vers_ts, chunk_size=chunk_size)
            end = time.time()
            execution_time_insert = end - start
            new_row = pd.DataFrame([[triple_store.name, dataset, 'positive_change_set_' + str(version), len(added_triples_raw), chunk_size, execution_time_insert]], columns=df.columns)
            df = pd.concat([df, new_row], ignore_index=True)

        if filename.startswith("data-deleted"):
            logging.info("Read negative changeset {0} into memory.".format(filename))
            deleted_triples_raw = open(source_cs + "/" + filename, "r").read().splitlines()
            deleted_triples_raw = list(filter(None, deleted_triples_raw))
            cnt_trpls = len(deleted_triples_raw)

            logging.info(f"Oudate {cnt_trpls} triples in the RDF-star dataset which match the triples in {filename}.")                
            start = time.time()
            rdf_star_engine.outdate(triples=deleted_triples_raw, timestamp=vers_ts, chunk_size=chunk_size)
            end = time.time()
            execution_time_outdate = end - start
            new_row = pd.DataFrame([[triple_store.name, dataset, 'negative_change_set_' + str(version), len(deleted_triples_raw), chunk_size, execution_time_outdate]], columns=df.columns)
            df = pd.concat([df, new_row], ignore_index=True)
    
        df.to_csv(f"/starvers_eval/output/measurements/time_update_{str(chunk_size)}.csv", sep=";", index=False, mode='w', header=True)

    # Shutdown engine
    subprocess.call(shlex.split(f"{mgmt_script} --log-file {LOG_FILE} shutdown"))
    subprocess.call(shlex.split(f"{mgmt_script} --log-file {LOG_FILE} shutdown"))

    return df

# via composition from raw files
def construct_tb_star_ds(source_ic0: str, source_cs: str, destination: str,
                         last_version: int, init_timestamp: datetime, dataset:str):
        
    init_timestamp_str = f'"{versioning_timestamp_format(init_timestamp)}"^^<http://www.w3.org/2001/XMLSchema#dateTime>'
    aet = '"9999-12-31T00:00:00.000+02:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>'

    # Read initial snapshot
    logging.info("Read initial snapshot {0} into memory.".format(source_ic0))
    ic0_raw = open(source_ic0, "r").read().splitlines()
    ic0_list = list(filter(None, ic0_raw))
    ic0_list_clean = list(filter(lambda x: not x.startswith("# "), ic0_list))
    ic0_list_timestamped = [f"<< << {triple[:-1].strip()}>> <https://github.com/GreenfishK/DataCitation/versioning/valid_from> {init_timestamp_str}>> <https://github.com/GreenfishK/DataCitation/versioning/valid_until> {aet} .\n" for triple in ic0_list_clean]

    # Write timestamped snapshot
    logging.info("Add triples from initial snapshot {0} as nested triples into the RDF-star dataset.".format(source_ic0)) 
    with open(destination, "w") as rdf_star_dataset:
        rdf_star_dataset.writelines(ic0_list_timestamped)

    # Map versions to files in chronological orders
    change_sets = {}
    for filename in sorted(os.listdir(source_cs)):
        if not (filename.startswith("data-added") or filename.startswith("data-deleted")):
            continue
        version = int(filename.split('-')[2].split('.')[0].zfill(len(str(last_version)))) - 1
        change_sets[filename] = version

    # Apply changesets to RDF-star dataset
    for filename, version in sorted(change_sets.items(), key=lambda item: item[1]):
        vers_ts = init_timestamp + timedelta(seconds=version)
        vers_ts_str = f'"{versioning_timestamp_format(vers_ts)}"^^<http://www.w3.org/2001/XMLSchema#dateTime>'
        mem_in_usage = psutil.virtual_memory().percent
        logging.info(f"Memory in usage: {mem_in_usage}%")

        if filename.startswith("data-added"):
            logging.info("Read positive changeset {0} into memory.".format(filename))
            added_triples_raw = open(source_cs + "/" + filename, "r").read().splitlines()
            added_triples_list = list(filter(None, added_triples_raw))
            added_triples_list_timestamped = [f"<< << {triple[:-1].strip()}>> <https://github.com/GreenfishK/DataCitation/versioning/valid_from> {vers_ts_str}>> <https://github.com/GreenfishK/DataCitation/versioning/valid_until> {aet} .\n" for triple in added_triples_list]
            cnt_trpls = len(added_triples_raw)

            logging.info(f"Add {cnt_trpls} triples from changeset {filename} as nested triples into the RDF-star dataset.")
            with open(destination, "a") as rdf_star_dataset:
                rdf_star_dataset.writelines(added_triples_list_timestamped)
        
        if filename.startswith("data-deleted"):
            logging.info(f"Read negative changeset {filename} into memory.")
            deleted_triples_raw = open(os.path.join(source_cs, filename), "r").read().splitlines()
            deleted_triples_list = list(filter(None, deleted_triples_raw))
            deleted_triples_set = set(t[:-1].strip() for t in deleted_triples_list)

            # Count triples to be invalidated
            cnt_trpls = len(deleted_triples_list)
            logging.info(f"Invalidate {cnt_trpls} triples in the RDF-star dataset which match the triples in {filename}.")

            # Update aet timestamps for matching triples in the negative delta set
            logging.info(f"Updating aet timestamps for matching triples in the negative delta set {filename}.")
            
            valid_until = "<https://github.com/GreenfishK/DataCitation/versioning/valid_until>"
            valid_until_escaped = re.escape(valid_until)
            
            tmp_out = f"{data_dir}/alldata.TB_star_hierarchical.tmp"
            total_replacements = 0
            with open(destination, "r") as rdf_star_graph, open(tmp_out, "w") as fout:
                for line in rdf_star_graph:
                    timestamped_triple = line.strip()
                    fact_triple = timestamped_triple.split(">> <https://github.com/GreenfishK/DataCitation/versioning/valid_from>")[0][6:] 

                    if fact_triple in deleted_triples_set:
                        parts = timestamped_triple.split(valid_until)
                        left, _ = parts
                        left_escaped = re.escape(left)

                        pattern = re.compile(rf'^({left_escaped}{valid_until_escaped}\s+)([^ ]+)(\s+\.)')

                        def replace_valid_until(match):
                            return f"{match.group(1)}{vers_ts_str}{match.group(3)}"

                        # apply replacement to the CURRENT line
                        line = pattern.sub(replace_valid_until, line)
                        total_replacements += 1

                    fout.write(line)

            logging.info(f"Writing updated RDF-star dataset to {destination}.")
            os.replace(tmp_out, destination)
            
            logging.info(f"Invalidated {total_replacements} triples in the RDF-star dataset which match the triples in {filename}.")


def measure_updates(dataset:str, source_ic0: str, source_cs: str, last_version: int, init_timestamp: datetime):
    # HTTPError
    triple_stores = [TripleStore.GRAPHDB]
    chunk_sizes = range(1000, 10000, 1000)
    measure_ts_with_varying_chunk_sizes = partial(insert_ic0_and_cbs, 
                                                dataset=dataset,
                                                source_ic0=source_ic0, 
                                                source_cs=source_cs, 
                                                last_version=last_version, 
                                                init_timestamp=init_timestamp)

    measurements: list[pd.DataFrame] = []
    for ts, chunk_size in product(triple_stores, chunk_sizes):
        result = measure_ts_with_varying_chunk_sizes(ts, chunk_size)
        if result is False:
            # Stop iteration if HTTPError occurred
            break
        measurements.append(result)

    combined_measurements = pd.concat(measurements, join="inner")
    
    logging.info("Writing performance measurements to disk ...")            
    combined_measurements.to_csv("/starvers_eval/output/measurements/time_update.csv", sep=";", index=False, mode='w', header=True)

    # Remove temporary output files
    dir_path = "/starvers_eval/output/measurements/"
    files_to_remove = [os.path.join(dir_path, f) for f in os.listdir(dir_path) if f.startswith("time_update_") and f.endswith(".csv")]
    for file in files_to_remove:
        os.remove(file)


def construct_cbng_ds(source_ic0: str, source_cs: str, destination: str, last_version: int):
    """
    TODO: write docu
    """

    logging.info("Constructing CBNG dataset: The initial IC and changesets are stored as named graphs.")

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

    logging.info("Building version {0}. ".format(str(0)))
    cbng_dataset = ""
    prefixes = {}
    ns_cnt = 1
    ic0_raw = open(source_ic0, "r").read()
    sub_prefixes, ic0 = split_prefixes_dataset(ic0_raw)
    max_version_digits = len(str(last_version))

    template = open("/starvers_eval/scripts/3_construct_datasets/templates/cbng.txt", "r").read()
    cbng_dataset = cbng_dataset + template.format(str(0).zfill(max_version_digits), ic0, "")

    # build list (version, filename_added, filename_deleted)
    cs_add_files = {}
    cs_del_files = {}
    change_sets = []

    if not os.path.exists(source_cs):
        os.makedirs(source_cs)

    for filename in os.listdir(source_cs):
        if not (filename.startswith("data-added") or filename.startswith("data-deleted")):
            continue 
        version = int(filename.split('-')[2].split('.')[0].zfill(4)) - 1
        if filename.startswith("data-added"):
            cs_add_files[version] = filename
        if filename.startswith("data-deleted"):
            cs_del_files[version] = filename
    logging.info("{0} change sets are in directory {1}".format(len(cs_add_files), source_cs))

    for vers, cs_add_file in sorted(cs_add_files.items()):
        change_sets.append((vers, cs_add_file, cs_del_files[vers]))

    assert last_version - 1 <= len(change_sets)
    for i, t in enumerate(change_sets[0:last_version-1]):
        logging.info("Building version {0}. ".format(int(t[0])))
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

    logging.info("Export data set.")
    f = open(destination, "w")
    f.write("\n".join(["@prefix " + key + ":" + value + " ." for key, value in prefixes.items()]) + "\n" + cbng_dataset)
    f.close()


def construct_icng_ds(source: str, destination: str, last_version: int, basename_length: int):
    """
    TODO: write docu
    """

    logging.info("Constructing the ICNG dataset with ICs as named graphs.")
    template = open("/starvers_eval/scripts/3_construct_datasets/templates/icng.txt", "r").read()
    if not os.path.exists(source):
        os.makedirs(source)

    logging.info("Create empty dataset.")
    f = open(destination, "w")
    f.write("")
    f.close()

    for i in range(last_version):
        logging.info("Building version {0}. ".format(str(i+1)))
        ic = open(source + "/" + str(i+1).zfill(basename_length)  + ".nt", "r").read()
    
        logging.info("Write ic {} to data set.".format(str(i+1)))
        f = open(destination, "a")
        f.write(template.format(str(i), ic) + "\n")
        f.close()




############################################# Start procedure #############################################
logging.info(f"Starting procedure for constructing the different dataset variants for the datasets: {datasets}")
for dataset in datasets:
    if dataset not in allowed_datasets:
        logging.error("Dataset must be one of: ", allowed_datasets, "but is: {0}".format(dataset))
        break

    data_dir = f"/starvers_eval/rawdata/{dataset}"
    total_versions = dataset_versions[dataset]
    logging.info("Constructing datasets for {0}".format(dataset))

    if not skip_change_sets == "True":
        construct_change_sets(snapshots_dir=f"{data_dir}/{snapshot_dir}", change_sets_dir=f"{data_dir}/{change_sets_dir}",
                                end_vers=total_versions, format=in_frm, basename_length=ic_basename_lengths[dataset])

    if not skip_tb_star_ds == "True":
        construct_tb_star_ds(source_ic0=f"{data_dir}/{snapshot_dir}/" + "1".zfill(ic_basename_lengths[dataset])  + ".nt",
                            source_cs=f"{data_dir}/{change_sets_dir}.{in_frm}",
                            destination=f"{data_dir}/alldata.TB_star_hierarchical.ttl",
                            last_version=total_versions,
                            init_timestamp=init_version_timestamp,
                            dataset=dataset)    
        
    if not skip_update_measurement == "True":
        measure_updates(dataset=dataset, 
                        source_ic0=f"{data_dir}/{snapshot_dir}/" + "1".zfill(ic_basename_lengths[dataset])  + ".nt",
                        source_cs=f"{data_dir}/{change_sets_dir}.{in_frm}", 
                        last_version=total_versions, 
                        init_timestamp=init_version_timestamp)
    
    if not skip_cbng_ds == "True":
        construct_cbng_ds(source_ic0=f"{data_dir}/{snapshot_dir}/" + "1".zfill(ic_basename_lengths[dataset])  + ".nt",
                        source_cs=f"{data_dir}/{change_sets_dir}.{in_frm}",
                        destination=f"{data_dir}/alldata.CBNG.trig",
                        last_version=total_versions)
    
    if not skip_icng_ds == "True":
        construct_icng_ds(source=f"{data_dir}/{snapshot_dir}/",
                        destination=f"{data_dir}/alldata.ICNG.trig",
                        last_version=total_versions,
                        basename_length=ic_basename_lengths[dataset])
    
logging.info("Finished with constructing datasets.")
