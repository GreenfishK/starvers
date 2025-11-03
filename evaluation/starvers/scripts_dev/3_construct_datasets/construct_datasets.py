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


def construct_tb_star_ds(source_ic0, source_cs: str, destination: str, last_version: int, init_timestamp: datetime, dataset:str):
    """
    :param: source_ic0: The path in the filesystem to the initial snapshot.
    :param: destination: The path in the filesystem to the resulting dataset.
    :param: init_timestamp: The initial timestamp that is being incremented by 1sec for each dataset version/pair of changesets.

    Constructs an rdf-star dataset from the initial snapshot and the subsequent changesets. It constructs the dataset
    10 times for each triple store with an initial chunk size of 2000 for the update operations and increasing the chunk size 
    by 2000 in every iteration.
    """
    policy = "tb_rs_sr"
    repository = policy + "_" + dataset
    triple_store_configs = {'graphdb': {'start_script': '/starvers_eval/scripts/3_construct_datasets/start_graphdb.sh',
                                        'query_endpoint': 'http://Starvers:7200/repositories/{0}_{1}'.format(policy, dataset),
                                        'update_endpoint': 'http://Starvers:7200/repositories/{0}_{1}/statements'.format(policy, dataset),
                                        'shutdown_process': f'/opt/java/java11/openjdk/bin/java',
                                       },
                            'jenatdb2': {'start_script': '/starvers_eval/scripts/3_construct_datasets/start_jenatdb2.sh',
                                        'query_endpoint': 'http://Starvers:3030/{0}_{1}/sparql'.format(policy, dataset),
                                        'update_endpoint': 'http://Starvers:3030/{0}_{1}/update'.format(policy, dataset),
                                        'shutdown_process': '/jena-fuseki/fuseki-server.jar',
                                        }}
    

    def construct_ds_in_db(triple_store: TripleStore, chunk_size: int, ts_configs: dict):
        logging.info(f"Constructing timestamped RDF-star dataset from ICs and changesets triple store {triple_store} and chunk size {chunk_size}.")
        configs = ts_configs[triple_store.name.lower()]

        logging.info("Ingest empty file into {0} repository and start {1}.".format(repository, triple_store.name))
        subprocess.call(shlex.split('{0} {1} {2} {3} {4} {5}'.format(
            configs['start_script'], policy, dataset, "true", "true", "false")))

        logging.info("Read initial snapshot {0} into memory.".format(source_ic0))
        added_triples_raw = open(source_ic0, "r").read().splitlines()
        added_triples_raw = list(filter(None, added_triples_raw))
        added_triples_raw = list(filter(lambda x: not x.startswith("# "), added_triples_raw))

        logging.info("Add triples from initial snapshot {0} as nested triples into the RDF-star dataset.".format(source_ic0))
        rdf_star_engine = TripleStoreEngine(configs['query_endpoint'], configs['update_endpoint'])
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
                logging.info("Memory usage over 85%. Restarting {0} server.".format(triple_store.name))
                subprocess.call(shlex.split('{0} {1} {2} {3} {4} {5}'.format(
                    configs['start_script'], policy, dataset, "false", "false", "true")))
            
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

        return df
    
    if dataset == 'bearc':
        # HTTPError
        triple_stores = [TripleStore.GRAPHDB]
        chunk_sizes = range(1000, 10000, 1000)
        measure_updates = partial(construct_ds_in_db, ts_configs=triple_store_configs)

        measurements: list[pd.DataFrame] = []
        for ts, chunk_size in product(triple_stores, chunk_sizes):
            result = measure_updates(ts, chunk_size)
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
    else:
        construct_ds_in_db(TripleStore.GRAPHDB, chunk_size=5000, ts_configs=triple_store_configs)    

    logging.info("Extract the whole dataset from the GraphDB repository.")
    # Reboot to free up main memory
    #logging.info("Restarting {0} server.".format(triple_store.name))
    #subprocess.call(shlex.split('{0} {1} {2} {3} {4} {5}'.format(
    #    configs['start_script'], policy, dataset, "false", "false", "true")))
    sparql_engine = SPARQLWrapper(triple_store_configs['graphdb']['query_endpoint'])
    
    sparql_engine.setReturnFormat(JSON)
    sparql_engine.setOnlyConneg(True)
    sparql_engine.setQuery("""
    select ?s ?p ?o ?x ?y ?a ?b {
        << <<?s ?p ?o >> ?x ?y >> ?a ?b .
    }
    """)
    results = sparql_engine.queryAndConvert()
    logging.info("There are {0} triples in the extraction.".format(len(results["results"]["bindings"])))

    logging.info("Line-wise convert JSON output of final RDF-star dataset into N3 format and write to: {0}".format(destination))
    with open(destination, "w") as rdf_star_ds_file:
        for r in results["results"]["bindings"]:
            # Further potential replacements: 
            # replace(r"\"", '\\"')
            # replace(r"\x", r"\\x")

            # Parse subject at nesting level 2
            if r['s']['type'] == "uri":
                s = "<" + r['s']['value'] + ">"
            else:
                s = r['s']['value'] \
                  .replace("\\","\\\\") \
                  .replace(r'"', r'\"') \
                  .replace("\n","\\n") \
                  .replace("\t", "\\t") \
                  .replace("\r", "\\r")
            # Parse predicate at nesting level 2
            p = URIRef(r['p']['value'])

            # Parse object at nesting level 2
            if r['o']['type']  == "uri":
                o = "<" + r['o']['value'] + ">"
            elif r['o']['type'] == "blank":
                o = r['o']['value']
            else:
                value = r['o']["value"] \
                  .replace("\\","\\\\") \
                  .replace(r'"', r'\"') \
                  .replace("\n","\\n") \
                  .replace("\t", "\\t") \
                  .replace("\r", "\\r")
                lang = r['o'].get("xml:lang", None)
                datatype = r['o'].get("datatype", None)
                o = '"' + value + '"'
                if lang:
                    o+='@' + lang 
                elif datatype:
                    o+="^^" + "<" + datatype + ">"
            
            # Parse predicate at nesting level 1
            x = URIRef(r['x']['value'])

            # Parse object at nesting level 1
            value = r['y']["value"]
            datatype = r['y'].get("datatype", None)
            y = '"' + value + '"^^' + "<" + datatype + ">"
            
            # Parse predicate at nesting level 0
            a = URIRef(r['a']['value'])

            # Parse object at nesting level 0
            value = r['b']["value"]
            datatype = r['b'].get("datatype", None)
            b = '"' + value + '"^^' + "<" + datatype + ">"
            
            rdf_star_ds_file.write("<< << " + s + " " + p.n3() + " " + o  + ">>" + x.n3()  + " " + y  + " >>" + a.n3()  + " " + b + " .\n")
    
    cnt_rdf_star_trpls = subprocess.run(["sed", "-n", '$=', destination], capture_output=True, text=True)   
    logging.info("There are {0} triples in the RDF-star dataset {1}. Should be the same number as in the extraction.".format(cnt_rdf_star_trpls.stdout, destination))
    cnt_rdf_star_valid_trpls = subprocess.run(["grep", "-c", '<https://github.com/GreenfishK/DataCitation/versioning/valid_until> "9999-12-31T00:00:00.000+02:00"', destination], capture_output=True, text=True)  
    logging.info("There are {0} not outdated triples in the RDF-star dataset {1}. Should be the same number as in the extraction.".format(cnt_rdf_star_valid_trpls.stdout, destination))

    logging.info("Shutting down GraphDB server and removing database files.")
    subprocess.run(["pkill", "-f", "{0}".format(triple_store_configs['graphdb']['shutdown_process'])])
    shutil.rmtree("/starvers_eval/databases/construct_datasets/", ignore_errors=True)
    shutil.rmtree("/run/configuration", ignore_errors=True)


def construct_cbng_ds(source_ic0, source_cs: str, destination: str, last_version: int):
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

############################################# Logging #############################################
if not os.path.exists('/starvers_eval/output/logs/construct_datasets'):
    os.makedirs('/starvers_eval/output/logs/construct_datasets')
with open('/starvers_eval/output/logs/construct_datasets/construct_datasets.txt', "w") as log_file:
    log_file.write("")
logging.basicConfig(handlers=[logging.FileHandler(filename="/starvers_eval/output/logs/construct_datasets/construct_datasets.txt", 
                                                  encoding='utf-8', mode='a+')],
                    format="%(asctime)s %(name)s:%(levelname)s:%(message)s", 
                    datefmt="%F %A %T", 
                    level=logging.INFO)

############################################# Parameters #############################################
datasets = sys.argv[1].split(" ")
skip_change_sets = sys.argv[2]
skip_tb_star_ds = sys.argv[3]
skip_cbng_ds = sys.argv[4]
skip_icng_ds = sys.argv[5]

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
