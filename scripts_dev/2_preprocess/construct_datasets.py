from datetime import datetime
from datetime import timedelta, timezone
import os
from enum import Enum
import re
import sys
import re
import logging
import subprocess
import shlex
from SPARQLWrapper import SPARQLWrapper, JSON
from rdflib.term import URIRef, Literal, BNode
from starvers.starvers import TripleStoreEngine


# Global variables for assertions
# cnt_net_triples_added = 0
# cnt_triples_rdf_star = 0
# cnt_valid_triples_last_ic = 0

class AnnotationStyle(Enum):
    HIERARCHICAL = 1
    FLAT = 2

def construct_change_sets(dataset_dir: str, end_vers: int, format: str, basename_length: int):
    """
    end_vers: The last version that should be built. Can only build as many versions as there are snapshots provided
    in the dataset_dir.
    format: ttl or nt.

    """

    logging.info("Constructing changesets.")
    cb_comp_dir = dataset_dir + "/alldata.CB_computed." + format
    if not os.path.exists(cb_comp_dir):
        logging.info("Create directory: " + cb_comp_dir)
        os.makedirs(cb_comp_dir)

    cnt_net_triples_added = 0    
    cnt_triples_rdf_star = 0
    cnt_valid_triples_last_ic = 0

    for i in range(1, end_vers):
        ic1_ds_path = "{0}/alldata.IC.nt/{1}.nt".format(dataset_dir, str(i).zfill(basename_length))
        ic2_ds_path = "{0}/alldata.IC.nt/{1}.nt".format(dataset_dir, str(i+1).zfill(basename_length))
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
    # sed -n "$=" alldata.TB_star_hierarchical.ttl      
    logging.info("Assertion: Triples that are still valid with the latest snapshot: {0}".format(cnt_valid_triples_last_ic))
    # grep -c '<https://github.com/GreenfishK/DataCitation/versioning/valid_until> "9999-12-31T00:00:00.000' alldata.TB_star_hierarchical.ttl 


def construct_tb_star_ds(source_ic0, source_cs: str, destination: str, last_version: int, init_timestamp: datetime, policy:str, dataset:str):
    """
    :param: source_ic0: The path in the filesystem to the initial snapshot.
    :param: destination: The path in the filesystem to the resulting dataset.
    :param: init_timestamp: The initial timestamp that is being incremented by 1sec for each dataset version/pair of changesets.

    Constructs an rdf-star dataset from the initial snapshot and the subsequent changesets.
    """

    """
    # Call GraphDB instance
    subprocess.call(shlex.split('/starvers_eval/scripts/2_preprocess/ingest_and_start_graphdb.sh source_ic0 tbsh beara'))

    """
    
    logging.info("Constructing RDF-star dataset for the {} policy from ICs and changesets.".format(policy))
    # Constants
    valid_from_predicate = "<https://github.com/GreenfishK/DataCitation/versioning/valid_from>"
    valid_until_predicate = "<https://github.com/GreenfishK/DataCitation/versioning/valid_until>"
    xsd_datetime = "<http://www.w3.org/2001/XMLSchema#dateTime>"
    tz_offset = "+02:00"
    valid_ufn_ts_res = '"9999-12-31T00:00:00.000{tz_offset}"^^{datetimeref}'.format(tz_offset=tz_offset, datetimeref=xsd_datetime)
    sys_ts_formatted = datetime.strftime(init_timestamp, "%Y-%m-%dT%H:%M:%S.%f")[:-3]
    init_ts_res = '"{ts}{tz_offset}"^^{datetimeref}'.format(ts=sys_ts_formatted, tz_offset=tz_offset, datetimeref=xsd_datetime) 

    # Start GraphDB  
    logging.info("Ingest empty file into GraphDB repository and start GraphDB.")
    subprocess.call(shlex.split('/starvers_eval/scripts/2_preprocess/start_graphdb.sh {0} {1}'.format(policy, dataset)))

    # Create RDF engines
    rdf_star_engine = TripleStoreEngine('http://Starvers:7200/repositories/{0}_{1}'.format(policy, dataset),
                                        'http://Starvers:7200/repositories/{0}_{1}/statements'.format(policy, dataset))
    sparql_engine = SPARQLWrapper('http://Starvers:7200/repositories/{0}_{1}'.format(policy, dataset))
    sparql_engine.setReturnFormat(JSON)
    sparql_engine.setOnlyConneg(True)

    logging.info("Read initial snapshot {0} into memory.".format(source_ic0))
    added_triples_raw = open(source_ic0, "r").read().splitlines()
    added_triples_raw = list(filter(None, added_triples_raw))
    added_triples_raw = list(filter(lambda x: not x.startswith("# "), added_triples_raw))
    insert_block = ""
    for line in added_triples_raw:
        insert_block = insert_block + "(" + line[:-1] + ")\n"

    # Ingest ic0 into GraphDB as RDF-star dataset
    logging.info("Add triples from initial snapshot {0} as nested triples into the RDF-star dataset.".format(source_ic0))
    rdf_star_engine.insert(triples=insert_block, timestamp=init_timestamp)

    # transform all triples in the list to their starvers RDF-star representations
    """result_set += list(map(list, zip(["<< <<"] * len(added_triples_raw),
                             added_triples_raw, 
                             [">>"] * len(added_triples_raw), 
                             [valid_from_predicate] * len(added_triples_raw),
                             [init_ts_res] * len(added_triples_raw), 
                             [">>"] * len(added_triples_raw),
                             [valid_until_predicate] * len(added_triples_raw),
                             [valid_ufn_ts_res] * len(added_triples_raw),
                             ['.'] * len(added_triples_raw))))"""

    # Map versions to files in chronological orders
    change_sets = {}
    for filename in sorted(os.listdir(source_cs)):
        if not (filename.startswith("data-added") or filename.startswith("data-deleted")):
            continue 
        version = int(filename.split('-')[2].split('.')[0].zfill(len(str(last_version)))) - 1
        change_sets[filename] = version

    # First add all triples from the "add changesets", then delete the matching triples from the "delete changesets"
    for filename, version in sorted(change_sets.items(), key=lambda item: item[1]):
        vers_ts = init_timestamp + timedelta(seconds=version)
        #vers_ts_str = '"{ts}{tz_offset}"^^{datetimeref}'.format(ts=datetime.strftime(vers_ts, "%Y-%m-%dT%H:%M:%S.%f")[:-3], tz_offset=tz_offset, datetimeref=xsd_datetime)            
        
        if filename.startswith("data-added"):
            logging.info("Read positive changeset {0} into memory.".format(filename))
            added_triples_raw = open(source_cs + "/" + filename, "r").read().splitlines()
            added_triples_raw = list(filter(None, added_triples_raw))
            insert_block = ""
            for line in added_triples_raw:
                insert_block = insert_block + "(" + line[:-1] + ")\n"

            # logging.info("Add changeset to RDF-star dataset.")
            """result_set += list(map(list, zip(["<< <<"] * len(added_triples_raw),
                                      added_triples_raw, 
                                      [">>"] * len(added_triples_raw), 
                                      [valid_from_predicate] * len(added_triples_raw),
                                      [vers_ts_str] * len(added_triples_raw), 
                                      [">>"] * len(added_triples_raw),
                                      [valid_until_predicate] * len(added_triples_raw),
                                      [valid_ufn_ts_res] * len(added_triples_raw),
                                      ['.'] * len(added_triples_raw))))
            result_set = sorted(result_set, key=lambda x: x[1])   """    
            logging.info("Add triples from changeset {0} as nested triples into the RDF-star dataset.".format(filename))
            rdf_star_engine.insert(triples=insert_block, timestamp=vers_ts)
            # logging.info("Positive change {0} set added: Number of triples in RDF-star dataset: {1}".format(filename, len(result_set)))       
        if filename.startswith("data-deleted"):
            logging.info("Read negative changeset {0} into memory.".format(filename))
            #deleted_triples_raw = sorted(open(source_cs + "/" + filename, "r").read().splitlines())
            deleted_triples_raw = open(source_cs + "/" + filename, "r").read().splitlines()
            deleted_triples_raw = list(filter(None, deleted_triples_raw))
            outdate_block = ""
            for line in deleted_triples_raw:
                outdate_block = outdate_block + "(" + line[:-1] + ")\n"

            #logging.info("Update the artificial valid_until timestamps of all triples in the RDF-star dataset that match with the triples in {0}.".format(filename))
            logging.info("Oudate triples in the RDF-star dataset which match the triples in {0}.".format(filename))
            rdf_star_engine.outdate(triples=outdate_block, timestamp=vers_ts)

            """try:
                for i, triple in enumerate(result_set):
                    #if len(deleted_triples_raw) == 0:
                    #    break  
                    if triple[1] == deleted_triples_raw[0] and triple[7] == valid_ufn_ts_res:
                        result_set[i][7] = vers_ts_str
                        deleted_triples_raw.pop(0)
                    #if (i % round(len(result_set)/10)) == 0:
                    #    logging.info("{0}% artificial valid_until timestamps updated.".format((i/len(result_set))*100))
            except IndexError:
                logging.info("All artificial valid_until timestamps in the RDF-star dataset have been updated according to the triples in {0}.".format(filename))
                continue"""
        
    #logging.info("The final RDF-star dataset has {0} triples".format(len(result_set)))
    logging.info("Query final RDF-star dataset.")
    sparql_engine.setQuery("""
    select ?s ?p ?o ?x ?y ?a ?b {
        << <<?s ?p ?o >> ?x ?y >> ?a ?b .
    }
    """)
    results = sparql_engine.queryAndConvert()
    results_str = ""

    logging.info("Convert JSON output of final RDF-star dataset into N3 format.")
    for r in results["results"]["bindings"]:
        if r['s']['type'] == "uri":
            s = URIRef(r['s']['value'])
        else:
            s = BNode(r['s']['value'])
        p = URIRef(r['p']['value'])
        if r['o']['type']  == "uri":
            o = URIRef(r['o']['value'])
        elif r['o']['type'] == "blank":
            o = BNode(r['o']['value'])
        else:
            value = r['o']["value"]
            lang = r['o'].get("xml:lang", None)
            datatype = r['o'].get("datatype", None)
            o = Literal(value, lang=lang, datatype=datatype)
        x = URIRef(r['x']['value'])
        value = r['y']["value"]
        lang = r['y'].get("xml:lang", None)
        datatype = r['y'].get("datatype", None)
        y = Literal(value,lang=lang,dataype=datatype)
        a = URIRef(r['a']['value'])
        value = r['b']["value"]
        lang = r['b'].get("xml:lang", None)
        datatype = r['b'].get("datatype", None)
        b = Literal(value,lang=lang,dataype=datatype)
        results_str = results_str + "<< << " + s + " " + p + " " + o + ">>" + x + " " + y + " >>" + a + " " + b + " ." + "\n"

    logging.info("Write RDF-star dataset from memory to file.")
    with open(destination, "w") as rdf_star_ds_file:
        rdf_star_ds_file.write(results_str)

    # TODO: extract ?b and ?y and assemble RDF star dataset
    logging.info("Shutting down GraphDB server.")
    subprocess.run("pkill", "-f", "'/opt/java/openjdk/bin/java'")

    """rdf_star_ds_str = ""
    with open(destination, "w") as rdf_star_ds_file:
        rdf_star_ds_file.write(rdf_star_ds_str)
    for i, rdf_star_triple_list in enumerate(result_set):
        assert rdf_star_triple_list[1][-2:] == " ."
        rdf_star_triple_list[1] = rdf_star_triple_list[1][:-2]
        rdf_star_ds_str += " ".join(rdf_star_triple_list) + "\n"
    with open(destination, "a") as rdf_star_ds_file:
          rdf_star_ds_file.write(rdf_star_ds_str)"""


  
def construct_cbng_ds(source_ic0, source_cs: str, destination: str, last_version: int):
    """
    TODO: write docu
    """

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

    logging.info("Building version {0}. ".format(str(0)))
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
    template = open("/starvers_eval/scripts/1_get_and_prepare_data/templates/icng.txt", "r").read()
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

############################################# Logging ###################################################################
if not os.path.exists('/starvers_eval/output/logs/preprocessing'):
    os.makedirs('/starvers_eval/output/logs/preprocessing')
with open('/starvers_eval/output/logs/preprocessing/construct_datasets.txt', "w") as log_file:
    log_file.write("")
logging.basicConfig(handlers=[logging.FileHandler(filename="/starvers_eval/output/logs/preprocessing/construct_datasets.txt", 
                                                  encoding='utf-8', mode='a+')],
                    format="%(asctime)s %(name)s:%(levelname)s:%(message)s", 
                    datefmt="%F %A %T", 
                    level=logging.INFO)

############################################# Parameters and function calls #############################################
datasets = sys.argv[1].split(" ")
in_frm = "nt"
LOCAL_TIMEZONE = datetime.now(timezone.utc).astimezone().tzinfo
init_version_timestamp = datetime(2022,10,1,12,0,0,0,LOCAL_TIMEZONE)
dataset_versions = {'beara':58, 'bearb_day':89, 'bearb_hour':1299, 'bearc':33, 'beart': 4}
ic_basename_lengths = {'beara': 1, 'bearb_hour': 6, 'bearb_day': 6, 'bearc': 1, 'beart': 6}

for dataset in datasets:
    if dataset not in ['beara', 'bearb_day', 'bearb_hour', 'bearc', 'beart']:
        print("Dataset must be one of: ", ['beara', 'bearb_day', 'bearb_hour', 'bearc', 'beart'])
        break

    data_dir = "/starvers_eval/rawdata/" + dataset
    total_versions = dataset_versions[dataset]
    print("Constructing datasets for {0}".format(dataset))

    # CB
    construct_change_sets(dataset_dir=data_dir, end_vers=total_versions, format=in_frm, basename_length=ic_basename_lengths[dataset])

    # TBSH
    construct_tb_star_ds(source_ic0=data_dir + "/alldata.IC.nt/" + "1".zfill(ic_basename_lengths[dataset])  + ".nt",
                        source_cs=data_dir + "/alldata.CB_computed." + in_frm,
                        destination=data_dir + "/alldata.TB_star_hierarchical" + ".ttl",
                        last_version=total_versions,
                        init_timestamp=init_version_timestamp,
                        policy="tbsh",
                        dataset=dataset)
    
    # TBSF
    # construct_tb_star_ds(source_ic0=data_dir + "/alldata.IC.nt/" + "1".zfill(ic_basename_lengths[dataset])  + ".nt",
    #                    source_cs=data_dir + "/alldata.CB_computed." + in_frm,
    #                    destination=data_dir + "/alldata.TB_star_flat." + ".ttl",
    #                    last_version=total_versions,
    #                    init_timestamp=init_version_timestamp,
    #                    annotation_style=AnnotationStyle.FLAT)
    
    # CBNG
    construct_cbng_ds(source_ic0=data_dir + "/alldata.IC.nt/" + "1".zfill(ic_basename_lengths[dataset])  + ".nt",
                      source_cs=data_dir + "/alldata.CB_computed." + in_frm,
                      destination=data_dir + "/alldata.CBNG.trig",
                      last_version=total_versions)

    # ICNG
    construct_icng_ds(source=data_dir + "/alldata.IC.nt",
                      destination=data_dir + "/alldata.ICNG.trig",
                      last_version=total_versions,
                      basename_length=ic_basename_lengths[dataset])
