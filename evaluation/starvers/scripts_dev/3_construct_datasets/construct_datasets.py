from datetime import datetime,  timedelta, timezone
import os
import sys
import re
import logging
import tomli
import psutil

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
skip_icng_ds = sys.argv[4]


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



# via composition from raw files
def construct_tb_star_ds(source_ic0: str, source_cs: str, destination: str,
                         last_version: int, init_timestamp: datetime):
        
    init_timestamp_str = f'"{versioning_timestamp_format(init_timestamp)}"^^<http://www.w3.org/2001/XMLSchema#dateTime>'
    aet = '"9999-12-31T00:00:00.000+02:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>'

    Constructs an rdf-star dataset from the initial snapshot and the subsequent changesets and saves
    it to the :destination path.
    """
    policy = "tb_rs_sr"

    mgmt_script = eval_setup["rdf_stores"][TripleStore.GRAPHDB.name.lower()]["mgmt_script"]
    database_dir = f"{databases_dir}/{TripleStore.GRAPHDB.name.lower()}"

    # Insert first snapshot and change sets into GraphDB
    insert_ic0_and_cbs(TripleStore.GRAPHDB, chunk_size=5000, dataset=dataset,
                       source_ic0=source_ic0, source_cs=source_cs, 
                       last_version=last_version, init_timestamp=init_timestamp)    

    # Reboot GraphDB to free up main memory
    logging.info(f"Restarting GraphDB server.")
    subprocess.call(shlex.split(f"{mgmt_script} shutdown"))
    subprocess.call(shlex.split(f"{mgmt_script} startup {database_dir} {policy} {dataset}"))
    
    # Extract and dump repository
    logging.info(f"Extract the whole dataset from the GraphDB repository {policy}_{dataset} and dump it to {destination}.")
    subprocess.call(shlex.split(f"{mgmt_script} dump_repo {database_dir} {policy} {dataset} {destination}"))
    
    # Count triples
    cnt_rdf_star_trpls: subprocess.CompletedProcess[str] = subprocess.run(["awk",
        r'''
        /^<<<<</ { in_block = 1 }
        in_block && /\^\^xsd:dateTime \.$/ {
            count++
            in_block = 0
        }
        END { print count }
        ''', destination], capture_output=True, text=True)   
    logging.info("There are {0} timestamped triples in the RDF-star dataset {1}. Should be the same number as in the extraction.".format(cnt_rdf_star_trpls.stdout, destination))
    cnt_rdf_star_valid_trpls = subprocess.run(["grep", "-c", '<https://github.com/GreenfishK/DataCitation/versioning/valid_until> "9999-12-31T00:00:00.000+02:00"', destination], capture_output=True, text=True)  
    logging.info("There are {0} not outdated triples in the RDF-star dataset {1}. Should be the same number as in the extraction.".format(cnt_rdf_star_valid_trpls.stdout, destination))

    # Shutdown triple store
    logging.info("Shutting down GraphDB server.")
    subprocess.call(shlex.split(f"{mgmt_script} shutdown"))

    # Remove database files
    logging.info("Removing database files.")
    shutil.rmtree("/starvers_eval/databases/construct_datasets/", ignore_errors=True)
    shutil.rmtree("/run/configuration", ignore_errors=True)

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


def construct_bear_ng(source: str, destination: str, last_version: int, basename_length: int):
    """
    Creates a dataset variant that includes the version in which a triple was valid in the named grah identifier in the fourth position of a triple, according to the following example:
        ex:s1 ex:p1 ex:o1 :v_21_22_23_25 .
        :v_21_22_23_25 owl:versionInfo "21" :versions .
        :v_21_22_23_25 owl:versionInfo "22" :versions .
        :v_21_22_23_25 owl:versionInfo "23" :versions .
        :v_21_22_23_25 owl:versionInfo "25" :versions .
    """
    pass



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
        construct_tb_star_ds_from_files(source_ic0=f"{data_dir}/{snapshot_dir}/" + "1".zfill(ic_basename_lengths[dataset])  + ".nt",
                            source_cs=f"{data_dir}/{change_sets_dir}.{in_frm}",
                            destination=f"{data_dir}/alldata.TB_star_hierarchical.ttl",
                            last_version=total_versions,
                            init_timestamp=init_version_timestamp)    
    
    if not skip_icng_ds == "True":
        construct_icng_ds(source=f"{data_dir}/{snapshot_dir}/",
                        destination=f"{data_dir}/alldata.ICNG.trig",
                        last_version=total_versions,
                        basename_length=ic_basename_lengths[dataset])
    
logging.info("Finished with constructing datasets.")
