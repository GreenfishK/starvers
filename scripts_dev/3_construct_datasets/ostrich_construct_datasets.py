from datetime import datetime
from datetime import timezone
import os
import sys
import logging
import tomli

def construct_snapshot(dst_dir: str, snapshots_dir: str, basename_length: int):
    logging.info("Constructing snapshot ...")

    v0_dir = f"{dst_dir}/alldata.IC.nt"
    if not os.path.exists(v0_dir):
        logging.info(f"Create directory: {v0_dir}")
        os.makedirs(v0_dir) 
    

    snapshot_ds_path = "{0}/{1}.nt".format(snapshots_dir, str(1).zfill(basename_length))
    with open(snapshot_ds_path, "r") as snapshot_file:
        snapshot_content = snapshot_file.read().splitlines()

    lines_in_snapshot = set([line for line in snapshot_content if not (line.startswith("#") or len(line) == 0)])
    snapshot_str = "\n".join(triple for triple in lines_in_snapshot)

    logging.info("Create alldata.IC.nt/000001.nt with {0} triples.".format(len(lines_in_snapshot)))

    file_path_snapshot =  f"{dst_dir}/alldata.IC.nt/000001.nt"

    with open(file_path_snapshot, "w") as file_snapshot:
        file_snapshot.write(snapshot_str)
  
def construct_empty_version(dst_dir: str):
    logging.info("Constructing change set for v1 ...")

    v1_dir = f"{dst_dir}/1"
    if not os.path.exists(v1_dir):
        logging.info(f"Create directory: {v1_dir}")
        os.makedirs(v1_dir)

    file_path_added =  f"{dst_dir}/1/main.nt.additions.txt"
    file_path_deleted = f"{dst_dir}/1/main.nt.deletions.txt"

    with open(file_path_added, "w") as file_added, open(file_path_deleted, "w") as file_deleted:
        file_added.write("")
        file_deleted.write("")

def construct_dataset(dst_dir: str, snapshots_dir: str, end_vers: int, basename_length: int):
    if not os.path.exists(dst_dir):
        logging.info("Create directory: " + dst_dir)
        os.makedirs(dst_dir)

    construct_snapshot(dst_dir, snapshots_dir, basename_length)
    #construct_empty_version(dst_dir)

    cnt_net_triples_added = 0    
    cnt_triples_rdf_star = 0
    cnt_valid_triples_last_ic = 0

    for i in range(1, end_vers):
        logging.info(f"Constructing changeset for v{i} ...")

        v_dir = f"{dst_dir}/{i}"
        if not os.path.exists(v_dir):
            logging.info(f"Create directory: {v_dir}")
            os.makedirs(v_dir)

        ic1_ds_path = "{0}/{1}.nt".format(snapshots_dir, str(i).zfill(basename_length))
        ic2_ds_path = "{0}/{1}.nt".format(snapshots_dir, str(i+1).zfill(basename_length))

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

        logging.info("Create {0}/main.nt.additions.txt with {1} triples.".format(i, len(cs_added)))
        file_path_added = f"{v_dir}/main.nt.additions.txt"
        with open(file_path_added, "w") as cs_added_file:
            cs_added_file.write(cs_added_str)
        cs_added, cs_added_str = None, None

        cs_deleted_str = "\n".join(triple for triple in cs_deleted)
        cnt_net_triples_added -= len(cs_deleted)

        logging.info("Create {0}/main.nt.deletions.txt with {1} triples.".format(i, len(cs_deleted)))
        file_path_deleted = f"{v_dir}/main.nt.deletions.txt"
        with open(file_path_deleted, "w") as cs_deleted_file:
            cs_deleted_file.write(cs_deleted_str)
        cs_deleted, cs_deleted_str = None, None
    
    logging.info("Assertion: From the first to the last snapshot {1} triples were added (net)".format(end_vers, cnt_net_triples_added))        
    logging.info("Assertion: The rdf-star dataset created with function construct_tb_star_ds should have {1} triples".format(end_vers, cnt_triples_rdf_star))
    logging.info("Assertion: Triples that are still valid with the latest snapshot: {0}".format(cnt_valid_triples_last_ic))


############################################# Logging #############################################
if not os.path.exists('/ostrich_eval/output/logs/construct_datasets'):
    os.makedirs('/ostrich_eval/output/logs/construct_datasets')
with open('/ostrich_eval/output/logs/construct_datasets/construct_datasets.txt', "w") as log_file:
    log_file.write("")
logging.basicConfig(handlers=[logging.FileHandler(filename="/ostrich_eval/output/logs/construct_datasets/construct_datasets.txt", 
                                                  encoding='utf-8', mode='a+')],
                    format="%(asctime)s %(name)s:%(levelname)s:%(message)s", 
                    datefmt="%F %A %T", 
                    level=logging.INFO)

############################################# Parameters #############################################
datasets = sys.argv[1].split(" ")

in_frm = "nt"
LOCAL_TIMEZONE = datetime.now(timezone.utc).astimezone().tzinfo
with open("/ostrich_eval/configs", mode="rb") as config_file:
    eval_setup = tomli.load(config_file)
dataset_versions = {dataset: infos['snapshot_versions'] for dataset, infos in eval_setup['datasets'].items()}
ic_basename_lengths = {dataset: infos['ic_basename_length'] for dataset, infos in eval_setup['datasets'].items()}
allowed_datasets = list(dataset_versions.keys())
snapshot_dir = eval_setup['general']['snapshot_dir']
change_sets_dir = eval_setup['general']['change_sets_dir']

############################################# Start procedure #############################################
for dataset in datasets:
    if dataset not in allowed_datasets:
        print("Dataset must be one of: ", allowed_datasets, "but is: {0}".format(dataset))
        break

    data_dir = f"/ostrich_eval/rawdata/{dataset}"
    dst_dir = f"/ostrich_eval/datasets/{dataset}"
    total_versions = dataset_versions[dataset]
    print("Constructing dataset for {0}".format(dataset))

    construct_dataset(dst_dir=dst_dir, snapshots_dir=f"{data_dir}/{snapshot_dir}", end_vers=total_versions, basename_length=ic_basename_lengths[dataset])

 
