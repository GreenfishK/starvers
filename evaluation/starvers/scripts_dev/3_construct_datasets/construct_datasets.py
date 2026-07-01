from datetime import datetime,  timedelta, timezone
import os
import sys
import re
import logging
import tomli
import psutil
from pathlib import Path

from starvers._helper import versioning_timestamp_format
from scripts.logging import setup_logging

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
BASE_LOG_DIR, LOG = setup_logging("construct_datasets")

# ---------------------------------------------------------------------------
# Static eval parameters
# ---------------------------------------------------------------------------
CONFIG_PATH = Path("/starvers_eval/configs/eval_setup.toml")
def _load_config() -> dict:
    with open(CONFIG_PATH, "rb") as f:
        return tomli.load(f)
static_eval_params = _load_config()

# ---------------------------------------------------------------------------
# Environment / path constants
# ---------------------------------------------------------------------------
datasets = os.environ.get("datasets").split(" ")
skip_change_sets = os.environ.get("skip_change_sets")
skip_tb_star_ds = os.environ.get("skip_tb_star_ds")
skip_tb_reif_ds = os.environ.get("skip_tb_reif_ds")
skip_icng_ds = os.environ.get("skip_icng_ds")
skip_tb_ds = os.environ.get("skip_tb_ds")

in_frm = "nt"
LOCAL_TIMEZONE = datetime.now(timezone.utc).astimezone().tzinfo
init_version_timestamp = datetime(2022,10,1,12,0,0,0,LOCAL_TIMEZONE)


dataset_versions = {dataset: infos['snapshot_versions'] for dataset, infos in static_eval_params['datasets'].items()}
ic_basename_lengths = {dataset: infos['ic_basename_length'] for dataset, infos in static_eval_params['datasets'].items()}
allowed_datasets = list(dataset_versions.keys())
snapshot_dir = static_eval_params['general']['snapshot_dir']
change_sets_dir = static_eval_params['general']['change_sets_dir']



# ---------------------------------------------------------------------------
# Functions
# ---------------------------------------------------------------------------
def construct_change_sets(snapshots_dir: str, change_sets_dir: str, end_vers: int, format: str, basename_length: int):
    """
    end_vers: The last version that should be built. Can only build as many versions as there are snapshots provided
    in the dataset_dir.
    format: ttl or nt.

    """

    LOG.info("Constructing changesets.")
    cb_comp_dir = f"{change_sets_dir}.{format}"
    if not os.path.exists(cb_comp_dir):
        LOG.info("Create directory: " + cb_comp_dir)
        os.makedirs(cb_comp_dir)

    cnt_net_triples_added = 0    
    cnt_triples_rdf_star = 0
    cnt_valid_triples_last_ic = 0

    for i in range(1, end_vers):
        ic1_ds_path = "{0}/{1}.nt".format(snapshots_dir, str(i).zfill(basename_length))
        ic2_ds_path = "{0}/{1}.nt".format(snapshots_dir, str(i+1).zfill(basename_length))
        LOG.info("Calculating changesets between snapshots {0}.nt and {1}.nt".format(str(i).zfill(basename_length), str(i+1).zfill(basename_length)))


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
        LOG.info("Create data-added_{0}-{1}.nt with {2} triples.".format(i, i + 1, len(cs_added)))
        with open(cb_comp_dir + "/" + "data-added_{0}-{1}.{2}".format(i, i + 1, format), "w") as cs_added_file:
            cs_added_file.write(cs_added_str)
        cs_added, cs_added_str = None, None

        cs_deleted_str = "\n".join(triple for triple in cs_deleted)
        cnt_net_triples_added -= len(cs_deleted)
        LOG.info("Create data-deleted_{0}-{1}.nt with {2} triples.".format(i, i + 1, len(cs_deleted)))
        with open(cb_comp_dir + "/" + "data-deleted_{0}-{1}.{2}".format(i, i + 1, format), "w") as cs_deleted_file:
            cs_deleted_file.write(cs_deleted_str)
        cs_deleted, cs_deleted_str = None, None
    
    LOG.info("Assertion: From the first to the last snapshot {1} triples were added (net)".format(end_vers, cnt_net_triples_added))        
    LOG.info("Assertion: The rdf-star dataset created with function construct_tb_star_ds should have {1} triples".format(end_vers, cnt_triples_rdf_star))
    LOG.info("Assertion: Triples that are still valid with the latest snapshot: {0}".format(cnt_valid_triples_last_ic))



# via composition from raw files
def construct_tb_star_ds(source_ic0: str, source_cs: str, destination: str,
                         last_version: int, init_timestamp: datetime):
    """
    Constructs an rdf-star dataset from the initial snapshot and the subsequent changesets and saves
    it to the :destination path.
    """

    init_timestamp_str = f'"{versioning_timestamp_format(init_timestamp)}"^^<http://www.w3.org/2001/XMLSchema#dateTime>'
    aet = '"9999-12-31T00:00:00.000+02:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>'

    # Read initial snapshot
    LOG.info("Read initial snapshot {0} into memory.".format(source_ic0))
    ic0_raw = open(source_ic0, "r").read().splitlines()
    ic0_list = list(filter(None, ic0_raw))
    ic0_list_clean = list(filter(lambda x: not x.startswith("# "), ic0_list))
    ic0_list_timestamped = [f"<< << {triple[:-1].strip()}>> <https://github.com/GreenfishK/DataCitation/versioning/valid_from> {init_timestamp_str}>> <https://github.com/GreenfishK/DataCitation/versioning/valid_until> {aet} .\n" for triple in ic0_list_clean]

    # Write timestamped snapshot
    LOG.info("Add triples from initial snapshot {0} as nested triples into the RDF-star dataset.".format(source_ic0)) 
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
        LOG.info(f"Memory in usage: {mem_in_usage}%")

        if filename.startswith("data-added"):
            LOG.info("Read positive changeset {0} into memory.".format(filename))
            added_triples_raw = open(source_cs + "/" + filename, "r").read().splitlines()
            added_triples_list = list(filter(None, added_triples_raw))
            added_triples_list_timestamped = [f"<< << {triple[:-1].strip()}>> <https://github.com/GreenfishK/DataCitation/versioning/valid_from> {vers_ts_str}>> <https://github.com/GreenfishK/DataCitation/versioning/valid_until> {aet} .\n" for triple in added_triples_list]
            cnt_trpls = len(added_triples_raw)

            LOG.info(f"Add {cnt_trpls} triples from changeset {filename} as nested triples into the RDF-star dataset.")
            with open(destination, "a") as rdf_star_dataset:
                rdf_star_dataset.writelines(added_triples_list_timestamped)
        
        if filename.startswith("data-deleted"):
            LOG.info(f"Read negative changeset {filename} into memory.")
            deleted_triples_raw = open(os.path.join(source_cs, filename), "r").read().splitlines()
            deleted_triples_list = list(filter(None, deleted_triples_raw))
            deleted_triples_set = set(t[:-1].strip() for t in deleted_triples_list)

            # Count triples to be invalidated
            cnt_trpls = len(deleted_triples_list)
            LOG.info(f"Invalidate {cnt_trpls} triples in the RDF-star dataset which match the triples in {filename}.")

            # Update aet timestamps for matching triples in the negative delta set
            LOG.info(f"Updating aet timestamps for matching triples in the negative delta set {filename}.")
            
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

            LOG.info(f"Writing updated RDF-star dataset to {destination}.")
            os.replace(tmp_out, destination)
            
            LOG.info(f"Invalidated {total_replacements} triples in the RDF-star dataset which match the triples in {filename}.")

def construct_tb_reif_ds(source_ic0: str, source_cs: str, destination: str,
                         last_version: int, init_timestamp: datetime):
    init_timestamp_str = f'"{versioning_timestamp_format(init_timestamp)}"^^<http://www.w3.org/2001/XMLSchema#dateTime>'
    aet = '"9999-12-31T00:00:00.000+02:00"^^<http://www.w3.org/2001/XMLSchema#dateTime>'

    # Read initial snapshot
    LOG.info("Read initial snapshot {0} into memory.".format(source_ic0))
    ic0_raw = open(source_ic0, "r").read().splitlines()
    ic0_list = list(filter(None, ic0_raw))
    ic0_list_clean = list(filter(lambda x: not x.startswith("# "), ic0_list))
    ic0_list_timestamped = [f"_:b rdf:reifies << {triple[:-1].strip()}>> ; <https://github.com/GreenfishK/DataCitation/versioning/valid_from> {init_timestamp_str}; <https://github.com/GreenfishK/DataCitation/versioning/valid_until> {aet} .\n" for triple in ic0_list_clean]

    # Write timestamped snapshot
    LOG.info("Add triples from initial snapshot {0} as nested triples into the RDF-star dataset.".format(source_ic0)) 
    with open(destination, "w") as rdf_star_dataset:
        rdf_star_dataset.writelines(["@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"])
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
        LOG.info(f"Memory in usage: {mem_in_usage}%")

        if filename.startswith("data-added"):
            LOG.info("Read positive changeset {0} into memory.".format(filename))
            added_triples_raw = open(source_cs + "/" + filename, "r").read().splitlines()
            added_triples_list = list(filter(None, added_triples_raw))
            added_triples_list_timestamped = [f"_:b rdf:reifies << {triple[:-1].strip()}>> ; <https://github.com/GreenfishK/DataCitation/versioning/valid_from> {vers_ts_str}; <https://github.com/GreenfishK/DataCitation/versioning/valid_until> {aet} .\n" for triple in added_triples_list]
            cnt_trpls = len(added_triples_raw)

            LOG.info(f"Add {cnt_trpls} triples from changeset {filename} as nested triples into the RDF-star dataset.")
            with open(destination, "a") as rdf_star_dataset:
                rdf_star_dataset.writelines(added_triples_list_timestamped)
        
        if filename.startswith("data-deleted"):
            LOG.info(f"Read negative changeset {filename} into memory.")
            deleted_triples_raw = open(os.path.join(source_cs, filename), "r").read().splitlines()
            deleted_triples_list = list(filter(None, deleted_triples_raw))
            deleted_triples_set = set(t[:-1].strip() for t in deleted_triples_list)

            # Count triples to be invalidated
            cnt_trpls = len(deleted_triples_list)
            LOG.info(f"Invalidate {cnt_trpls} triples in the RDF-star dataset which match the triples in {filename}.")

            # Update aet timestamps for matching triples in the negative delta set
            LOG.info(f"Updating aet timestamps for matching triples in the negative delta set {filename}.")
            
            valid_until = "<https://github.com/GreenfishK/DataCitation/versioning/valid_until>"
            valid_until_escaped = re.escape(valid_until)
            
            tmp_out = f"{data_dir}/alldata.TB_star_reif.tmp"
            total_replacements = 0
            with open(destination, "r") as rdf_star_graph, open(tmp_out, "w") as fout:
                for line in rdf_star_graph:
                    timestamped_triple = line.strip()
                    fact_triple = timestamped_triple.split(">> ; <https://github.com/GreenfishK/DataCitation/versioning/valid_from>")[0][19:] 

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

            LOG.info(f"Writing updated RDF-star dataset to {destination}.")
            os.replace(tmp_out, destination)
            
            LOG.info(f"Invalidated {total_replacements} triples in the RDF-star dataset which match the triples in {filename}.")
        
    # Iterate through all blank nodes and add a numerical suffix
    tmp_out = f"{data_dir}/alldata.TB_star_reif.tmp"
    with open(destination, "r") as rdf_star_graph, open(tmp_out, "w") as fout:
        for i, line in enumerate(rdf_star_graph):
            timestamped_triple = line.strip()

            # replace _:b with _:b{i}
            if timestamped_triple.startswith("_:b"):
                timestamped_triple = timestamped_triple.replace("_:b ", f"_:b{i} ", 1)

            fout.write(timestamped_triple + "\n")
    
    LOG.info(f"Writing updated RDF-star dataset with unique blank nodes to {destination}.")
    os.replace(tmp_out, destination)

    LOG.info(f"Finished creating the TB dataset variant with reification")

def construct_icng_ds(source: str, destination: str, last_version: int, basename_length: int):
    """
    TODO: write docu
    """

    LOG.info("Constructing the ICNG dataset with ICs as named graphs.")
    template = open("/starvers_eval/scripts/3_construct_datasets/templates/icng.txt", "r").read()
    if not os.path.exists(source):
        os.makedirs(source)

    LOG.info("Create empty dataset.")
    f = open(destination, "w")
    f.write("")
    f.close()

    for i in range(last_version):
        LOG.info("Building version {0}. ".format(str(i+1)))
        ic = open(source + "/" + str(i+1).zfill(basename_length)  + ".nt", "r").read()
    
        LOG.info("Write ic {} to data set.".format(str(i+1)))
        f = open(destination, "a")
        f.write(template.format(str(i), ic) + "\n")
        f.close()


def construct_TB(source_ic0: str, source_cs: str, destination: str, last_version: int, basename_length: int):
    """
    Creates the TB dataset variant that includes the version in which a triple was valid in the named grah identifier in the fourth position of a triple, according to the following example:
        ex:s1 ex:p1 ex:o1 :v21_22_23_25 .
        :v21_22_23_25 owl:versionInfo "21" :versions .
        :v21_22_23_25 owl:versionInfo "22" :versions .
        :v21_22_23_25 owl:versionInfo "23" :versions .
        :v21_22_23_25 owl:versionInfo "25" :versions .
    """

    # Read initial snapshot
    mem_in_usage = psutil.virtual_memory().percent
    LOG.info(f"Memory in usage: {mem_in_usage}%")

    LOG.info("Read initial snapshot {0} into memory.".format(source_ic0))
    ic0_raw = open(source_ic0, "r").read().splitlines()
    ic0_list = list(filter(None, ic0_raw))
    ic0_list_clean = list(filter(lambda x: not x.startswith("# "), ic0_list))

    triples_dict = {}

    for triple in ic0_list_clean:
        # Read all triples from the source dataset and add them to the dictionary with version 0. 
        # E.g. {"ex:s1 ex:p1 ex:o1": [0, 1]}
        # The first entry, 0, is the initial version. the second entry, 1, means that the triples are curerntly valid
        triples_dict[triple[:-1].strip()] = [[0], 1]

    LOG.info(f"Initial triples in the dictionary: {len(triples_dict)}")

    # Map versions to files in chronological orders
    change_sets = {}
    for filename in sorted(os.listdir(source_cs)):
        if not (filename.startswith("data-added") or filename.startswith("data-deleted")):
            continue
        version = int(filename.split('-')[2].split('.')[0].zfill(len(str(last_version)))) - 1
        change_sets[filename] = version

    mem_in_usage = psutil.virtual_memory().percent
    LOG.info(f"Memory in usage: {mem_in_usage}%")

    current_version = 0
    for filename, version in sorted(change_sets.items(), key=lambda item: item[1]):
        
        # Update the dictionary with the new version for all triples
        if current_version != version:
            #triples_dict = {triple: versions[0] = [version] for triple, versions in triples_dict.items() if versions[1] == 1}
            for triple, versions in triples_dict.items():
                if versions[1] == 1:
                    versions[0].append(version)
                    triples_dict[triple] = versions
        current_version = version

        if filename.startswith("data-deleted"):
            LOG.info(f"Read negative changeset {filename} into memory.")
            deleted_triples_raw = open(os.path.join(source_cs, filename), "r").read().splitlines()
            deleted_triples_list = list(filter(None, deleted_triples_raw))
            
            # For all triples in the data-deleted_{i}_{i+1}.nt change set, remove the version i+1-1=i from the list in the dictionary, e.g. {"ex:s1 ex:p1 ex:o1": [0]} becomes {"ex:s1 ex:p1 ex:o1": []} if the triple is deleted in version 1, or remains {"ex:s2 ex:p2 ex:o2": [1]} if the triple is not deleted in version 1.
            for triple in deleted_triples_list:
                triple_clean = triple[:-1].strip()
                LOG.info(f"Check triple {triple_clean}.")
                if triple_clean in triples_dict:
                    versions = triples_dict[triple_clean]
                    LOG.info(f"Remove last version for triple {triple_clean}: {versions[0]}.")

                    # Remove the last version from the list and set it to currently invalid (versions[1] = 0)
                    if versions[0]:
                        versions[0].pop()   # remove last version 
                    versions[1] = 0
                    triples_dict[triple_clean] = versions
            
            mem_in_usage = psutil.virtual_memory().percent
            LOG.info(f"Memory in usage: {mem_in_usage}%")
                   
                
        if filename.startswith("data-added"):
            LOG.info("Read positive changeset {0} into memory.".format(filename))
            added_triples_raw = open(source_cs + "/" + filename, "r").read().splitlines()
            added_triples_list = list(filter(None, added_triples_raw))

            # Add the triples in the data-added_{i}-{i+1}.nt change set to the dictionary with version i, e.g. {"ex:s1 ex:p1 ex:o1": [0, 1]} if the triple was already in the initial dataset and is added again in version 1, or {"ex:s2 ex:p2 ex:o2": [1]} if the triple is new in version 1.
            for triple in added_triples_list:
                triple_clean = triple[:-1].strip()
                LOG.info(f"Check triple {triple_clean}.")
                if triple_clean in triples_dict: # in case it has been previously added and then deleted
                    versions = triples_dict[triple_clean]
                    versions[0].append(version)
                    versions[1] = 1

                    triples_dict[triple_clean] = versions
                else:
                    triples_dict[triple_clean] = [[version], 1]

            mem_in_usage = psutil.virtual_memory().percent
            LOG.info(f"Memory in usage: {mem_in_usage}%")
    
    # Write the triples with their versions to the destination file in the format described above.
    LOG.info(f"Write the triples with their versions to the destination file {destination}.")
    version_strs = set()
    with open(destination, "w") as TB_dataset:
        # Write all fact triples with their version strings in the fourth position.
        for triple, versions in triples_dict.items():
            versions_str = "_".join(str(v) for v in versions[0])
            version_strs.add(versions_str)
            
            TB_dataset.write(f"{triple} <http://example.org/v{versions_str}> .\n")

        # Write all verison strings and map each version contained in the string to it
        for version_str in version_strs:
            # Extract versions from version_str
            versions = list(map(int, re.findall(r'\d+', version_str)))
            for version in versions:
                TB_dataset.write(f"<http://example.org/v{version_str}> <http://www.w3.org/2002/07/owl#versionInfo> \"{version}\"^^<http://www.w3.org/2001/XMLSchema#integer> <http://example.org/versions> .\n")




# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------
LOG.info(f"Starting procedure for constructing the different dataset variants for the datasets: {datasets}")
for dataset in datasets:
    if dataset not in allowed_datasets:
        logging.error("Dataset must be one of: ", allowed_datasets, "but is: {0}".format(dataset))
        break

    data_dir = f"{os.environ['RUN_DIR']}/rawdata/{dataset}"
    total_versions = dataset_versions[dataset]
    LOG.info("Constructing datasets for {0}".format(dataset))

    if not skip_change_sets == "True":
        construct_change_sets(snapshots_dir=f"{data_dir}/{snapshot_dir}", change_sets_dir=f"{data_dir}/{change_sets_dir}",
                                end_vers=total_versions, format=in_frm, basename_length=ic_basename_lengths[dataset])

    if not skip_tb_star_ds == "True":
        construct_tb_star_ds(source_ic0=f"{data_dir}/{snapshot_dir}/" + "1".zfill(ic_basename_lengths[dataset])  + ".nt",
                            source_cs=f"{data_dir}/{change_sets_dir}.{in_frm}",
                            destination=f"{data_dir}/alldata.TB_star_hierarchical.ttl",
                            last_version=total_versions,
                            init_timestamp=init_version_timestamp)    
    
    if not skip_tb_reif_ds == "True":
        construct_tb_reif_ds(source_ic0=f"{data_dir}/{snapshot_dir}/" + "1".zfill(ic_basename_lengths[dataset])  + ".nt",
                            source_cs=f"{data_dir}/{change_sets_dir}.{in_frm}",
                            destination=f"{data_dir}/alldata.TB_star_reif.ttl",
                            last_version=total_versions,
                            init_timestamp=init_version_timestamp)
    
    if not skip_icng_ds == "True":
        construct_icng_ds(source=f"{data_dir}/{snapshot_dir}/",
                        destination=f"{data_dir}/alldata.ICNG.trig",
                        last_version=total_versions,
                        basename_length=ic_basename_lengths[dataset])


    if not skip_tb_ds == "True":
        construct_TB(source_ic0=f"{data_dir}/{snapshot_dir}/" + "1".zfill(ic_basename_lengths[dataset])  + ".nt",
                     source_cs=f"{data_dir}/{change_sets_dir}.{in_frm}",
                     destination=f"{data_dir}/alldata.TB_computed.nq",
                     last_version=total_versions,
                     basename_length=ic_basename_lengths[dataset])
    
LOG.info("Finished with constructing datasets.")
