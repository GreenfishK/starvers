from pathlib import Path
from datetime import datetime
from datetime import timedelta
import os

valid_from_predicate = "<https://github.com/GreenfishK/DataCitation/versioning/valid_from>"
valid_until_predicate = "<https://github.com/GreenfishK/DataCitation/versioning/valid_until>"
alldata_versioned_path = str(Path.home()) + "/.BEAR/rawdata-bearb/hour/alldata.TB_star.ttl"
sys_ts = datetime.now()


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


def annotate_changeset(cb_rel_path: str):
    """
    :param: cb_rel_path: The name of the directory where the change sets are stored. This is not the absolute
    but only the relative path to "/.BEAR/rawdata-bearb/hour/

    :return:
    """

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

    # only for testing
    # change_sets = change_sets[0:10]

    # build dataset with rdf* annotations
    for t in change_sets:
        # annotate new triples
        print("Changeset version {0} processing".format(t[0]))
        tz_offset = "+02:00"
        with open(change_sets_path + "/" + t[1], "r") as cs:
            for triple in cs:
                # Remove dot from statement
                triple_trimmed = triple[:-2]

                alldata_versioned = open(alldata_versioned_path, "a")
                alldata_versioned.write('<<{triple}>> {vers_p} "{ts}{tz_offset}"^^xsd:dateTime .\n'.
                                        format(triple=triple_trimmed, ts=t[3], vers_p=valid_from_predicate,
                                               tz_offset=tz_offset))
                alldata_versioned.write('<<{triple}>> {vers_p} "9999-12-31T00:00:00.000+02:00"^^xsd:dateTime .\n'.
                                        format(triple=triple_trimmed, vers_p=valid_until_predicate))
                alldata_versioned.close()

        # annotate deleted triples
        with open(change_sets_path + "/" + t[2], "r") as cs:
            alldata_versioned = open(alldata_versioned_path, "r")
            alldata_versioned_new = alldata_versioned.read()
            for triple in cs:
                # Remove dot from statement
                triple_trimmed = triple[:-2]

                alldata_versioned_new = alldata_versioned_new.\
                    replace('<<{triple}>> {vers_p} "9999-12-31T00:00:00.000+02:00"^^xsd:dateTime .'.
                            format(triple=triple_trimmed, vers_p=valid_until_predicate),
                            '<<{triple}>> {vers_p} "{ts}{tz_offset}"^^xsd:dateTime .'.
                            format(triple=triple_trimmed, ts=t[3], vers_p=valid_until_predicate,
                                   tz_offset=tz_offset)
                            )
            alldata_versioned = open(alldata_versioned_path, "w")
            alldata_versioned.write(alldata_versioned_new)
            alldata_versioned.close()


annotate_initial_set()
# Take the change sets that were manually computed from the ICs by compute_change_sets.py
annotate_changeset("alldata.CB_computed.nt")
