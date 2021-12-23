from pathlib import Path
from datetime import datetime
from datetime import timedelta
import os
import pandas as pd
from rdflib import Graph

valid_from_predicate = "<https://github.com/GreenfishK/DataCitation/versioning/valid_from>"
valid_until_predicate = "<https://github.com/GreenfishK/DataCitation/versioning/valid_until>"
alldata_versioned_path = str(Path.home()) + "/.BEAR/rawdata-bearb/hour/alldata.TB_star.ttl"
sys_ts = datetime.now()

desired_width=320
pd.set_option('display.width', desired_width)
# np.set_printoption(linewidth=desired_width)
pd.set_option('display.max_columns', 10)


def construct_tb_star_ds(cb_rel_path: str):
    """
    :param: cb_rel_path: The name of the directory where the change sets are stored. This is not the absolute
    but only the relative path to "/.BEAR/rawdata-bearb/hour/

    :return:
    """

    """ Annotation of initial version triples """
    output_path = str(Path.home()) + "/.BEAR/rawdata-bearb/hour/alldata.TB_star.ttl"
    ic0_ds_path = str(Path.home()) + "/.BEAR/rawdata-bearb/hour/alldata.IC.nt/000001.nt"
    change_sets_path = str(Path.home()) + "/.BEAR/rawdata-bearb/hour/{0}".format(cb_rel_path)
    tz_offset = "+02:00"
    valid_ufn_ts = '9999-12-31T00:00:00.000'
    sys_ts_formatted = datetime.strftime(sys_ts, "%Y-%m-%dT%H:%M:%S.%f")[:-3]

    ic0 = Graph()
    ic0.parse(ic0_ds_path)

    init_ds = []
    for s, p, o in ic0:
        init_ds.append([s.n3(), p.n3(), o.n3(), valid_from_predicate, '"{ts}{tz_offset}"^^xsd:dateTime'.format(
            ts=sys_ts_formatted, tz_offset=tz_offset)])
        init_ds.append([s.n3(), p.n3(), o.n3(), valid_until_predicate, '"{ts}{tz_offset}"^^xsd:dateTime'.format(
            ts=valid_ufn_ts, tz_offset=tz_offset)])

    df_tb_set = pd.DataFrame(init_ds, columns=['s', 'p', 'o', 'vers_predicate', 'timestamp'])

    """ Loading change set files """
    # build list (version, filename_added, filename_deleted, version_timestamp)
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

    """ Annotation of change set triples """

    for t in change_sets:
        print("Change set between version {0} and {1} processing. ".format(int(t[0])-1, int(t[0])))
        
        """ Annotate added triples using rdf* syntax """
        cs_add = Graph()
        cs_add.parse(change_sets_path + "/" + t[1])
        for s, p, o in cs_add:
            df_tb_set.loc[len(df_tb_set)] = [s.n3(), p.n3(), o.n3(), valid_from_predicate,
                                             '"{ts}{tz_offset}"^^xsd:dateTime'.format(ts=t[3], tz_offset=tz_offset)]
            df_tb_set.loc[len(df_tb_set)] = [s.n3(), p.n3(), o.n3(), valid_until_predicate,
                                             '"{ts}{tz_offset}"^^xsd:dateTime'.format(ts=valid_ufn_ts,
                                                                                      tz_offset=tz_offset)]

        df_tb_set.set_index(['s', 'p', 'o', 'vers_predicate', 'timestamp'], drop=False, inplace=True)

        """ Annotate deleted triples using rdf* syntax """
        cs_del = Graph()
        cs_del.parse(change_sets_path + "/" + t[2])
        for s, p, o in cs_del:
            # Remove dot from statement
            df_tb_set.loc[(s.n3(), p.n3(), o.n3(), valid_until_predicate,
                           '"{ts}{tz_offset}"^^xsd:dateTime'.format(ts=valid_ufn_ts,
                                                                    tz_offset=tz_offset)), 'timestamp'] = \
                '"{ts}{tz_offset}"^^xsd:dateTime'.format(ts=t[3], tz_offset=tz_offset)

        print("Number of triples: {0}" .format(len(df_tb_set.query('timestamp == \'"{0}{1}"^^xsd:dateTime\''.format(valid_ufn_ts, tz_offset)))))

    """ Export dataset by reading out each line. Pandas does so far not provide any function to export to ttl oder n3"""
    print("Export data set.")
    f = open(output_path, "w")
    f.write("")
    f.close()
    with open(output_path, "a") as output_tb_ds:
        for index, row in df_tb_set.iterrows():
            triple = "{0} {1} {2}".format(row['s'], row['p'], row['o'])
            output_tb_ds.write("<<{triple}>> {vers_p} {ts} .\n".format(triple=triple,
                                                                       vers_p=row['vers_predicate'],
                                                                       ts=row['timestamp']))
        output_tb_ds.close()


# annotate_initial_set()
# Take the change sets that were manually computed from the ICs by compute_change_sets.py
construct_tb_star_ds("alldata.CB_computed.nt")
