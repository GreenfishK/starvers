import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
from datetime import timezone
import os
from pathlib import Path


work_dir = str(Path.home()) + "/.BEAR"
figures_out = work_dir + "/output/figures/"

if not os.path.exists(figures_out):
    os.makedirs(figures_out)


def plot_mat_lookup_queries(timestamp: datetime, triple_store: str, triple_pattern: str):
    """
    Version materialisation queries with simple lookups for IC, CB and TB approaches.
    :param timestamp: Timestamp of evaluation. Can be derived from the folder where the output of the evaluation is
    stored.
    :param triple_pattern: (s), p, (o), (sp), po, (so), (spo)
    :param triple_store: JenaTDB, GraphDB
    :return:
    """
    # TODO: make a list of policies as argument and pass tb and tb_star.
    #  For each policy a curve should be plotted for the respective set of queries.

    hostname = os.uname().nodename
    ts_formatted = timestamp.strftime("%Y-%m-%dT%H:%M:%S")
    policies = ['tb', 'tb_star_f', 'tb_star_h']
    df = pd.DataFrame(columns=['policy', 'tripleStore', 'ver', 'min', 'mean', 'max', 'stddev', 'count', 'sum'])
    for policy in policies:
        output_dir = work_dir + "/output/time/bearb-hour/{hostname}-{timestamp}".format(hostname=hostname,
                                                                                    timestamp=ts_formatted)
        filename = "time-{policy}-mat-lookup_queries_{triple_pattern}.txt.csv".format(policy=policy,
                                                                                      triple_pattern=triple_pattern)
        df_temp = pd.read_csv("{dir}/{fn}".format(dir=output_dir, fn=filename), delimiter=",")
        df_temp.insert(0, 'policy', policy, True)
        df = df.append(df_temp, ignore_index=True)

    df.rename(columns={'count': 'cnt_queries', 'sum': 'total_time_in_ms'}, inplace=True)

    plt.figure(figsize=(24, 13.5))
    ax = plt.gca()
    df_grouped = df[df.tripleStore == triple_store].groupby('policy')
    for name, group in df_grouped:
        plt1 = group.plot(kind="line", x='ver', y='mean', label=name, ax=ax)
        color = plt1.lines[-1].get_color()
        ax.axhline(group['mean'].mean(), color=color)
    ax.set_title(triple_store)
    ax.set_xlabel("Version")
    ax.set_ylabel("total query time (in ms)")

    return plt


ts = datetime(2022, 1, 21, 10, 36, 39)

# Jena
# simple lookups, 49 queries
plt1 = plot_mat_lookup_queries(ts, "JenaTDB", "p").savefig(fname=figures_out + "JenaTDB_p_lookup", format='png', dpi='figure')
plt.close(plt1)
# other triple patterns, 13 queries
plt2 = plot_mat_lookup_queries(ts, "JenaTDB", "po").savefig(fname=figures_out + "JenaTDB_po_lookup", format='png', dpi='figure')
plt.close(plt2)

# GraphDB
# simple lookups, 49 queries
plt3 = plot_mat_lookup_queries(ts, "GraphDB", "p").savefig(fname=figures_out + "GraphDB_p_lookup", format='png', dpi='figure')
plt.close(plt3)

# other triple patterns, 13 queries
plt4 = plot_mat_lookup_queries(ts, "GraphDB", "po").savefig(fname=figures_out + "GraphDB_po_lookup", format='png', dpi='figure')
plt.close(plt4)
