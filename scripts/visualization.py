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


def plot_mat_lookup_queries(timestamp: datetime, triple_store: str, triple_patterns: list):
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
    df = pd.DataFrame(columns=['policy', 'tripleStore', 'triple_pattern', 'ver', 'min', 'mean', 'max', 'stddev', 'count', 'sum'])
    for policy in policies:
        output_dir = work_dir + "/output/time/bearb-hour/{hostname}-{timestamp}".format(hostname=hostname,
                                                                                    timestamp=ts_formatted)

        for pattern in triple_patterns:
            filename = "time-{policy}-mat-lookup_queries_{triple_pattern}.txt.csv".format(policy=policy,
                                                                                          triple_pattern=pattern)
            df_temp = pd.read_csv("{dir}/{fn}".format(dir=output_dir, fn=filename), delimiter=",")
            df_temp.insert(0, 'policy', policy, True)
            df_temp.insert(2, 'triple_pattern', pattern, True)
            df = df.append(df_temp, ignore_index=True)

    df.rename(columns={'count': 'cnt_queries', 'sum': 'total_time_in_ms'}, inplace=True)

    fig, (ax1, ax2) = plt.subplots(2)
    fig.set_size_inches(24, 13.5, forward=True)
    fig.text(0.08, 0.5, 'Total query time (in ms)', va='center', rotation='vertical', size=20)
    ax1.tick_params(axis='both', which='major', labelsize=14)
    ax2.tick_params(axis='both', which='major', labelsize=14)
    ax1.xaxis.label.set_size(20)
    ax2.xaxis.label.set_size(20)

    # ax1 = plt.gca()
    df1_grouped = df[(df.tripleStore == triple_store) & (df.triple_pattern == 'p')].groupby('policy')
    for name, group in df1_grouped:
        plt1 = group.plot(kind="line", x='ver', y='mean', label=name, ax=ax1)
        color1 = plt1.lines[-1].get_color()
        ax1.axhline(group['mean'].mean(), color=color1)
    ax1.set_xlabel("Version")

    df2_grouped = df[(df.tripleStore == triple_store) & (df.triple_pattern == 'po')].groupby('policy')
    for name, group in df2_grouped:
        plt2 = group.plot(kind="line", x='ver', y='mean', label=name, ax=ax2)
        color2 = plt2.lines[-1].get_color()
        ax2.axhline(group['mean'].mean(), color=color2)
    ax2.set_xlabel("Version")

    handles, labels = ax1.get_legend_handles_labels()
    labels = ["named graphs", "flat RDF*", "hierarchical RDF*"]
    ax1.get_legend().remove()
    ax2.get_legend().remove()
    plt.figlegend(handles, labels, loc='lower center', ncol=5, labelspacing=0. ,fontsize=16)

    plt.savefig(fname=figures_out + triple_store + "_TB_lookup.png", format='png', dpi='figure')


ts = datetime(2022, 1, 21, 10, 36, 39)

# Jena
# simple lookups, 49 p-queries and 13 po-queries
plot_mat_lookup_queries(ts, "JenaTDB", ["p", "po"])
plot_mat_lookup_queries(ts, "GraphDB", ["p", "po"])


