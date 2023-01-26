import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
from datetime import datetime
from datetime import timezone
import os
from pathlib import Path
import sys


work_dir = "/starvers_eval/"
measurements_in = work_dir + "output/measurements/"
figures_out = work_dir + "output/figures/"
policies = sys.argv[1].split(" ")
datasets = sys.argv[2].split(" ")


if not os.path.exists(figures_out):
    os.makedirs(figures_out)

# Plots
# Plot1: filters: graphdb, bearb_day, lookup
# Plot1: measures: query execution time + snapshot creation time
# Plot1: x-axis: snapshots
# Plot1: lines: policy

# Plot2: filters: graphdb, bearb_day, join 
# Plot2: measures: query execution time + snapshot creation time
# Plot2: x-axis: snapshots
# Plot2: lines: policy

# Plot3: filters: graphdb, bearb_hour, lookup 
# Plot3: measures: query execution time + snapshot creation time
# Plot3: x-axis: snapshots
# Plot3: lines: policy

# Plot4: filters: graphdb, bearb_hour, join 
# Plot4: measures: query execution time + snapshot creation time
# Plot4: x-axis: snapshots
# Plot4: lines: policy

# Plot5: filters: graphdb, bearc, complex
# Plot5: measures: query execution time + snapshot creation time
# Plot5: x-axis: snapshots
# Plot5: lines: policy

# Plot6: filters: graphdb, bearb_day
# Plot6: measures: ingestion time, raw file size in MiB, db file size in MiB
# Plot6: x-axis: policy
# Plot6: type: bars

# Plot7: filters: graphdb, bearb_hour
# Plot7: measures: ingestion time, raw file size in MiB, db file size in MiB
# Plot7: x-axis: policy
# Plot7: type: bars

# Plot8: filters: graphdb, bearc
# Plot8: measures: ingestion time, raw file size in MiB, db file size in MiB
# Plot8: x-axis: policy
# Plot8: type: bars

# Plot9: filters: jenatdb2, bearb_day, lookup
# Plot9: measures: query execution time + snapshot creation time
# Plot9: x-axis: snapshots
# Plot9: lines: policy

# Plot10: filters: jenatdb2, bearb_day, join 
# Plot10: measures: query execution time + snapshot creation time
# Plot10: x-axis: snapshots
# Plot10: lines: policy

# Plot11: filters: jenatdb2, bearb_hour, lookup 
# Plot11: measures: query execution time + snapshot creation time
# Plot11: x-axis: snapshots
# Plot11: lines: policy

# Plot12: filters: jenatdb2, bearb_hour, join 
# Plot12: measures: query execution time + snapshot creation time
# Plot12: x-axis: snapshots
# Plot12: lines: policy

# Plot13: filters: jenatdb2, bearc, complex
# Plot13: measures: query execution time + snapshot creation time
# Plot13: x-axis: snapshots
# Plot13: lines: policy

# Plot14: filters: jenatdb2, bearb_day
# Plot14: measures: ingestion time, raw file size in MiB, db file size in MiB
# Plot14: x-axis: policy
# Plot14: type: bars

# Plot15: filters: jenatdb2, bearb_hour
# Plot15: measures: ingestion time, raw file size in MiB, db file size in MiB
# Plot15: x-axis: policy
# Plot15: type: bars

# Plot16: filters: jenatdb2, bearc
# Plot16: measures: ingestion time, raw file size in MiB, db file size in MiB
# Plot16: x-axis: policy
# Plot16: type: bars

def plot_lookup_queries(timestamp: datetime, triple_store: str, triple_patterns: list):
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
    df = pd.DataFrame(columns=['triplestore','dataset','policy','query_set','snapshot','query',
    'execution_time','snapshot_creation_time'])
    for policy in policies:
        output_dir = work_dir + "/output/time/bearb_hour/{hostname}-{timestamp}".format(hostname=hostname,
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
    fig.text(0.08, 0.5, 'Mean query time (in ms)', va='center', rotation='vertical', size=20)
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
plot_lookup_queries(ts, "JenaTDB", ["p", "po"])
plot_lookup_queries(ts, "GraphDB", ["p", "po"])


