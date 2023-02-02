import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
from datetime import datetime
from datetime import timezone
import os
from pathlib import Path
import sys
import math


work_dir = "/starvers_eval/"
measurements_in = work_dir + "output/measurements/"
figures_out = work_dir + "output/figures/"
policies = sys.argv[1].split(" ")
datasets = sys.argv[2].split(" ")

if not os.path.exists(figures_out):
    os.makedirs(figures_out)

"""
Consider the following header of a .csv file: triplestore;dataset;policy;query_set;snapshot;query;execution_time_total

write a python function def create_query_performance_plots(triplestore: str, query_set: str) that creates a figure with 4 subplots. Each subplot should be dedicated to one dataset. dataset is a categorical variable from the csv file and there are 4 datasets bearb_hour, bearb_day, bearc and beara. The title of each subplot should also be the name of the dataset. On the x-axis of each subplot the snapshot variable should be plotted. snapshot is a numerical variable with natural numbers. Each plot should be a line-plot. The y-axis should show the mean value for all queries (=query variable in the csv file) of execution_time_total. For each policy there should be a line. policy is a categorical variable and there are 4 policies, namely, ic_sr_ng, cb_sr_ng, tb_sr_ng, tb_sr_rs. Add a legend to the figure which contains 4 entries, one for each policy. Each line in the plot/each policy should have a different color. Across the subplots the colors for each policy should be the same. The title of the figure should be derived from query_set and triplestore. 
"""
# Plots for query performance measurements
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

# Plot6: filters: jenatdb2, bearb_day, lookup
# Plot6: measures: query execution time + snapshot creation time
# Plot6: x-axis: snapshots
# Plot6: lines: policy

# Plot7: filters: jenatdb2, bearb_day, join 
# Plot7: measures: query execution time + snapshot creation time
# Plot7: x-axis: snapshots
# Plot7: lines: policy

# Plot8: filters: jenatdb2, bearb_hour, lookup 
# Plot8: measures: query execution time + snapshot creation time
# Plot8: x-axis: snapshots
# Plot8: lines: policy

# Plot9: filters: jenatdb2, bearb_hour, join 
# Plot9: measures: query execution time + snapshot creation time
# Plot9: x-axis: snapshots
# Plot9: lines: policy

# Plot10: filters: jenatdb2, bearc, complex
# Plot10: measures: query execution time + snapshot creation time
# Plot10: x-axis: snapshots
# Plot10: lines: policy




def create_ingest_plots(triplestore: str):
    """
    Create a figure with 4 subplots, one for each dataset. Each subplot shows three measures - 
    ingestion_time, raw_file_size_MiB and db_files_disk_usage_MiB as a grouped bars. Each group represents 
    one policy and the groups are visually separated. Each plot has two y-axes, one for the ingestion time and 
    one for the storage consumption.

    """
    # Read data for ingestion measures
    ingestion_data = pd.read_csv(measurements_in + "ingestion.csv", delimiter=";", decimal=".")
    
    fig, axs = plt.subplots(2, 2)
    
    bar_width = 0.2
    opacity = 1
    index = range(len(policies))

    for j, dataset in enumerate(datasets):
        ax = axs.flat[j]
        ax2 = ax.twinx()
        for i, policy in enumerate(policies):
            
            policy_data = ingestion_data[(ingestion_data["policy"] == policy) & (ingestion_data['dataset'] == dataset)]
            ing_time = policy_data["ingestion_time"].mean()
            raw_size = policy_data["raw_file_size_MiB"].mean()
            db_size = policy_data["db_files_disk_usage_MiB"].mean()
            
            ax.bar(i - bar_width, ing_time, bar_width, alpha=opacity, color='blue', label="Ingestion Time")
            ax2.bar(i, raw_size, bar_width, alpha=opacity, color='limegreen', label="Raw File Size")
            ax2.bar(i, db_size, bar_width * 0.6, alpha=opacity, color='darkgreen', label="DB File Size")
        
        ax.set_xticks(index)
        ax.yaxis.label.set_color('blue')
        ax.set_xticklabels(policies)
        ax.set_title(dataset)
        #ax.set_xlabel("Policy")
        ax.set_ylabel("Ingestion Time (s)")

        ax2.set_ylabel("Storage Consumption (MiB)")
        ax2.yaxis.label.set_color('darkgreen')


    # Add legend to figure
    limegreen_patch = mpatches.Patch(color='limegreen', label='Raw File Size')
    darkgreen_patch = mpatches.Patch(color='darkgreen', label='DB File Size')
    blue_patch = mpatches.Patch(color='blue', label='Ingestion Time')
    fig.legend(loc='upper right', handles={limegreen_patch, darkgreen_patch, blue_patch})
    
    fig.suptitle(f"{triplestore}", fontsize=32)
    fig.set_figheight(9)
    fig.set_figwidth(16)

    plt.tight_layout(pad=3.0, w_pad=2, h_pad=1.0)
    plt.savefig(f"/starvers_eval/output/figures/ingestion_{triplestore}.png")
    plt.close()


def create_query_performance_plots(triplestore: str):
    """
    Create a figure with 4 subplots, one for each dataset. Each subplot shows 4 lines, one for each policy. 
    On the x-axis the snapshots are shown and on the y-axis the mean total execution time aggregated 
    on policy and snapshot level.

    """
    performance_data = pd.read_csv(measurements_in + "time.csv", delimiter=";", decimal=".",
                            dtype={"triple_store": "category", "dataset": "category", "policy": "category",
                            "query_set": "category", "snapshot": "string", "query": "string",
                            "execution_time": "float", "snapshot_creation_time": "float"})
    performance_data['snapshot_ts'] = pd.to_datetime(performance_data['snapshot_ts'])
    performance_data['execution_time_total'] = performance_data['execution_time'] + performance_data['snapshot_creation_time']
    performance_data = performance_data[['dataset', 'policy', 'snapshot', 'execution_time_total']]

    datasets = ['bearb_hour', 'bearb_day', 'bearc', 'beara']
    policies = ['ic_sr_ng', 'cb_sr_ng', 'tb_sr_ng', 'tb_sr_rs']
    colors = ['red', 'blue', 'green', 'purple']
    color_map = dict(zip(policies, colors))
    
    fig, axs = plt.subplots(2, 2, figsize=(15, 10))
    axs = axs.flatten()
    
    for i, dataset in enumerate(datasets):
        ax = axs[i]
        dataset_df = performance_data[performance_data['dataset'] == dataset]
        means = dataset_df.groupby(['policy', 'snapshot']).mean()
        means = means.reset_index()
        
        for policy in policies:
            policy_df = means[means['policy'] == policy]
            ax.plot(policy_df['snapshot'], policy_df['execution_time_total'], label=policy, color=color_map[policy])
        
        ax.set_title(dataset)
        ax.set_xlabel('snapshot')
        ax.set_ylabel('execution_time_total')
        tick_steps = max(math.floor(len(policy_df['snapshot'])/10), 1)
        ax.set_xticks(ticks=range(0, len(policy_df['snapshot']), tick_steps),
                      labels=[*range(0, len(policy_df['snapshot']), tick_steps)])

    
    # Add legend
    red_patch = mpatches.Patch(color='red', label='ic_sr_ng')
    blue_patch = mpatches.Patch(color='blue', label='cb_sr_ng')
    green_patch = mpatches.Patch(color='green', label='tb_sr_ng')
    purple_patch = mpatches.Patch(color='purple', label='tb_sr_rs')
    fig.legend(loc="upper right", handles={red_patch, blue_patch, green_patch, purple_patch})

    fig.suptitle(f"{triplestore} query performance plots", fontsize=32)
    fig.set_figheight(9)
    fig.set_figwidth(16)

    plt.tight_layout(pad=3.0, w_pad=2, h_pad=1.0)
    plt.savefig(f"/starvers_eval/output/figures/time_{triplestore}.png")
    plt.close()

    

#create_ingest_plots("graphdb")
#create_ingest_plots("jenatdb2")
create_query_performance_plots("graphdb")
create_query_performance_plots("jenatdb2")