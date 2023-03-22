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
import logging
import itertools

############################################# Logging #############################################
if not os.path.exists('/starvers_eval/output/logs/visualize'):
    os.makedirs('/starvers_eval/output/logs/visualize')
with open('/starvers_eval/output/logs/visualize/visualize.txt', "w") as log_file:
    log_file.write("")
logging.basicConfig(handlers=[logging.FileHandler(filename="/starvers_eval/output/logs/visualize/visualize.txt", 
                                                  encoding='utf-8', mode='a+')],
                    format="%(asctime)s %(name)s:%(levelname)s:%(message)s", 
                    datefmt="%F %A %T", 
                    level=logging.INFO)

############################################# Parameters #############################################
work_dir = "/starvers_eval/"
measurements_in = work_dir + "output/measurements/"
figures_out = work_dir + "output/figures/"
policies = sys.argv[1].split(" ")
datasets = sys.argv[2].split(" ")

############################################# Visualize #############################################

    
def create_plots(triplestore: str, dataset: str):
    """


    """
    # Data
    performance_data = pd.read_csv(measurements_in + "time.csv", delimiter=";", decimal=".",
                            dtype={"triplestore": "category", "dataset": "category", "policy": "category",
                            "query_set": "category", "snapshot": "string", "query": "string",
                            "execution_time": "float", "snapshot_creation_time": "float"})
    performance_data['snapshot_ts'] = pd.to_datetime(performance_data['snapshot_ts'])
    performance_data['execution_time_total'] = performance_data['execution_time'] + performance_data['snapshot_creation_time']
    performance_data = performance_data[['triplestore', 'dataset', 'policy', 'snapshot', 'query_set', 'execution_time_total']]

    ingestion_data = pd.read_csv(measurements_in + "ingestion.csv", delimiter=";", decimal=".")
    ingestion_data['triplestore'] = ingestion_data['triplestore'].str.lower()

    # Parameters
    policies = ['ic_sr_ng', 'cb_sr_ng', 'tb_sr_ng', 'tb_sr_rs']
    colors = ['red', 'blue', 'green', 'purple']
    color_map = dict(zip(policies, colors))

    # Figure and axes
    fig = plt.figure()
    gs = fig.add_gridspec(2,2)    
       
    def plot_performance(query_set: str, ax):
        dataset_df = performance_data[(performance_data['triplestore'] == triplestore) & (performance_data['dataset'] == dataset) & (performance_data['query_set'] == query_set)]
        means = dataset_df.groupby(['policy', 'snapshot']).mean()
        means = means.reset_index()
        
        for policy in policies:
            policy_df = means[means['policy'] == policy]
            ax.set_yscale('log')
            ax.plot(policy_df['snapshot'], policy_df['execution_time_total'], label=policy, color=color_map[policy])
        
        ax.set_title(f"Query set: {query_set}")
        ax.set_xlabel('snapshots')
        ax.set_ylabel('Execution time (s)')
        tick_steps = max(math.floor(len(policy_df['snapshot'])/10), 1)
        ax.set_xticks(ticks=range(0, len(policy_df['snapshot']), tick_steps),
                    labels=[*range(0, len(policy_df['snapshot']), tick_steps)])

    query_sets = performance_data[performance_data['dataset'] == dataset]['query_set'].unique()
    if len(query_sets) == 1:
        ax = fig.add_subplot(gs[0, :])
        plot_performance(query_set=query_sets[0], ax=ax)
    else:
        assert len(query_sets) == 2
        ax1 = fig.add_subplot(gs[0, 0])
        plot_performance(query_set=query_sets[0], ax=ax1)
        ax2 = fig.add_subplot(gs[0, 1])
        plot_performance(query_set=query_sets[1], ax=ax2)


    def plot_ingestion(ax, ax2):
        bar_width = 0.2
        spacing = 0.1
        opacity = 1
        index = range(len(policies))

        for i, policy in enumerate(policies):
            policy_data = ingestion_data[(ingestion_data['triplestore'] == triplestore) & (ingestion_data["policy"] == policy) & (ingestion_data['dataset'] == dataset)]
            raw_size = policy_data["raw_file_size_MiB"].mean()
            db_size = policy_data["db_files_disk_usage_MiB"].mean()
            
            ax.boxplot(policy_data["ingestion_time"], positions=[i], widths=0.8)

            ax2.bar(i, raw_size, bar_width, alpha=opacity, color='limegreen', label="Raw File Size")
            ax2.bar(i + bar_width + spacing, db_size, bar_width, alpha=opacity, color='darkgreen', label="DB File Size")
            ax2.text(i, raw_size, "{:.2f}".format(raw_size), ha='center', va='bottom')
            ax2.text(i + bar_width + spacing, db_size, "{:.2f}".format(db_size), ha='center', va='bottom')
        
        ax.set_xticks(index)
        ax.set_xticklabels(policies)
        #ax.set_title("")
        ax.set_xlabel("Policies")
        ax.set_ylabel("Ingestion Time (s)")
        ax.yaxis.label.set_color('coral')

        ax2.set_xticks([bar_width/2 + spacing/2 + x for x in range(len(policies))])
        ax2.set_xticklabels(policies)
        ax2.set_xlabel("Policies")
        ax2.set_ylabel("Storage Consumption (MiB)")
        ax2.yaxis.label.set_color('darkgreen')

    ax3 = fig.add_subplot(gs[1, 0])
    ax4 = fig.add_subplot(gs[1, 1])
    plot_ingestion(ax=ax3, ax2=ax4)

    
    # Add legend
    red_patch = mpatches.Patch(color='red', label='ic_sr_ng')
    blue_patch = mpatches.Patch(color='blue', label='cb_sr_ng')
    green_patch = mpatches.Patch(color='green', label='tb_sr_ng')
    purple_patch = mpatches.Patch(color='purple', label='tb_sr_rs')

    limegreen_patch = mpatches.Patch(color='limegreen', label='Raw File Size')
    darkgreen_patch = mpatches.Patch(color='darkgreen', label='DB File Size')
    coral_patch = mpatches.Patch(color='coral', label='Ingestion Time')

    handles1 = [red_patch, blue_patch, green_patch, purple_patch]
    handles2 = [coral_patch, limegreen_patch, darkgreen_patch]
    fixed_labels = ['coral_patch', 'limegreen_patch', 'darkgreen_patch']


    fig.legend(loc="upper right", ncol=4, handles=sorted(handles1, key=lambda x: x.get_label()))
    fig.legend(loc="lower right", ncol=3, handles=sorted(handles2, key=lambda x: fixed_labels.index(x.get_label()) if x.get_label() in fixed_labels else len(fixed_labels)))

    #fig.suptitle(f"Query performance and data ingestion & storage plots \nfor {triplestore} and {dataset}", fontsize=24)
    fig.set_figheight(9)
    fig.set_figwidth(16)

    plt.tight_layout(pad=3.0, w_pad=2, h_pad=1.0)
    plt.savefig(f"/starvers_eval/output/figures/time_{triplestore}_{dataset}.png")
    plt.close()


args = itertools.product(['graphdb', 'jenatdb2'], datasets)
list(map(lambda x: create_plots(*x), args))

