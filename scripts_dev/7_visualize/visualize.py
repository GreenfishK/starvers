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


def create_plots(dataset: str, query_set: str):
    """
    Create a figure with 4 subplots, one for each dataset. Each subplot shows 4 lines, one for each policy. 
    On the x-axis the snapshots are shown and on the y-axis the mean total execution time aggregated 
    on policy and snapshot level.

    """
    # Data
    performance_data = pd.read_csv(measurements_in + "time.csv", delimiter=";", decimal=".",
                            dtype={"triplestore": "category", "dataset": "category", "policy": "category",
                            "query_set": "category", "snapshot": "string", "query": "string",
                            "execution_time": "float", "snapshot_creation_time": "float"})
    performance_data['snapshot_ts'] = pd.to_datetime(performance_data['snapshot_ts'])
    performance_data['execution_time_total'] = performance_data['execution_time'] + performance_data['snapshot_creation_time']
    performance_data = performance_data[['triplestore', 'dataset', 'policy', 'snapshot', 'execution_time_total']]

    ingestion_data = pd.read_csv(measurements_in + "ingestion.csv", delimiter=";", decimal=".")

    # Parameters
    policies = ['ic_sr_ng', 'cb_sr_ng', 'tb_sr_ng', 'tb_sr_rs']
    colors = ['red', 'blue', 'green', 'purple']
    color_map = dict(zip(policies, colors))
    
    # Plot variables
    fig, axs = plt.subplots(2, 2, figsize=(15, 10))
    axs = axs.flatten()
    
       
    def plot_performance(triplestore: str, ax):
        dataset_df = performance_data[(performance_data['triplestore'] == triplestore) & (performance_data['dataset'] == dataset) & (performance_data['query_set'] == query_set)]
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

    plot_performance(triplestore="graphdb", ax=axs[0])
    plot_performance(triplestore="jenatdb2", ax=axs[1])

    def plot_ingestion(triplestore: str, ax):
        ax2 = ax.twinx()
        bar_width = 0.2
        opacity = 1
        index = range(len(policies))

        for i, policy in enumerate(policies):
            
            policy_data = ingestion_data[(ingestion_data['triplestore'] == triplestore) & (ingestion_data["policy"] == policy) & (ingestion_data['dataset'] == dataset)]
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

    plot_ingestion(triplestore="graphdb", ax=axs[2])
    plot_ingestion(triplestore="jenatdb2", ax=axs[3])

    
    # Add legend
    #red_patch = mpatches.Patch(color='red', label='ic_sr_ng')
    #blue_patch = mpatches.Patch(color='blue', label='cb_sr_ng')
    #green_patch = mpatches.Patch(color='green', label='tb_sr_ng')
    #purple_patch = mpatches.Patch(color='purple', label='tb_sr_rs')
    #fig.legend(loc="upper right", handles={red_patch, blue_patch, green_patch, purple_patch})

    fig.suptitle(f"{dataset} and {query_set} query performance and ingest plots", fontsize=32)
    fig.set_figheight(9)
    fig.set_figwidth(16)

    plt.tight_layout(pad=3.0, w_pad=2, h_pad=1.0)
    plt.savefig(f"/starvers_eval/output/figures/time_{dataset}_{query_set}.png")
    plt.close()

    

create_plots("bearb_hour", "lookup")
create_plots("bearb_hour", "join")
create_plots("bearb_day", "lookup")
create_plots("bearb_day", "join")
create_plots("bearbc", "complex")
