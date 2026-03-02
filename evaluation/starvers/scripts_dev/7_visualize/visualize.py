import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.lines as mlines
import numpy as np
from datetime import datetime
from datetime import timezone
import os
from pathlib import Path
import sys
import math
import logging
import itertools
import re
import numpy as np

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
figures_out = work_dir + "output/figures"
tables_out = work_dir + "output/tables"
policies = sys.argv[1].split(" ")
datasets = sys.argv[2].split(" ")

############################################# Visualize #############################################
pd.set_option('display.max_columns', None)




def create_plots(triplestore: str, dataset: str):
    """
    Create and save performance and ingestion plots for the specified triplestore and dataset.
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
    policies = ['ic_sr_ng', 'cb_sr_ng', 'tb_sr_ng', 'tb_sr_rs', 'ostrich']

    # Figure and axes for query performance and ingestion
    fig = plt.figure()
    gs = fig.add_gridspec(2, 2)   
    symbol_map = dict(zip(policies, ['s', 'o', 'v', '*', '^']))
    markerfacecolors = dict(zip(policies, ['none', 'none', 'none', 'black', 'none']))

    def plot_performance(query_set: str, ax):
        dataset_df = performance_data[(performance_data['triplestore'] == triplestore) & (performance_data['dataset'] == dataset) & (performance_data['query_set'] == query_set)]
        means = dataset_df.groupby(['policy', 'snapshot']).mean()
        means = means.reset_index()
        
        for policy in policies:
            policy_df = means[means['policy'] == policy]
            if dataset == 'bearc':
                markevery = math.ceil(len(policy_df['snapshot'])/120)
            else:
                markevery = math.ceil(len(policy_df['snapshot'])/60)
            ax.set_yscale('log')
            ax.plot(policy_df['snapshot'], policy_df['execution_time_total'], linestyle='none',
                marker=symbol_map[policy], markersize=7, markerfacecolor=markerfacecolors[policy], markeredgewidth=1, drawstyle='steps', linewidth=0.5,
                label=policy, color='black', markevery=markevery)

        
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
            
            ax.boxplot(policy_data["ingestion_time"], positions=[i], widths=0.8, medianprops=dict(color='black', linestyle='--'))
            
            # Raw File Size Bar (dashed filling)
            ax2.bar(i, raw_size, bar_width, alpha=opacity, hatch='/', color='white', edgecolor='black', label="Raw File Size")

            # DB File Size Bar (solid black)
            ax2.bar(i + bar_width + spacing, db_size, bar_width, alpha=opacity, color='black', label="DB File Size")

            ax2.text(i, raw_size, "{:.2f}".format(raw_size), ha='center', va='bottom')
            ax2.text(i + bar_width + spacing, db_size, "{:.2f}".format(db_size), ha='center', va='bottom')
    
        ax.set_xticks(index)
        ax.set_xticklabels(policies)
        ax.set_xlabel("Policies")
        ax.set_ylabel("Ingestion Time (s)")

        ax2.set_xticks([bar_width/2 + spacing/2 + x for x in range(len(policies))])
        ax2.set_xticklabels(policies)
        ax2.set_xlabel("Policies")
        ax2.set_ylabel("Storage Consumption (MiB)")

    ax3 = fig.add_subplot(gs[1, 0])
    ax4 = fig.add_subplot(gs[1, 1])
    plot_ingestion(ax=ax3, ax2=ax4)

    ###############################
    # Plot tuning and export
    ###############################

    # Add legend
    ic_sr_ng_line = mlines.Line2D([], [], color='black', marker='s', linestyle='None', markersize=7, markeredgecolor='black', markerfacecolor='none', label='ic_sr_ng')
    cb_sr_ng_line = mlines.Line2D([], [], color='black', marker='o', linestyle='None', markersize=7, markeredgecolor='black', markerfacecolor='none',label='cb_sr_ng')
    tb_sr_ng_line = mlines.Line2D([], [], color='black', marker='v', linestyle='None', markersize=7, markeredgecolor='black', markerfacecolor='none',label='tb_sr_ng')
    tb_sr_rs_line = mlines.Line2D([], [], color='black', marker='*', linestyle='None', markersize=7, markeredgecolor='black', markerfacecolor='black',label='tb_sr_rs')
    ostrich = mlines.Line2D([], [], color='black', marker='^', linestyle='None', markersize=7, markeredgecolor='black', markerfacecolor='none',label='ostrich')
    
    raw_file_size_patch = mpatches.Patch(facecolor='white', edgecolor='black', hatch='/', label='Raw File Size')
    db_file_size_path = mpatches.Patch(color='black', label='DB File Size')

    handles1 = [ic_sr_ng_line, cb_sr_ng_line, tb_sr_ng_line, tb_sr_rs_line, ostrich]
    handles2 = [raw_file_size_patch, db_file_size_path]
    fixed_labels = ['raw_file_size_patch', 'db_file_size_path']

    fig.legend(loc="upper right", ncol=4, handles=sorted(handles1, key=lambda x: x.get_label()))
    fig.legend(loc="lower right", ncol=3, handles=sorted(handles2, key=lambda x: fixed_labels.index(x.get_label()) if x.get_label() in fixed_labels else len(fixed_labels)))

    fig.set_figheight(9)
    fig.set_figwidth(16)

    plt.tight_layout(pad=3.0, w_pad=2, h_pad=1.0)
    plt.savefig(f"{figures_out}/time_{triplestore}_{dataset}.png")
    plt.close()

def create_plots_update(triplestore: str, dataset: str):
    # Data
    performance_update_data = pd.read_csv(measurements_in + "time_update.csv", delimiter=";", decimal=".",
                            dtype={"triplestore": "category", "dataset": "category", "batch": "category",
                            "cnt_batch_trpls": "int", "chunk_size": "category", "execution_time": "float"})
    
    triplestores_map = {'GRAPHDB': 'GraphDB', 'JENA': 'Jena', 'OSTRICH': 'Ostrich'}
    
    # Figure and axes for update performance
    fig = plt.figure()
    gs = fig.add_gridspec(2,1)   

    def plot_performance_update(ax1, ax2):
        data = performance_update_data[(performance_update_data['triplestore'] == triplestore) & (performance_update_data['dataset'] == dataset)]

        chunk_sizes = data['chunk_size'].unique().to_list()
        # Select chunk sizes 1000, 4000, and 8000:
        chunk_sizes = [chunk_sizes[0], chunk_sizes[3], chunk_sizes[7]]

        # Linestyles
        linestyles = ['solid', 'dotted', 'dashed']
        for i, chunk_size in enumerate(chunk_sizes):
            data_chunk_size = data[data['chunk_size']==chunk_size]
            data_add = data_chunk_size.query('batch.str.startswith("snapshot") | batch.str.startswith("positive")')
            data_delete = data_chunk_size.query('batch.str.startswith("snapshot") | batch.str.startswith("negative")')
            labels_add = data_add['batch'].str.split("_").str[-1].astype(str) + "\n" + np.floor(data_add['cnt_batch_trpls']/1000).astype(int).astype(str) + "k"
            labels_delete = data_delete['batch'].str.split("_").str[-1].astype(str) + "\n" + np.floor(data_delete['cnt_batch_trpls']/1000).astype(int).astype(str) + "k"
            
            ax1.set_xticks([i for i in range(len(data_add))])
            ax1.set_xticklabels(labels_add)
            ax2.set_xticks([i for i in range(len(data_delete))])
            ax2.set_xticklabels(labels_delete)
            ax1.plot(labels_add, data_add['execution_time'], label=str(chunk_size), linestyle=linestyles[i], color='black') 
            ax2.plot(labels_delete, data_delete['execution_time'], label=str(chunk_size), linestyle=linestyles[i], color='black')

    ax1 = fig.add_subplot(gs[0,0])
    ax2 = fig.add_subplot(gs[1,0])
    plot_performance_update(ax1, ax2)

    ###############################
    # Plot tuning and export
    ###############################

    ax1.set_ylabel('Execution Time in s')
    ax1.set_xlabel('Batch number and number of triples in batch')
    ax1.set_title('Insert')
    ax1.legend(loc='upper right')

    ax2.set_ylabel('Execution Time in s')
    ax2.set_xlabel('Batch number and number of triples in batch')
    ax2.set_title('Invalidate')
    ax2.legend(loc='upper right')

    fig.set_figheight(9)
    fig.set_figwidth(16)

    fig.suptitle(f'Insert and Invalidate performance for a range of chunk sizes (1000-8000) for the {dataset.upper()} dataset and {triplestores_map[triplestore]}')
    
    plt.tight_layout(pad=3.0, w_pad=2, h_pad=1.0)
    plt.savefig(f"{figures_out}/time_update_{triplestore.lower()}_{dataset}.svg", format='svg')
    plt.close()


def create_latex_tables():
    """
    Create and save two LaTeX tables:
    1. Query performance (min/avg/max + timeout count) per query_set, policy, and triple store
    2. Storage + ingestion metrics per policy and triple store
    """

    # =========================
    # Parameters
    # =========================
    datasets = set(sys.argv[1].split(" "))
    policies = set(sys.argv[2].split(" "))
    triplestores = set(sys.argv[3].split(" "))
    query_sets = ["lookup", "join", "complex"]

    # =========================
    # Load data
    # =========================
    queries_data = pd.read_csv(
        measurements_in + "time_20260222T1755.csv",
        delimiter=";",
        decimal=".",
        dtype={
            "triplestore": "category",
            "dataset": "category",
            "policy": "category",
            "query_set": "category",
            "snapshot": "string",
            "query": "string",
            "execution_time": "float",
            "snapshot_creation_time": "float",
            "yn_timeout": "float",  # 0/1
        },
        parse_dates=["snapshot_ts"],
    )
    
    query_build_time_data = pd.read_csv(measurements_in + "query_rewriting_times.csv", delimiter=",", decimal=".",
        dtype={
            "dataset": "category",
            "policy": "category",
            "query_set": "category",
            "snapshot": "string",
            "query": "string",
            "rewriting_time": "float"})

    queries_data = queries_data.merge(query_build_time_data, on=["dataset", "policy", "query_set", "snapshot", "query"], how="left")
    queries_data["rewriting_time"] = queries_data["rewriting_time"].fillna(0)

    queries_data["execution_time_total"] = queries_data["execution_time"] + queries_data["snapshot_creation_time"] + queries_data["rewriting_time"]
    

    ingestion_data = pd.read_csv(
        measurements_in + "ingestion.csv", delimiter=";", decimal="."
    )
    ingestion_data["triplestore"] = ingestion_data["triplestore"].str.lower()

    # =========================
    # Load template
    # =========================
    with open(f"{work_dir}scripts/7_visualize/templates/latex_table_results_tmpl.tex", "r") as f:
        template_results = f.read()

    # =========================
    # Aggregation
    # =========================

    # Performance per dataset, policy, query_set, triplestore
    queries_data["execution_time_total_clean"] = queries_data["execution_time_total"].replace(-1, np.nan)   

    queries_agg = queries_data.groupby(["triplestore", "dataset", "policy", "query_set"]).agg(
        min=("execution_time_total_clean", "min"),
        avg=("execution_time_total_clean", "mean"),
        max=("execution_time_total_clean", "max"),
        cnt_timeout=("yn_timeout", "sum")
    ).reset_index()
    logging.info(f"Aggregated measures:\n{queries_agg}")

    queries_agg = queries_agg[queries_agg["min"].notna()]

    queries_agg.to_csv("/starvers_eval/output/logs/visualize/queries.csv", index=False)

    # Storage
    storage_agg = ingestion_data.groupby(["triplestore", "dataset", "policy"]).agg(
        ingestion_time=("ingestion_time", "median"),
        raw_file_size=("raw_file_size_MiB", "mean"),
        db_file_size=("db_files_disk_usage_MiB", "mean")
    ).reset_index()
    storage_agg.to_csv("/starvers_eval/output/logs/visualize/storage.csv", index=False)

    # TODO: Build dataframe with the same index structure as the latex table
    
    

    # =========================
    # Formatting helpers
    # =========================
    def format_exec_time(v):
        if v < 1:
            return f"{v:.2f}".lstrip("0")
        else:
            return f"{v:.1f}"

    def format_storage(v):
        return f"{(v / 1024):.2f}"

    def format_ingestion(v):
        return f"{v:.1f}"

    # =========================
    # Fill placeholders
    # =========================
    placeholder_map = {}

    for dataset in datasets:
        # logical size placeholder
        for policy in policies:
            for store in triplestores:
                match_storage = storage_agg[
                    (storage_agg["dataset"] == dataset) &
                    (storage_agg["policy"] == policy) &
                    (storage_agg["triplestore"] == store)
                ]
                if not match_storage.empty:
                    storage_val_raw = format_storage(match_storage["raw_file_size"].values[0])
                    storage_val_db = format_storage(match_storage["db_file_size"].values[0])
                else:
                    storage_val_raw = "x"
                    storage_val_db = "x"
                logging.info(f"Replacing {dataset}_{store}_{policy}_raw with {storage_val_raw}")
                logging.info(f"Replacing {dataset}_{store}_{policy}_storage with {storage_val_db}")
                
                placeholder_map[f"{{{{{dataset}_{store}_{policy}_raw}}}}"] = storage_val_raw
                placeholder_map[f"{{{{{dataset}_{store}_{policy}_storage}}}}"] = storage_val_db

        # query placeholders
        for query_set in query_sets:
            for store in triplestores:
                for policy in policies:
                    match_query = queries_agg[
                        (queries_agg["dataset"] == dataset) &
                        (queries_agg["query_set"] == query_set) &
                        (queries_agg["triplestore"] == store) &
                        (queries_agg["policy"] == policy)
                    ]
                    if not match_query.empty:
                        logging.info(f"Query metrics for {dataset}_{store}_{query_set}_{policy}: {match_query}")
                        placeholder_map[f"{{{{{dataset}_{store}_{query_set}_{policy}_min}}}}"] = format_exec_time(match_query["min"].values[0])
                        placeholder_map[f"{{{{{dataset}_{store}_{query_set}_{policy}_avg}}}}"] = format_exec_time(match_query["avg"].values[0])
                        placeholder_map[f"{{{{{dataset}_{store}_{query_set}_{policy}_max}}}}"] = format_exec_time(match_query["max"].values[0])
                        placeholder_map[f"{{{{{dataset}_{store}_{query_set}_{policy}_to}}}}"] = str(int(match_query["cnt_timeout"].values[0]))
                    else:
                        placeholder_map[f"{{{{{dataset}_{store}_{query_set}_{policy}_min}}}}"] = "x"
                        placeholder_map[f"{{{{{dataset}_{store}_{query_set}_{policy}_avg}}}}"] = "x"
                        placeholder_map[f"{{{{{dataset}_{store}_{query_set}_{policy}_max}}}}"] = "x"
                        placeholder_map[f"{{{{{dataset}_{store}_{query_set}_{policy}_to}}}}"] = "x"

    # =========================
    # Replace placeholders in template
    # =========================
    filled_table = template_results
    for ph, val in placeholder_map.items():
        filled_table = filled_table.replace(ph, val)

    # =========================
    # Save file
    # =========================
    with open(f"{tables_out}/latex_table_results.tex", "w") as f:
        f.write(filled_table)

    logging.info("LaTeX tables filled and saved.")




# Plots for query performance and ingestion
#args = itertools.product(['graphdb', 'jenatdb2', 'ostrich'], datasets)
#list(map(lambda x: create_plots(*x), args))

# Plots for update performance 
#create_plots_update("GRAPHDB", 'bearc')

# Latex table for query performance, ingestion, and db file size
create_latex_tables()
