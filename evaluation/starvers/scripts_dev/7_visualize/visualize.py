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
import tomli

#############################################
# Logging 
#############################################
if not os.path.exists(f"{os.environ['RUN_DIR']}/output/logs/visualize"):
    os.makedirs(f"{os.environ['RUN_DIR']}/output/logs/visualize")
with open(f"{os.environ['RUN_DIR']}/output/logs/visualize/visualize.txt", "w") as log_file:
    log_file.write("")
logging.basicConfig(handlers=[logging.FileHandler(filename=f"{os.environ['RUN_DIR']}/output/logs/visualize/visualize.txt", 
                                                  encoding='utf-8', mode='a+')],
                    format="%(asctime)s %(name)s:%(levelname)s:%(message)s", 
                    datefmt="%F %A %T", 
                    level=logging.INFO)
#############################################
# Paths and parameters
#############################################
# f"{os.environ['RUN_DIR']}
work_dir = "/starvers_eval/"
CONFIG_PATH = f"{work_dir}configs/eval_setup.toml"
RESULTS_TMPL = f"{work_dir}scripts/7_visualize/templates/latex_table_results_tmpl.tex"

RUN_DIR = os.environ['RUN_DIR']
measurements_in = RUN_DIR + "/output/measurements/"
figures_out = RUN_DIR + "/output/figures"
tables_out = RUN_DIR + "/output/tables"

policies =  os.environ.get("policies").split(" ")
datasets =  os.environ.get("datasets").split(" ")


#############################################
# Visualize 
#############################################
pd.set_option('display.max_columns', None)



def create_plots(triplestore: str, dataset: str):
    """
    Create and save performance and ingestion plots for the specified triplestore and dataset.
    """
    # Data
    performance_data = pd.read_csv(measurements_in + "time_20260330T1215.csv", delimiter=";", decimal=".",
                            dtype={"triplestore": "category", "dataset": "category", "policy": "category",
                            "query_set": "category", "snapshot": "string", "query": "string",
                            "execution_time": "float", "snapshot_creation_time": "float"})
    performance_data['snapshot_ts'] = pd.to_datetime(performance_data['snapshot_ts'])
    performance_data['execution_time_total'] = performance_data['execution_time'] + performance_data['snapshot_creation_time']
    performance_data = performance_data[['triplestore', 'dataset', 'policy', 'snapshot', 'query_set', 'execution_time_total']]

    # Convert category columns to string before groupby aggregation —
    # pandas category dtype does not support mean() directly
    for col in ['triplestore', 'dataset', 'policy', 'query_set']:
        if performance_data[col].dtype.name == 'category':
            performance_data[col] = performance_data[col].astype(str)

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
        means = dataset_df.groupby(['policy', 'snapshot'], observed=False).mean(numeric_only=True)
        means = means.reset_index()
        
        for policy in policies:
            policy_df = means[means['policy'] == policy]
            if dataset == 'bearc':
                markevery = math.ceil(len(policy_df['snapshot'])/120)
            else:
                markevery = math.ceil(len(policy_df['snapshot'])/60)
            markevery = max(markevery, 1)

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
    datasets = set(os.environ.get("datasets").split(" "))
    policies = set(os.environ.get("policies").split(" "))
    triplestores = set(os.environ.get("triple_stores").split(" "))
    query_sets = ["lookup", "join", "complex"]

    # =========================
    # Load data
    # =========================
    queries_data = pd.read_csv(
        measurements_in + "time_20260330T1215.csv",
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
    
    # Set queries_data["execution_time_total"] to 30 for timedout queries and other failure reasons
    queries_data.loc[queries_data["execution_time"] >= 30, "yn_timeout"] = 1
    queries_data["execution_time_clean"] = queries_data["execution_time"].where(
        (queries_data["execution_time"] <= 30) & (queries_data["execution_time"] >= 0), 
        other=30
    )

    # total should be max 30
    queries_data["execution_time_total"] = (queries_data["execution_time_clean"] + queries_data["rewriting_time"]).clip(upper=30)
    
    ingestion_data = pd.read_csv(
        measurements_in + "ingestion.csv", delimiter=";", decimal="."
    )
    ingestion_data["triplestore"] = ingestion_data["triplestore"].str.lower()

    # =========================
    # Load template
    # =========================
    with open(RESULTS_TMPL, "r") as f:
        template_results = f.read()

    # =========================
    # Aggregation
    # =========================

    # Performance per dataset, policy, query_set, triplestore

    queries_agg = queries_data.groupby(["triplestore", "dataset", "policy", "query_set"], observed=False).agg(
        min=("execution_time_total", "min"),
        avg=("execution_time_total", "mean"),
        max=("execution_time_total", "max"),
        cnt_timeout=("yn_timeout", "sum")
    ).reset_index()
    logging.info(f"Aggregated measures:\n{queries_agg}")

    queries_agg = queries_agg[queries_agg["min"].notna()]

    queries_agg.to_csv(f"{os.environ['RUN_DIR']}/output/logs/visualize/queries.csv", index=False)

    # Storage
    storage_agg = ingestion_data.groupby(["triplestore", "dataset", "policy"], observed=False).agg(
        ingestion_time=("ingestion_time", "median"),
        raw_file_size=("raw_file_size_MiB", "mean"),
        db_file_size=("db_files_disk_usage_MiB", "mean")
    ).reset_index()
    storage_agg.to_csv(f"{os.environ['RUN_DIR']}/output/logs/visualize/storage.csv", index=False)
    

    # =========================
    # Formatting helpers
    # =========================
    def format_exec_time(v):
        if v == 0:
            return "0"
        elif v < 0.01:
            return f"{v:.0e}".replace("e-0", "e-").replace("e+0", "e")
        elif v < 1:
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
            lowest_min = None
            lowest_avg = None
            lowest_max = None
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

    # =========================
    # Calculate RDF versioning system evaluation metrics
    # =========================
    # --- Load version counts from eval_setup.toml ---
    with open(CONFIG_PATH, "rb") as f:
        config = tomli.load(f)

    version_counts = {
        ds_name: ds_cfg["snapshot_versions"]
        for ds_name, ds_cfg in config["datasets"].items()
    }
    version_counts_normalised = {k.lower().replace("-", "_"): v for k, v in version_counts.items()}
 
    def get_version_count(dataset_value):
        key = str(dataset_value).lower().replace("-", "_")
        if key not in version_counts_normalised:
            raise KeyError(
                f"Dataset '{dataset_value}' not found in eval_setup.toml. "
                f"Available keys: {list(version_counts_normalised.keys())}"
            )
        return version_counts_normalised[key]
 
    # --- Derive the ordered list of datasets and store-policy combos from the data ---
    all_datasets   = sorted(queries_agg["dataset"].unique())
    all_stores     = sorted(queries_agg["triplestore"].unique())
    all_policies   = sorted(queries_agg["policy"].unique())
    combo_index    = [(s, p) for s in all_stores for p in all_policies]  # (triplestore, policy)
 
 
    # --- Step 2: dataset weights ---
    # ic_ng raw size for each dataset (MiB → GiB; only need relative proportions so
    # the unit cancels out, but we convert for interpretability)
    ic_ng_sizes = (
        storage_agg[storage_agg["policy"] == "ic_sr_ng"]
        .drop_duplicates(subset=["dataset"])
        .set_index("dataset")["raw_file_size"]
        / 1024.0  # MiB -> GiB
    )
 
    R = np.array([ic_ng_sizes.loc[d] for d in all_datasets], dtype=float)
    V = np.array([get_version_count(d) for d in all_datasets], dtype=float)

    # Metric A: size-only weights
    w_size = R / R.sum()
 
    # Metric B: 0.5 * normalised_size + 0.5 * normalised_versions, then re-normalise
    w_comb_raw = 0.5 * (R / R.sum()) + 0.5 * (V / V.sum())
    w_comb = w_comb_raw / w_comb_raw.sum()
 
    dataset_weight_size = dict(zip(all_datasets, w_size))
    dataset_weight_comb = dict(zip(all_datasets, w_comb))
 
    logging.info(f"Dataset weights (size only): {dataset_weight_size}")
    logging.info(f"Dataset weights (size+versions): {dataset_weight_comb}")
 
    # --- Step 3: weighted geometric mean of avgs ---
    # Each query set row gets weight = dataset_weight[d] / #query_sets_for_d
    # Then re-normalised across all rows so weights sum to 1.
    #
    # GeoAvg_s = exp( sum_q( ŵ_q * ln(ã_{q,s}) ) )
    def weighted_geo_mean_avgs(weight_dict):
        """
        Returns a dict {(triplestore, policy): GeoAvg} using the provided dataset weights.
        """
        # Count how many query sets exist per dataset (across all combos — use the union)
        qs_per_dataset = (
            queries_agg.groupby("dataset", observed=False)["query_set"]
            .nunique()
            .to_dict()
        )
 
        results = {}
        for store, policy in combo_index:
            subset = queries_agg[
                (queries_agg["triplestore"] == store) &
                (queries_agg["policy"] == policy)
            ][["dataset", "query_set", "avg"]].copy()

            # Assign per-row weight = dataset_weight / #query_sets_for_that_dataset
            subset["row_weight"] = subset["dataset"].map(
                lambda d: weight_dict.get(d, 0.0) / qs_per_dataset.get(d, 1)
            )
            # convert to float
            subset["row_weight"] = subset["row_weight"].astype(float)

            # Drop rows where avg is NaN (query set not available for this combo)
            subset = subset.dropna(subset=["avg"])
 
            if subset.empty or (subset["avg"] <= 0).any():
                results[(store, policy)] = np.nan
                continue
 
            # Re-normalise weights among available rows
            total_w = subset["row_weight"].sum()
            if total_w == 0:
                results[(store, policy)] = np.nan
                continue
            subset["row_weight"] = subset["row_weight"] / total_w
 
            geo_mean = np.exp(
                (subset["row_weight"] * np.log(subset["avg"])).sum()
            )
            results[(store, policy)] = geo_mean
 
        return results
 
    geo_avg_A = weighted_geo_mean_avgs(dataset_weight_size)
    geo_avg_B = weighted_geo_mean_avgs(dataset_weight_comb)
 
    # --- Step 4: weighted arithmetic mean of db sizes ---
    # WeightedDb_s = sum_d( w_d * db_{d,s} )   (in GiB)
    def weighted_arith_mean_db(weight_dict):
        results = {}
        for store, policy in combo_index:
            total = 0.0
            total_w = 0.0
            for d in all_datasets:
                match = storage_agg[
                    (storage_agg["triplestore"] == store) &
                    (storage_agg["policy"] == policy) &
                    (storage_agg["dataset"] == d)
                ]
                if match.empty:
                    continue
                db_gib = match["db_file_size"].values[0] / 1024.0
                w = weight_dict.get(d, 0.0)
                total   += w * db_gib
                total_w += w
            results[(store, policy)] = total / total_w if total_w > 0 else np.nan
        return results
 
    weighted_db_A = weighted_arith_mean_db(dataset_weight_size)
    weighted_db_B = weighted_arith_mean_db(dataset_weight_comb)
 
    # --- Step 5: min-max normalise ---
    def minmax_dict(d):
        vals = np.array(list(d.values()), dtype=float)
        vmin, vmax = np.nanmin(vals), np.nanmax(vals)
        if vmax == vmin:
            return {k: 0.0 for k in d}
        return {k: (v - vmin) / (vmax - vmin) for k, v in d.items()}
 
    norm_avg_A  = minmax_dict(geo_avg_A)
    norm_avg_B  = minmax_dict(geo_avg_B)
    norm_db_A   = minmax_dict(weighted_db_A)
    norm_db_B   = minmax_dict(weighted_db_B)
 
    # --- Step 6: composite score  0.75 * norm_avg + 0.25 * norm_db ---
    composite_A = {
        k: 0.75 * norm_avg_A[k] + 0.25 * norm_db_A[k]
        for k in combo_index
    }
    composite_B = {
        k: 0.75 * norm_avg_B[k] + 0.25 * norm_db_B[k]
        for k in combo_index
    }
 
    # --- Build avg pivot for the CSV (one column per combo) ---
    # Rows = (dataset, query_set), Columns = (triplestore, policy)
    pivot = queries_agg.pivot_table(
        index=["dataset", "query_set"],
        columns=["triplestore", "policy"],
        values="avg",
        observed=False
    )
    pivot.columns = [
        f"{s}-{p}_avg" for s, p in pivot.columns
    ]
 
    # --- Assemble summary rows (one row per store-policy combo) ---
    def build_metrics_df(weight_dict, geo_avg, weighted_db, norm_avg, norm_db, composite, weight_label):
        rows = []
        for store, policy in combo_index:
            combo_str = f"{store}-{policy}"
            row = {
                "triplestore_policy":          combo_str,
                f"dataset_weight_{weight_label}_BEARB_day":   weight_dict.get("bearb_day",  weight_dict.get("BEARB_day",  np.nan)),
                f"dataset_weight_{weight_label}_BEARB_hour":  weight_dict.get("bearb_hour", weight_dict.get("BEARB_hour", np.nan)),
                f"dataset_weight_{weight_label}_BEARC":       weight_dict.get("bearc",      weight_dict.get("BEARC",      np.nan)),
                f"dataset_weight_{weight_label}_ORKG":        weight_dict.get("orkg",       weight_dict.get("ORKG",       np.nan)),
                "weighted_geo_mean_avg_s":          geo_avg.get((store, policy), np.nan),
                "weighted_arith_mean_db_GiB":                 weighted_db.get((store, policy), np.nan),
                "norm_weighted_geo_mean_avg":       norm_avg.get((store, policy), np.nan),
                "norm_weighted_arith_mean_db":                norm_db.get((store, policy), np.nan),
                "composite_score_0.75avg_0.25db":             composite.get((store, policy), np.nan),
            }
            rows.append(row)
        summary_df = pd.DataFrame(rows).set_index("triplestore_policy")
 
        return summary_df
 
    metrics_size     = build_metrics_df(
        dataset_weight_size, geo_avg_A, weighted_db_A, norm_avg_A, norm_db_A, composite_A,
        weight_label="size"
    )
    metrics_size_ver = build_metrics_df(
        dataset_weight_comb, geo_avg_B, weighted_db_B, norm_avg_B, norm_db_B, composite_B,
        weight_label="size_and_version"
    )

    metrics_size = metrics_size.dropna(subset=["weighted_geo_mean_avg_s"])
    metrics_size_ver = metrics_size_ver.dropna(subset=["weighted_geo_mean_avg_s"])
 
    # --- Save ---
    metrics_size.to_csv(f"{tables_out}/metrics_size_weights.csv", sep=";", decimal=".", float_format="%.6f")
    metrics_size_ver.to_csv(f"{tables_out}/metrics_size_and_version_weights.csv", sep=";", decimal=".", float_format="%.6f")
 
    logging.info(f"Metrics (size weights):\n{metrics_size}")
    logging.info(f"Metrics (size+version weights):\n{metrics_size_ver}")





# Plots for query performance and ingestion
args = itertools.product(['graphdb', 'jenatdb2', 'ostrich'], datasets)
list(map(lambda x: create_plots(*x), args))

# Plots for update performance 
#create_plots_update("GRAPHDB", 'bearc')

# Latex table for query performance, ingestion, and db file size
create_latex_tables()
