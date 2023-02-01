import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
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




def create_ingest_plots(triple_store: str):
    """


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

        
        ax2.set_ylabel("File Size (MiB) and DB File Size (MiB)")
        ax2.yaxis.label.set_color('darkgreen')


    # Add legend to figure
    limegreen_patch = mpatches.Patch(color='limegreen', label='Raw File Size')
    darkgreen_patch = mpatches.Patch(color='darkgreen', label='DB File Size')
    blue_patch = mpatches.Patch(color='blue', label='Ingestion Time')
    fig.legend(loc='upper right', handles={limegreen_patch, darkgreen_patch, blue_patch})
    
    fig.suptitle(f"{triple_store}", fontsize=32)
    fig.set_figheight(9)
    fig.set_figwidth(16)

    plt.tight_layout(pad=3.0, w_pad=2, h_pad=1.0)
    plt.savefig(f"/starvers_eval/output/figures/ingestion_{triple_store}.png")
    plt.close()


def create_query_performance_plots(triple_store: str):
    pass


create_ingest_plots("graphdb")
create_ingest_plots("jenatdb2")
create_query_performance_plots("graphdb")
create_query_performance_plots("jenatdb2")