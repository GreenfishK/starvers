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

# ChatGPT text
"""
Write a python function create_ingest_plots(triple_store: str, dataset: str) that takes a csv file called ingestion.csv 
as an input (see the content of ingestion.csv below) and reads it into a dataframe called ingestion_data. 
It then creates a bar plot of grouped bars. Each group represents one of the four policies ic_sr_ng, cb_sr_ng, tb_sr_ng and tb_sr_rs.  
Each group consists of three bars. One bar represents the values of the ingestion_time column and is shown on the left y-axis.
The other two bars represent the values of the raw_file_size_MiB and db_files_disk_usage_MiB columns. Latter two bars 
are plotted on the same position in each group, i.e. they are overlapping. db_files_disk_usage_MiB is slightly narrower than raw_file_size_MiB. 
Thus, db_files_disk_usage_MiB fits into raw_file_size_MiB and they are not plotted next to each other.
The groups are labeled with the policy names. Thus, there are four ticks on the x-axis.
Within each group each bar has a different color. 
Across the groups each measure/column has the same color. The measures and their colors are shown in a legend, 
which means that the legend has three entries and there is only one legend for both axes. 
The number of bars is equal to the number of policies times the number of measures/columns (three measures/columns). 
There should be two y-axis. The title of the plot carries the name of the given :triple_store and :dataset parameters. 
The plots are saved to /starvers_eval/output/figures/ingestion_{triple_store}_{dataset}.png.

triplestore;policy;dataset;ingestion_time;raw_file_size_MiB;db_files_disk_usage_MiB
graphdb;tb_sr_rs;bearc;270.39;4688;3318
graphdb;tb_sr_rs;bearb_hour;19.80;79;192
graphdb;tb_sr_rs;bearb_day;17.03;37;158
graphdb;ic_sr_ng;bearc;207.88;3639;1643
graphdb;ic_sr_ng;bearb_hour;523.92;8312;3186
graphdb;ic_sr_ng;bearb_day;54.51;556;330
graphdb;cb_sr_ng;bearc;229.70;3912;1677
graphdb;cb_sr_ng;bearb_hour;18.23;50;164
graphdb;cb_sr_ng;bearb_day;15.81;20;143
graphdb;tb_sr_ng;bearc;138.36;2256;1151 
graphdb;tb_sr_ng;bearb_hour;290.24;9184;1323
graphdb;tb_sr_ng;bearb_day;14.19;33;142
jenatdb2;tb_sr_rs;bearc;803.91;4688;5250
jenatdb2;tb_sr_rs;bearb_hour;16.92;79;255
jenatdb2;tb_sr_rs;bearb_day;10.05;37;219
jenatdb2;ic_sr_ng;bearc;689.93;3639;6162
jenatdb2;ic_sr_ng;bearb_hour;3740.87;8312;17196
jenatdb2;ic_sr_ng;bearb_day;105.02;556;1171
jenatdb2;cb_sr_ng;bearc;679.32;3912;5242
jenatdb2;cb_sr_ng;bearb_hour;9.39;50;276
jenatdb2;cb_sr_ng;bearb_day;7.06;20;195
jenatdb2;tb_sr_ng;bearc;362.41;2256;3386
jenatdb2;tb_sr_ng;bearb_hour;835.02;9184;6787
jenatdb2;tb_sr_ng;bearb_day;5.57;33;195
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


#create_ingest_plots("graphdb", "beara")
#create_ingest_plots("graphdb", "bearb_hour")
#create_ingest_plots("graphdb", "bearb_day")
#create_ingest_plots("graphdb", "bearc")
#create_ingest_plots("jenatdb2", "beara")
#create_ingest_plots("jenatdb2", "bearb_hour")
#create_ingest_plots("jenatdb2", "bearb_day")
#create_ingest_plots("jenatdb2", "bearc")

create_ingest_plots("graphdb")
create_ingest_plots("jenatdb2")