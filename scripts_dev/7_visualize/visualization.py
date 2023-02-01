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



def create_ingest_plots(triple_store: str, dataset: str):
    """
    Creates for the dataset ingestion. 
    
    The plot for the dataset ingestion is a bar plot of grouped bars. Each group represents one of the four policies
    ic_sr_ng, cb_sr_ng, tb_sr_ng and tb_sr_rs and consits of three bars - one for each of the measures 
    ingestion_time, raw_file_size_MiB and db_files_disk_usage_MiB. 
    The groups of bars are labeled with the policy names. Each measure is assigned to one color. This color is 
    shown in a legend in the plot and the respective bar of the measure in each group has that very same color. The measure 
    values are displayed inside the bars. the title of the plot carries the name of the given :triple_store and :dataset parameters.

    """
    # Read data for ingestion measures
    ingestion_data = pd.read_csv(measurements_in + "ingest.csv", delimiter=";", decimal=".")

    # Define the colors for the measures 
    measures = {"ingestion_time": "red", "raw_file_size_MiB": "blue", "db_files_disk_usage_MiB": "green"}
    
    # Create the plot
    fig, ax = plt.subplots()
    for policy in policies:
        policy_data = ingestion_data[ingestion_data["policy"] == policy]
        x = policy_data["measure"]
        for measure, color in measures.items():
            y = policy_data[measure]
            ax.bar(x, y, color=color, label=measure)
    ax.legend()
    ax.set_xlabel("Policy")
    ax.set_ylabel("Measure Value")
    ax.set_title(f"{triple_store} - {dataset} Ingestion Plot")
    plt.savefig(f"/starvers_eval/output/figures/ingestion_{triple_store}_{dataset}.png")


create_ingest_plots("graphdb", "beara")
create_ingest_plots("graphdb", "bearb_hour")
create_ingest_plots("graphdb", "bearb_day")
create_ingest_plots("graphdb", "bearc")
create_ingest_plots("jenatdb2", "beara")
create_ingest_plots("jenatdb2", "bearb_hour")
create_ingest_plots("jenatdb2", "bearb_day")
create_ingest_plots("jenatdb2", "bearc")