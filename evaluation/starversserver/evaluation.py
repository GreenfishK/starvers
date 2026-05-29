import pandas as pd
import os
from matplotlib.lines import Line2D
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import matplotlib.dates as mdates
import numpy as np
from datetime import datetime
from app.LoggingConfig import get_logger, setup_logging

#######################################
# Inputs
#######################################
start_ts = "20250508-210017_658"
end_ts = "20260507-000001_000"

#######################################
# Logging
#######################################
logger = get_logger(__name__, f"evaluation.log")
setup_logging()

#######################################
# Process
#######################################
eval_dir = "/data/evaluation"
figures_dir = "/data/figures"
df_dict: dict[str, tuple[str, str, str]] = {}

# TODO: Change back to _timings_sparql_new.csv after the paper submission and implement the TODOs in verisoning_pipeline.py
logger.info("Retrieving file names of the timings CSV files.")
for dir in os.listdir(eval_dir):

    logger.info(f"Scanning directory for the <repo>_timings.csv and <repo>_timings_sparql.csv files: {dir}")
    timings_file_inmemory = f"{eval_dir}/{dir}/{dir}_timings.csv"
    timings_file_sparql = f"{eval_dir}/{dir}/{dir}_timings_sparql.csv"

    if not os.path.isfile(timings_file_inmemory) and not os.path.isfile(timings_file_sparql):
        raise FileNotFoundError(f"No timings file found in {eval_dir}/{dir}")

    # Extract base name by removing known suffixes
    base_name: str = dir
    df_dict[base_name] = ((f"{dir}", timings_file_sparql, timings_file_inmemory))


for key, value in df_dict.items():
    logger.info(f"Preparing DataFrame for repo {key} and files: {value}")
    onto_or_kg = key

    # Prepare dataframes and labels
    dfs: list[pd.DataFrame] = []
    linestyles = ['solid', 'dashed', 'dotted', 'dashdot']

    def add_df_to_list(method: str, csv_path: str, dfs: list[pd.DataFrame]):
        df = pd.read_csv(csv_path)
        df.columns = df.columns.str.strip()

        # Convert timestamp to yy-mm-dd
        df['yy-mm-dd'] = df['timestamp'].str[:8].apply(lambda x: f"{x[:4]}-{x[4:6]}-{x[6:8]}")

        # Print the missing dates and the number of missing dates in the period from start_ts to end_ts
        all_dates = pd.date_range(
            start=datetime.strptime(start_ts[:8], "%Y%m%d"),
            end=datetime.strptime(end_ts[:8], "%Y%m%d"),
            freq='D'
        )

        existing_dates = set(df['yy-mm-dd'].unique())

        missing_dates = [
            d.strftime("%Y-%m-%d")
            for d in all_dates
            if d.strftime("%Y-%m-%d") not in existing_dates
        ]

        logger.info(
            f"Dataset {key} / Method {method}: "
            f"{len(missing_dates)} missing dates between "
            f"{start_ts[:8]} and {end_ts[:8]}"
        )

        logger.info(f"Missing dates for dataset {key} / Method {method}: {', '.join(missing_dates)}")

        # Remove lines where insertions and deletions are 0
        if 'insertions' in df.columns and 'deletions' in df.columns:
            df = df[~((df['insertions'] == 0) & (df['deletions'] == 0))]

        # Shift the ingest time from time_prepare_ns to the time_delta_ns for the SPARQL method
        if method == "SPARQL":
            # Add a column time_ingest_ns to the SPARQL dataframe by subtracting time_prepare_ns from the IN-MEMORY dataframe from the time_prepare_ns from the SPARQL dataframe
            df_sparql = pd.read_csv(value[1])
            df_sparql.columns = df_sparql.columns.str.strip()
            df_inmemory = pd.read_csv(value[2])
            df_inmemory.columns = df_inmemory.columns.str.strip()

            df['time_ingest_ns'] = df_sparql['time_prepare_ns'] - df_inmemory['time_prepare_ns']

            # Rename the time_prepare_ns to time_preapare_ns_old
            df = df.rename(columns={'time_prepare_ns': 'time_preapare_ns_old'})

            # Add a column time_prepare_ns by subtracting time_ingest_ns from time_prepare_ns_old
            df['time_prepare_ns'] = df['time_preapare_ns_old'] - df['time_ingest_ns']

            # Rename time_delta_ns to time_delta_ns_old
            df = df.rename(columns={'time_delta_ns': 'time_delta_ns_old'})

            # Add a column time_delta_ns by adding time_ingest_ns to time_delta_ns_old
            df['time_delta_ns'] = df['time_delta_ns_old'] + df['time_ingest_ns']
        
        # Convert ns to s
        for col in ['time_prepare_ns', 'time_delta_ns', 'time_versioning_ns']:
            if col in df.columns:
                df[col.replace('_ns', '_s')] = df[col] / 1_000_000_000

        # Remove outliers that are bigger than 2400 seconds
        df['total_time_s'] = df['time_prepare_s'] + df['time_delta_s'] + df['time_versioning_s']
        # Calculate Q1, Q3 and IQR for total_time_s
        Q1 = df['total_time_s'].quantile(0.25)
        Q3 = df['total_time_s'].quantile(0.75)
        IQR = Q3 - Q1
        outliers = df[df['total_time_s'] > 2400]
        logger.info(f"{len(outliers)} outliers for total_time_s for method {method} and dataset {key}:")
        for _, row in outliers.iterrows():
            logger.info(f"Timestamp: {row['timestamp']}, total_time_s: {row['total_time_s']:.2f} s")
        
        # Print total_time_s statistics
        logger.info(f"Statistics for total_time_s for method {method} and dataset {key}:")
        logger.info(f"Min: {df['total_time_s'].min():.2f} s")
        logger.info(f"Q1: {Q1:.2f} s")
        logger.info(f"Median total: {df['total_time_s'].median():.2f} s")
        logger.info(f"Median versioning: {df['time_versioning_s'].median():.2f} s")
        logger.info(f"Q3: {Q3:.2f} s")
        logger.info(f"Max: {df['total_time_s'].max():.2f} s")

        # Print the min and max total_time_s for rows where thet total number of updates (inserts + deletes) exceeds 100,000
        df_large_updates = df[(df['insertions'] + df['deletions']) > 100_000]
        logger.info(f"Rows with more than 100,000 updates for method {method} and dataset {key}:")
        for _, row in df_large_updates.iterrows():
            logger.info(f"Timestamp: {row['timestamp']}, total_time_s: {row['total_time_s']:.2f} s, insertions: {row['insertions']}, deletions: {row['deletions']}")
            logger.info(f"Timestamp: {row['timestamp']}, time_versioning_s: {row['time_versioning_s']:.2f} s, insertions: {row['insertions']}, deletions: {row['deletions']}")


        # Keep only lines where total_time_s is smaller than or equal to 2400 seconds
        df = df[df['total_time_s'] <= 2400]

        logger.info(f"Total missing dates after outlier removal for dataset {key} / Method {method}: {len(missing_dates) + len(outliers)}")
        logger.info(f"Total number of included dates for dataset {key} / Method {method}: {len(df['yy-mm-dd'].unique())}")

        # Add method label
        df['method'] = method

        dfs.append(df)

    add_df_to_list("SPARQL", value[1], dfs)
    add_df_to_list("IN-MEMORY", value[2], dfs)

    if not dfs:
        continue

    plot_df = pd.concat(dfs, ignore_index=True)


    plot_df = plot_df[(plot_df['timestamp'] >= start_ts) & (plot_df['timestamp'] <= end_ts)]

    # Prepare plot
    logger.info(f"Plotting DataFrame for repo {key}")
    fig, axes = plt.subplots(
        3, 1, figsize=(12, 12),
        sharex=False,
        gridspec_kw={'height_ratios': [0.4, 0.4, 0.2]}
    )

    methods = ['IN-MEMORY', 'SPARQL']
    stack_cols = ['time_prepare_s', 'time_delta_s', 'time_versioning_s']
    stack_labels = {"IN-MEMORY": ['Download, Preprocessing', 'Delta Computation', 'Versioning'],
                    "SPARQL": ['Download, Preprocessing', 'Delta Computation', 'Versioning']}
    stack_colors = ['#d9d9d9', '#969696', '#525252']  # light → dark greys

    # Convert unique_dates to datetime objects (use year from timestamp, e.g., 2025)
    unique_dates = sorted(plot_df['yy-mm-dd'].unique())
    dates_dt = [datetime.strptime(plot_df[plot_df['yy-mm-dd'] == d]['timestamp'].iloc[0][:8], '%Y%m%d') 
                for d in unique_dates]

    # Top two plots: IN-MEMORY and SPARQL stacked areas
    for ax, method in zip(axes[:2], methods):
        method_df = plot_df[plot_df['method'].str.lower() == method.lower()]
        if method_df.empty:
            ax.text(0.5, 0.5, f'No data for {method}', ha='center', va='center')
            continue

        # Stack performance times
        y_data = np.zeros((len(stack_cols), len(unique_dates)))
        for j, col in enumerate(stack_cols):
            y_data[j, :] = [
                method_df[method_df['yy-mm-dd'] == d][col].sum() if col in method_df.columns else 0
                for d in unique_dates
            ]

        bottoms = np.zeros(len(unique_dates))
        stack_labels_method = stack_labels[method]
        for j, (col, color, label) in enumerate(zip(stack_cols, stack_colors, stack_labels_method)):
            ax.fill_between(dates_dt, bottoms, bottoms + y_data[j, :], color=color, alpha=0.8, label=label)
            bottoms += y_data[j, :]

        # Plot total number of triples for every date in dates_dt 
        cnt_triples = [plot_df[plot_df['yy-mm-dd'] == d]['cnt_triples'].iat[-1] for d in unique_dates]
        ax2 = ax.twinx()  
        ax2.plot(dates_dt, cnt_triples, color='#5485AB', marker='o', markersize=2, 
                 label="# triples", linewidth=1)
        ax2.set_ylabel("Number of triples (in millions)")
        ax2.legend(loc='upper center', frameon=True)

        ax.set_title(f"{method} Delta Computation Method", fontsize=13, fontweight='bold')
        ax.set_ylabel("Runtime (minutes)")
        ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: format(int(x), ',')))
        ax.legend(loc='upper left', frameon=True)

        # Set Y-ticks every 1 minute 
        max_y = ax.get_ylim()[1]  # get top of y-axis
        yticks = np.arange(0, max_y + 60, 60) 
        ax.set_yticks(yticks)
        ax.set_yticklabels([f"{int(t // 60)}" for t in yticks])

        # Set grid lines
        ax.grid(which='major', axis='x', color='black', alpha=0.3, linestyle='-')
        ax.grid(which='major', axis='y', color='black', alpha=0.3, linestyle='--')

        ax.xaxis.set_major_locator(mdates.MonthLocator(bymonthday=-1))  # last day of each month
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
        ax.xaxis.set_minor_locator(mdates.DayLocator())  # minor ticks every day
        ax.tick_params(axis='x', which='minor', labelbottom=False)
        ax.tick_params(axis='x', rotation=0)


    # Bottom plot: insertions and deletions 
    ax_bottom = axes[2]
    # Sum across all methods for bottom plot 
    insertions = [plot_df[plot_df['yy-mm-dd'] == d]['insertions'].iat[0] for d in unique_dates]
    deletions = [plot_df[plot_df['yy-mm-dd'] == d]['deletions'].iat[0] for d in unique_dates]

    # Flooring values to 1 due to log scale (looks bad in the plot otherwise)
    insertions_safe = [max(1, val) for val in insertions]
    deletions_safe = [max(1, val) for val in deletions]

    ax_bottom.plot(dates_dt, insertions_safe, marker = "x", color='#007E71', markersize=3,
                   label='Insertions', linewidth=1)
    ax_bottom.plot(dates_dt, deletions_safe, marker = "D", color='#BA4682', markersize=1.5,
                   label='Deletions', linewidth=1)
    
    ax_bottom.xaxis.set_major_locator(mdates.MonthLocator(bymonthday=-1))  # last day of each month
    ax_bottom.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
    ax_bottom.xaxis.set_minor_locator(mdates.DayLocator())  # minor ticks every day
    ax_bottom.tick_params(axis='x', which='minor', labelbottom=False)
    ax_bottom.tick_params(axis='x', rotation=0)

    # BLUE GRID: vertical every tick, horizontal at powers of 10 
    ax_bottom.grid(which='major', axis='x', color='black', alpha=0.3, linestyle='-')
    ax_bottom.grid(which='major', axis='y', color='black', alpha=0.3, linestyle='--')
    
    # Set y-axis scale, labels and title
    ax_bottom.set_yscale('log')
    ax_bottom.set_ylabel('Inserted / Deleted\nTriples (log)')
    ax_bottom.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: format(int(x), ',')))
    ax_bottom.set_title("Inserted / Deleted Triples", fontsize=13, fontweight='bold')
    ax_bottom.legend(loc='upper left', frameon=True)

    plt.tight_layout(rect=[0, 0, 1, 1])

    # Save figure
    logger.info(f"Saving plot to {figures_dir}/{onto_or_kg}.pdf")
    plt.savefig(f"{figures_dir}/{onto_or_kg}.pdf", format='pdf', bbox_inches='tight')
    plt.close()

logger.info("Process finished.")
