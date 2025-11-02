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

logger.info("Retrieving file names of the timings CSV files.")
for dir in os.listdir(eval_dir):
    logger.info(f"Scanning directory for the <repo>_timings.csv and <repo>_timings_sparql.csv files: {dir}")
    timings_file_iterative = f"{eval_dir}/{dir}/{dir}_timings.csv"
    timings_file_sparql = f"{eval_dir}/{dir}/{dir}_timings_sparql.csv"

    if not os.path.isfile(timings_file_iterative) and not os.path.isfile(timings_file_sparql):
        raise FileNotFoundError(f"No timings file found in {eval_dir}/{dir}")

    # Extract base name by removing known suffixes
    base_name: str = dir
    df_dict[base_name] = ((f"{dir}", timings_file_sparql, timings_file_iterative))


for key, value in df_dict.items():
    logger.info(f"Preparing DataFrame for repo {key} and files: {value}")
    onto_or_kg = key

    # Prepare dataframes and labels
    dfs: list[pd.DataFrame] = []
    linestyles = ['solid', 'dashed', 'dotted', 'dashdot']

    def add_df_to_list(method: str, csv_path: str, dfs: list[pd.DataFrame]):
        df = pd.read_csv(csv_path)
        df.columns = df.columns.str.strip()
        
        # Remove lines where insertions and deletions are 0
        if 'insertions' in df.columns and 'deletions' in df.columns:
            df = df[~((df['insertions'] == 0) & (df['deletions'] == 0))]
        
        # Convert timestamp to mm-dd
        df['mm-dd'] = df['timestamp'].str[:8].apply(lambda x: f"{x[4:6]}-{x[6:8]}")
        
        # Convert ns to s
        for col in ['time_prepare_ns', 'time_delta_ns', 'time_versioning_ns']:
            if col in df.columns:
                df[col.replace('_ns', '_s')] = df[col] / 1_000_000_000

        # Add method label
        df['method'] = method

        dfs.append(df)

    add_df_to_list("SPARQL", value[1], dfs)
    add_df_to_list("ITERATIVE", value[2], dfs)

    if not dfs:
        continue

    plot_df = pd.concat(dfs, ignore_index=True)

    # Filter timestamp range
    start_ts = "20250508-210017_658"
    end_ts = "20251031-211417_193"
    plot_df = plot_df[(plot_df['timestamp'] >= start_ts) & (plot_df['timestamp'] <= end_ts)]

    # Prepare plot
    logger.info(f"Plotting DataFrame for repo {key}")
    fig, axes = plt.subplots(
        3, 1, figsize=(12, 12),
        sharex=False,
        gridspec_kw={'height_ratios': [0.4, 0.4, 0.2]}
    )

    methods = ['ITERATIVE', 'SPARQL']
    stack_cols = ['time_prepare_s', 'time_delta_s', 'time_versioning_s']
    stack_labels = {"ITERATIVE": ['Download, Skolemization, Control Sequence Removal', 'Delta Calculation', 'Versioning'],
                    "SPARQL": ['Download, Skolemization, Control Sequence Removal, \nIngestion', 'Delta Calculation', 'Versioning']}
    stack_colors = ['#d9d9d9', '#969696', '#525252']  # light → dark greys

    # Convert unique_dates to datetime objects (use year from timestamp, e.g., 2025)
    unique_dates = sorted(plot_df['mm-dd'].unique())
    dates_dt = [datetime.strptime(plot_df[plot_df['mm-dd'] == d]['timestamp'].iloc[0][:8], '%Y%m%d') 
                for d in unique_dates]

    # Top two plots: ITERATIVE and SPARQL stacked areas
    for ax, method in zip(axes[:2], methods):
        method_df = plot_df[plot_df['method'].str.lower() == method.lower()]
        if method_df.empty:
            ax.text(0.5, 0.5, f'No data for {method}', ha='center', va='center')
            continue

        # Stack performance times
        y_data = np.zeros((len(stack_cols), len(unique_dates)))
        for j, col in enumerate(stack_cols):
            y_data[j, :] = [
                method_df[method_df['mm-dd'] == d][col].sum() if col in method_df.columns else 0
                for d in unique_dates
            ]

        bottoms = np.zeros(len(unique_dates))
        stack_labels_method = stack_labels[method]
        for j, (col, color, label) in enumerate(zip(stack_cols, stack_colors, stack_labels_method)):
            ax.fill_between(dates_dt, bottoms, bottoms + y_data[j, :], color=color, alpha=0.8, label=label)
            bottoms += y_data[j, :]

        # Plot total number of triples for every date in dates_dt 
        cnt_triples = [plot_df[plot_df['mm-dd'] == d]['cnt_triples'].iat[-1] for d in unique_dates]
        ax2 = ax.twinx()  
        ax2.plot(dates_dt, cnt_triples, color='#5485AB', marker='o', markersize=2, 
                 label="# triples", linewidth=1)
        ax2.set_ylabel("Number of triples (in millions)")
        ax2.legend(loc='upper center', frameon=True)

        ax.set_title(f"{method} Delta Calculation Method", fontsize=13, fontweight='bold')
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
    insertions = [plot_df[plot_df['mm-dd'] == d]['insertions'].iat[0] for d in unique_dates]
    deletions = [plot_df[plot_df['mm-dd'] == d]['deletions'].iat[0] for d in unique_dates]

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
