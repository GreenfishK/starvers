import pandas as pd
import os
from matplotlib.lines import Line2D
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker


eval_directory = "/data/evaluation"
df_dict: dict[str, tuple[str, str, str]] = {}

for dir in os.listdir(eval_directory):
    timings_file_iterative = f"{eval_directory}/{dir}/{dir}_timings.csv"
    timings_file_sparql = f"{eval_directory}/{dir}/{dir}_timings_sparql.csv"

    if not os.path.isfile(timings_file_iterative) and not os.path.isfile(timings_file_sparql):
        raise FileNotFoundError(f"No timings file found in {eval_directory}/{dir}")

    # Extract base name by removing known suffixes
    base_name: str = dir
    df_dict[base_name] = ((f"{dir}", timings_file_sparql, timings_file_iterative))


for key, value in df_dict.items():
    onto_or_kg = key

    # Prepare dataframes and labels
    dfs: list[pd.DataFrame] = []
    linestyles = ['solid', 'dashed', 'dotted', 'dashdot']
    for idx, (name, csv_path_sparql, csv_path_iterative) in enumerate(value):
        def add_df_to_list(method: str, csv_path: str, dfs: list[pd.DataFrame]):
            df = pd.read_csv(csv_path)
            df.columns = df.columns.str.strip()
            
            # Remove lines where insertions and deletions are 0
            if 'insertions' in df.columns and 'deletions' in df.columns:
                df = df[~((df['insertions'] == 0) & (df['deletions'] == 0))]
            
            # Convert timestamp to mm-dd
            df['mm-dd'] = df['timestamp'].str[:8].apply(lambda x: f"{x[4:6]}-{x[6:8]}")
            
            # Convert ns to ms
            for col in ['time_prepare_ns', 'time_delta_ns', 'time_versioning_ns']:
                if col in df.columns:
                    df[col.replace('_ns', '_s')] = df[col] / 1_000_000_000

            # Add a column 'method' to the dataframe
            df['method'] = method

            dfs.append(df)
        add_df_to_list("SPARQL", csv_path_sparql, dfs)
        add_df_to_list("ITERATIVE", csv_path_iterative, dfs)

    # Stack the dataframes vertically
    if not dfs:
        continue
    plot_df = pd.concat(dfs, ignore_index=True)

    # Remove records after 20250526-233249_190 and before 20250508-210017_658 from the dataframe
    # Keep only records where timestamp is between these two (inclusive)
    start_ts = "20250508-210017_658"
    end_ts = "20250526-233249_190"
    plot_df = plot_df[(plot_df['timestamp'] >= start_ts) & (plot_df['timestamp'] <= end_ts)]

    # Prepare plot
    fig, ax = plt.subplots(figsize=(10, 6))

    # Get unique methods and mm-dds
    unique_methods = plot_df['method'].unique()
    unique_dates = plot_df['mm-dd'].unique()

    # Set bar width and positions
    bar_width = 0.35
    n_methods = len(unique_methods)
    x = range(len(unique_dates))

    # Colors for stacks
    stack_colors = ['white', 'grey', 'black']
    stack_labels = ['Prepare', 'Delta Calculation', 'Versioning']
    stack_cols = ['time_prepare_s', 'time_delta_s', 'time_versioning_s']

    # For legend handles
    stack_handles = []
    method_handles = []

    # Plot grouped stacked bars
    for i, method in enumerate(unique_methods):
        method_df = plot_df[plot_df['method'] == method]
        # Align bars for each method
        offsets = [xi + (i - n_methods/2) * bar_width + bar_width/2 for xi in x]
        bottoms = [0] * len(unique_dates)
        bars = []
        for j, (col, color, label) in enumerate(zip(stack_cols, stack_colors, stack_labels)):
            values = []
            for date in unique_dates:
                val = method_df[method_df['mm-dd'] == date][col].sum() if col in method_df.columns else 0
                values.append(val)
            bar = ax.bar(offsets, values, bar_width, bottom=bottoms, color=color, 
                         label=label if i == 0 else "", 
                         edgecolor='black', linestyle=linestyles[i % len(linestyles)])
            if i == 0:
                stack_handles.append(bar)
            bars.append(bar)
            bottoms = [bottoms[k] + values[k] for k in range(len(values))]
        # Add a dummy handle for the method label (use a Line2D for legend)
        method_handles.append(Line2D([0], [0], color='black', linestyle=linestyles[i % len(linestyles)], label=method))

    # Plot one line (green) for insertions and one line for deletions (red).
    # Take the data from the SPARQL method
    sparql_df = plot_df[plot_df['method'].str.lower() == 'sparql']
    if not sparql_df.empty and 'insertions' in sparql_df.columns and 'deletions' in sparql_df.columns:
        # Ensure the order of dates matches the bars
        insertions = []
        deletions = []
        for date in unique_dates:
            insertions.append(sparql_df[sparql_df['mm-dd'] == date]['insertions'].sum())
            deletions.append(sparql_df[sparql_df['mm-dd'] == date]['deletions'].sum())

        # Add secondary y-axis for insertions/deletions
        ax2 = ax.twinx()
        
        # Plot lines on the secondary y-axis so their values match the axis
        line1, = ax2.plot(x, insertions, color='green', marker='o', label='Insertions', linewidth=2)
        line2, = ax2.plot(x, deletions, color='red', marker='o', label='Deletions', linewidth=2)
        ax2.set_ylabel('Inserted/Deleted Triples')
        ax2.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, _: format(int(x), ',')))
        ax2.set_ylim(bottom=0)

        # Add to legend handles
        method_handles.append(Line2D([0], [0], color='green', marker='o', label='Insertions', linewidth=2))
        method_handles.append(Line2D([0], [0], color='red', marker='o', label='Deletions', linewidth=2))

    # X-axis settings
    ax.set_xticks(x)
    ax.set_xticklabels(unique_dates, rotation=45)
    ax.set_xlabel('Date (mm-dd)')
    ax.set_ylabel('Performance in seconds')
    ax.yaxis.set_major_formatter(mticker.FuncFormatter(lambda x, p: format(int(x), ',')))

    # Legend for stacks, methods, and lines
    handles = [h[0] for h in stack_handles] + method_handles 
    labels = stack_labels + list(unique_methods) + ['Insertions', 'Deletions']
    
    # Place legend completely outside the plot area, right of the figure
    ax.legend(handles, labels, loc='center left', bbox_to_anchor=(1.2, 0.5), borderaxespad=0., frameon=True)

    plt.tight_layout(rect=[0, 0, 1, 1])  # Leave space for legend

    # Save as vector graphic
    plt.savefig(f"{eval_directory}/{onto_or_kg}.pdf", format='pdf', bbox_inches='tight')
    plt.close()