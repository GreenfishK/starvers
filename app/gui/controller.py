from datetime import datetime, timedelta
import pandas as pd
import matplotlib.pyplot as plt
import logging
from io import BytesIO
import numpy as np

# starvers and starversServer imports
from starvers.starvers import TripleStoreEngine
from app.services.ManagementService import get_dataset_metadata_by_repo_name
from app.Database import get_session

class GuiContr:
    def __init__(self, repo_name: str = "orkg_v2"):
        self.repo_name = repo_name
        self.__graph_db_get_endpoint = f"http://rdfstore:7200/repositories/{repo_name}" 
        self.__graph_db_post_endpoint = f"http://rdfstore:7200/repositories/{repo_name}/statements" 
        self.__starvers_engine = TripleStoreEngine(self.__graph_db_get_endpoint, self.__graph_db_post_endpoint, skip_connection_test=True)


    def query(self, query: str, timestamp: datetime = None, query_as_timestamped: bool = True) -> pd.DataFrame:
        if timestamp is not None and query_as_timestamped:
            logging.info(f"Execute timestamped query with timestamp={timestamp}")
        else:
            logging.info("Execute query without timestamp")
        
        result_set_df = self.__starvers_engine.query(query, timestamp, query_as_timestamped)
        timestamped_query = self.__starvers_engine.timestamped_query
        timestamped_query = timestamped_query.lstrip()
        return result_set_df, timestamped_query


    def get_repo_stats(self):    
        repo_name = self.repo_name 
        path = f"/code/evaluation/{repo_name}/{repo_name}_timings.csv"
        df = pd.read_csv(path)
        df.columns = df.columns.str.strip()
        logging.info(f"Loaded {len(df)} rows from {path}")

        session = next(get_session())
        tracking_infos = get_dataset_metadata_by_repo_name(repo_name, session)
        polling_interval = tracking_infos[2]
        session.close()

        # Remove lines where no adds and delets occured
        df = df[~((df["insertions"] == 0) & (df["deletions"] == 0))]
        logging.info(f"Rows after removing lines: {len(df)}")

        # Include only rows where the timestamp is not newer than the 28.05.2025
        cutoff_date = datetime.strptime("20250528", "%Y%m%d")
        def parse_ts_for_filter(ts):
            return datetime.strptime(ts[:8], "%Y%m%d")
        df = df[df.iloc[:, 0].apply(lambda ts: parse_ts_for_filter(ts) <= cutoff_date)]
        logging.info(f"Rows after cutting off date: {df}")

        # Skip header row
        timestamps = df["timestamp"]
        logging.info(f"Timestamps: {timestamps}")

        # Parse to datetime objects
        datetime_series = pd.to_datetime(timestamps.str[:15], format="%Y%m%d-%H%M%S", errors="coerce")
        logging.info(f"Parsed timestamps: {datetime_series}")
        start, end = datetime_series.min().strftime("%d.%m.%Y %H:%M:%S"), datetime_series.max().strftime("%d.%m.%Y %H:%M:%S")

        # === Plot inserts and deletes (bar plot) ===
        fig, ax = plt.subplots()

        # Data
        added = df["insertions"].astype(int).values
        deleted = df["deletions"].astype(int).values
        added_net = added - deleted  # Positive = net insertion, Negative = net deletion

        x = np.arange(len(datetime_series))
        width = 0.6

        used_labels = set()

        for i in range(len(x)):
            net = added_net[i]
            ins = added[i]
            dels = deleted[i]
            base = x[i]

            if net >= 0:
                # Green base bar for net insertions
                label = "Net Insertions" if "Net Insertions" not in used_labels and net > 0 else None
                ax.bar(base, net, width=width, color="green", label=label)
                if label: used_labels.add(label)

                # Red overlay for the deleted portion
                if dels > 0:
                    label = "Deleted" if "Deleted" not in used_labels else None
                    ax.bar(base, dels, width=width, bottom=net, color="#FFB6C1", label=label)
                    if label: used_labels.add(label)

            else:
                # Red base bar for net deletions
                label = "Net Deletions" if "Net Deletions" not in used_labels and net != 0 else None
                ax.bar(base, -net, width=width, color="red", label=label)
                if label: used_labels.add(label)

                # Green overlay for the inserted portion
                if ins > 0:
                    label = "Inserted" if "Inserted" not in used_labels else None
                    ax.bar(base, ins, width=width, bottom=-net, color="#98FB98", label=label)
                    if label: used_labels.add(label)
        
        ax.set_xlabel("Timestamp")
        ax.set_ylabel("Triple Count")
        ax.set_title(f"Triple Changes Over Time for {repo_name}")

        ax.legend()

        # Set 5 evenly spaced tick positions and labels
        n = len(datetime_series)
        tick_indices = [0, n // 4, n // 2, 3 * n // 4, n - 1]
        tick_labels = [datetime_series.iloc[i].strftime("%d.%m.%Y\n%H:%M:%S") for i in tick_indices]

        ax.set_xticks(tick_indices)
        ax.set_xticklabels(tick_labels, rotation=45, size=8)

        ax.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
        fig.tight_layout()

        buffer = BytesIO()
        plt.savefig(buffer, format="svg", bbox_inches='tight')
        plt.close(fig)
        buffer.seek(0)
        delta_plot = buffer.getvalue().decode('utf-8')

        # === Plot total triples over time (line plot) ===
        fig, ax = plt.subplots()
        
        total_triples = df["cnt_triples"].astype(int).values
        ax.plot(x, total_triples, label="Total Triples", color="blue")  # Use numeric x

        ax.set_xlabel("Timestamp")
        ax.set_ylabel("Total Triples")
        ax.set_title(f"Total Triples Over Time for {repo_name}")
        ax.legend()

        # Use same x-ticks and labels as the bar plot
        ax.set_xticks(tick_indices)
        ax.set_xticklabels(tick_labels, rotation=45, size=8)

        ax.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))
        fig.tight_layout()

        buffer = BytesIO()
        plt.savefig(buffer, format="svg", bbox_inches='tight')
        plt.close(fig)
        buffer.seek(0)
        total_plot = buffer.getvalue().decode('utf-8')

        return start, end, total_plot


    def get_repo_tracking_infos(self):
        repo_name = self.repo_name

        session = next(get_session())
        tracking_infos = get_dataset_metadata_by_repo_name(repo_name, session)
        logging.info(tracking_infos)
        rdf_dataset_url = tracking_infos[1]
        polling_interval = tracking_infos[2]
        session.close()

        logging.info(f"Tracking infos for {repo_name}: {rdf_dataset_url}; {polling_interval}")
        return rdf_dataset_url, polling_interval