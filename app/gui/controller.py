from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import logging
from io import BytesIO
import base64
import os
import numpy as np
# starvers and starversServer imports
from starvers.starvers import TripleStoreEngine
from app.AppConfig import Settings
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

        return self.__starvers_engine.query(query, timestamp, query_as_timestamped)


    def get_repo_stats(self):
        repo_name = self.repo_name 
        path = f"/code/evaluation/{repo_name}/{repo_name}_timings.csv"
        df = pd.read_csv(path)
        timestamps = df.iloc[1:, 0]  # Skip header row

        # Parse to datetime objects
        def parse_ts(ts):
            return datetime.strptime(ts[:15], "%Y%m%d-%H%M%S")

        datetime_series = timestamps.apply(parse_ts)
        start, end = datetime_series.min().strftime("%d.%m.%Y %H:%M:%S"), datetime_series.max().strftime("%d.%m.%Y %H:%M:%S")

        # === Plot inserts and deletes (bar plot) ===
        fig, ax = plt.subplots()

        added = df.iloc[1:, 1].astype(int).values
        deleted = df.iloc[1:, 2].astype(int).values
        x = np.arange(len(datetime_series))  
        width = 0.4
        
        ax.bar(x - width/2, added, width=width, label="Added Triples", color="green")
        ax.bar(x + width/2, deleted, width=width, label="Deleted Triples", color="red")

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
        
        total_triples = df.iloc[1:, 7].astype(int).values
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

        return start, end, delta_plot, total_plot


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