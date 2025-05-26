from datetime import datetime
import pandas as pd
import matplotlib.pyplot as plt
import logging
from io import BytesIO
import base64
import os
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
        timestamps = df.iloc[:, 0]

        def parse_ts(ts):
            dt = datetime.strptime(ts[:15], "%Y%m%d-%H%M%S")
            return dt.strftime("%d.%m.%Y %H:%M:%S")

        formatted_timestamps = timestamps.apply(parse_ts)
        start, end = formatted_timestamps.min(), formatted_timestamps.max()

        # Plot
        fig, ax = plt.subplots()
        ax.plot(formatted_timestamps, df.iloc[:, 1], label="Added Triples")
        ax.plot(formatted_timestamps, df.iloc[:, 2], label="Deleted Triples")
        ax.set_xlabel("Timestamp")
        ax.set_ylabel("Triple Count")
        ax.set_title(f"Triple Changes Over Time for {repo_name}")
        ax.legend()

        #ax.set_yscale('log')  # Set y-axis to logarithmic scale

        # Set 5 evenly spaced x-ticks: first, three in between, last
        n = len(formatted_timestamps)
        tick_indices = [0, n // 4, n // 2, 3 * n // 4, n - 1]
        tick_positions = [formatted_timestamps.iloc[i] for i in tick_indices]
        tick_labels = [ts.replace(" ", "\n") for ts in tick_positions]
        ax.set_xticks(tick_positions)
        ax.set_xticklabels(tick_labels, rotation=45, size=8)

        # Format y-ticks with thousand separator
        ax.get_yaxis().set_major_formatter(plt.FuncFormatter(lambda x, _: f"{int(x):,}"))

        fig.tight_layout()

        # Convert plot to SVG
        buffer = BytesIO()
        plt.savefig(buffer, format="svg", bbox_inches='tight')
        plt.close(fig)
        buffer.seek(0)
        svg_data = buffer.getvalue().decode('utf-8')
        return start, end, svg_data


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