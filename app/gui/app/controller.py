from datetime import datetime
import pandas as pd
import logging
# starvers and starversServer imports
from starvers.starvers import TripleStoreEngine
from app.AppConfig import Settings


class GuiContr:
    def __init__(self, repo_name: str = "orkg_v2"):
        self.__graph_db_get_endpoint = f"http://rdfstore:7200/repositories/{repo_name}" 
        self.__graph_db_post_endpoint = f"http://rdfstore:7200/repositories/{repo_name}/statements" 
        self.__starvers_engine = TripleStoreEngine(self.__graph_db_get_endpoint, self.__graph_db_post_endpoint, skip_connection_test=True)


    def query(self, query: str, timestamp: datetime = None, query_as_timestamped: bool = True, repo_name: str = None) -> pd.DataFrame:
        if timestamp is not None and query_as_timestamped:
            print(f"Execute timestamped query with timestamp={timestamp}")
        else:
            print("Execute query without timestamp")

        return self.__starvers_engine.query(query, timestamp, query_as_timestamped)
