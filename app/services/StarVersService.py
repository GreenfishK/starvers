from datetime import datetime
import logging
from typing import List, Tuple
from uuid import UUID
from pandas import DataFrame
from starvers.starvers import TripleStoreEngine
from SPARQLWrapper import SPARQLWrapper, POST, DIGEST

from app.AppConfig import Settings
from app.utils.graphdb.GraphDatabaseUtils import loadInsertTemplate, loadQueryAllTemplate

LOG = logging.getLogger(__name__)

class StarVersService():
    def __init__(self, repository_name: str, knowledge_graph_id: UUID) -> None:
        self.repository_name = repository_name
        self.knowledge_graph_id = knowledge_graph_id

        self.__graph_db_get_endpoint = Settings().graph_db_url_get_endpoint.replace('{:repo_name}', self.repository_name)
        self.__graph_db_post_endpoint = Settings().graph_db_url_post_endpoint.replace('{:repo_name}', self.repository_name)

        self.__starvers_engine = TripleStoreEngine(self.__graph_db_get_endpoint, self.__graph_db_post_endpoint)
        self.__sparql_wrapper = SPARQLWrapper(self.__graph_db_post_endpoint)
        self.__sparql_wrapper.setHTTPAuth(DIGEST)
        self.__sparql_wrapper.setMethod(POST)

    def push_initial_dataset(self, data: str):
        insert = loadInsertTemplate(data)
        self.__sparql_wrapper.setQuery(insert)
        self.__sparql_wrapper.query()

        self.__starvers_engine.version_all_triples()

    def query(self, query: str, timestamp: datetime, query_as_timestamped: bool = True):
        LOG.info(f"Query at timestamp={timestamp} from repository {self.repository_name} with uuid={self.knowledge_graph_id}")
        return self.__starvers_engine.query(query, timestamp, query_as_timestamped)

    def get_latest_version(self):
        query = loadQueryAllTemplate()
        query_result = self.__starvers_engine.query(query)
        return self.__convert_df_to_triples(query_result)

    def process_latest_version(self, newest_revision: str) -> bool:
        current_revision = self.get_latest_version()

        inserts = self.__calculate_delta(newest_revision, current_revision)
        deletions = self.__calculate_delta(current_revision, newest_revision, False)
        
        LOG.info(f"Found {len(inserts)} inserts and {len(deletions)} deletions for knowledge graph with uuid={self.knowledge_graph_id}")
        #TODO notify by RSS, Websocket, wenhook or whatever

        self.__starvers_engine.insert(self.__convert_to_n3(inserts))
        self.__starvers_engine.outdate(self.__convert_to_n3(deletions))
        return len(inserts) > 0 or len(deletions) > 0
    
    def __convert_df_to_triples(self, df: DataFrame) -> List[Tuple]:
        result = []
        for index in df.index:
            result.append((df['x'][index], df['y'][index], df['z'][index]))
        return result
    
    def __calculate_delta(self, triples1: List[Tuple], triples2: List[Tuple], respect_updates: bool = True) -> List[Tuple]:
        delta = []

        for t1 in triples1:
            for t2 in triples2:
                if t1[0] == t2[0] and t1[1] == t2[1]:
                    if t1[2] == t2[2]:
                        break
                    else:
                        #TODO handle possible changes???
                        pass
            else:
                delta.append(t1)

        return delta

    def __convert_to_n3(self, triples: List[Tuple]):
        n3: List[str] = []
        for triple in triples:
            n3.append(f"<{triple[0]}> <{triple[1]}> {triple[2]} .")

        print(n3)
        return n3
            
        
        
