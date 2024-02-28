from uuid import UUID
from datetime import datetime
import logging
from typing import List, Tuple
from pandas import DataFrame
from starvers.starvers import TripleStoreEngine
from SPARQLWrapper import SPARQLWrapper, POST, DIGEST

from app.AppConfig import Settings
from app.enums.DeltaQueryEnum import DeltaQuery
from app.services.DeltaQueryService import IterativeDeltaQueryService, SparqlDeltaQueryService
from app.utils.graphdb.GraphDatabaseUtils import getInsertTemplate, getQueryAllTemplate

LOG = logging.getLogger(__name__)

class StarVersService():
    def __init__(self, repository_name: str, knowledge_graph_id: UUID, rdf_store_url: str, delta_query_type: DeltaQuery) -> None:
        self.repository_name = repository_name
        self.knowledge_graph_id = knowledge_graph_id
        self.rdf_store_url = rdf_store_url
        self.delta_query_type = delta_query_type
        
        match delta_query_type:
            case DeltaQuery.ITERATIVE:
                self.__delta_query_service = IterativeDeltaQueryService()
            case DeltaQuery.SPARQL:
                self.__delta_query_service = SparqlDeltaQueryService()

        self.__graph_db_get_endpoint = Settings().graph_db_url_get_endpoint.replace('{:repo_name}', self.repository_name)
        self.__graph_db_post_endpoint = Settings().graph_db_url_post_endpoint.replace('{:repo_name}', self.repository_name)

        self.__starvers_engine = TripleStoreEngine(self.__graph_db_get_endpoint, self.__graph_db_post_endpoint)
        self.__sparql_wrapper = SPARQLWrapper(self.__graph_db_post_endpoint)
        self.__sparql_wrapper.setHTTPAuth(DIGEST)
        self.__sparql_wrapper.setMethod(POST)

    def push_initial_dataset(self, data: str, version_timestamp):
        insert = getInsertTemplate(data)
        self.__sparql_wrapper.setQuery(insert)
        self.__sparql_wrapper.query()

        self.__starvers_engine.version_all_triples(initial_timestamp=version_timestamp)

    def query(self, query: str, timestamp: datetime = None, query_as_timestamped: bool = True):
        LOG.info(f"Query at timestamp={timestamp} from repository {self.repository_name} with uuid={self.knowledge_graph_id}")
        return self.__starvers_engine.query(query, timestamp, query_as_timestamped)

    def get_latest_version(self):
        query = getQueryAllTemplate()
        return self.__convert_df_to_triples(self.query(query))

    def run_versioning(self, version_timestamp) -> bool:
        tmp_name = self.repository_name + "_tmp_versioning"
        self.__delta_query_service.prepare(tmp_name)

        starvers_tmp = StarVersService(tmp_name, self.knowledge_graph_id, self.rdf_store_url, None)
        inserts, deletions = self.__delta_query_service.calculate_delta(self, starvers_tmp)        

        LOG.info(f"Found {len(inserts)} inserts and {len(deletions)} deletions for knowledge graph with uuid={self.knowledge_graph_id}")
        
        self.__starvers_engine.insert(self.__convert_to_n3(inserts), timestamp=version_timestamp)
        self.__starvers_engine.outdate(self.__convert_to_n3(deletions), timestamp=version_timestamp)
        
        self.__delta_query_service.clean_up(tmp_name)

        return len(inserts) > 0 or len(deletions) > 0
    
    def __convert_df_to_triples(self, df: DataFrame) -> List[Tuple]:
        result = []
        for index in df.index:
            result.append((df['x'][index], df['y'][index], df['z'][index]))
        return result


    def __convert_to_n3(self, triples: List[Tuple]):
        n3: List[str] = []
        for triple in triples:
            n3.append(f"<{triple[0]}> <{triple[1]}> {triple[2]} .")

        return n3
            
        
        
