
from abc import ABC, abstractmethod
import logging
import pandas as pd
from starvers.starvers import TripleStoreEngine

from app.utils.graphdb.GraphDatabaseUtils import get_delta_query_deletions_template, get_delta_query_insertions_template, get_drop_graph_template, get_load_template, get_query_all_template

LOG = logging.getLogger(__name__)

class DeltaCalculationService(ABC):
    @abstractmethod
    def prepare():
        pass

    @abstractmethod
    def calculate_delta():
        pass
    
    @abstractmethod
    def clean_up():
        pass

    @abstractmethod
    def load_rdf_data():
        pass



class IterativeDeltaQueryService(DeltaCalculationService):
    def __init__(self, starvers_engine: TripleStoreEngine, rdf_dataset_url: str, tmp_graph_name: str) -> None:
        super().__init__()
        self.__starvers_engine = starvers_engine
        self.__rdf_dataset_url = rdf_dataset_url
        self.__tmp_graph_name = tmp_graph_name  


    def prepare(self):
        # TODO: add support for archives/zip files, various RDF serializations such as RDF/turtle, RDF/xml, owl files
        # sometimes one file, sometimes multiple files

        self.load_rdf_data(self.__tmp_graph_name)


    def calculate_delta(self, timestamp):
        LOG.info("Get latest Versions")
        latest = self.__starvers_engine.query(get_query_all_template(self.__tmp_graph_name), yn_timestamp_query=False) #no versioning necessary

        versioned = self.__starvers_engine.query(get_query_all_template(), timestamp)

        LOG.info("Calculate Delta - Insertions & Deletions")
        insertions, deletions = self.__calculate_delta(versioned, latest)

        return insertions, deletions

    
    def clean_up(self):
        self.__starvers_engine.sparql_post.setQuery(get_drop_graph_template(self.__tmp_graph_name))
        self.__starvers_engine.sparql_post.query()


    def load_rdf_data(self, graph_name: str = None):
        self.__starvers_engine.sparql_post.setQuery(get_load_template(self.__rdf_dataset_url, graph_name))
        self.__starvers_engine.sparql_post.query()
    
    
    def __calculate_delta(self, df_versioned: pd.DataFrame, df_latest: pd.DataFrame):
        delta = df_versioned[['s', 'p', 'o']].merge(df_latest[['s', 'p', 'o']], on=['s', 'p', 'o'], how='outer', indicator=True)

        # Rows only in df_versioned (deletions)
        deletions = delta[delta['_merge'] == 'left_only'].drop(columns=['_merge'])
        # Rows only in df_latest (insertions)
        insertions = delta[delta['_merge'] == 'right_only'].drop(columns=['_merge'])

        return insertions, deletions
    

class SparqlDeltaQueryService(DeltaCalculationService):
    def __init__(self, starvers_engine: TripleStoreEngine, rdf_dataset_url: str, tmp_graph_name: str) -> None:
        super().__init__()
        self.__starvers_engine = starvers_engine
        self.__rdf_dataset_url = rdf_dataset_url
        self.__tmp_graph_name = tmp_graph_name


    def prepare(self):
        self.load_rdf_data(self.__tmp_graph_name)


    def calculate_delta(self, timestamp):
        LOG.info("Calculate Delta - Insertions")
        insertions = self.__starvers_engine.query(get_delta_query_insertions_template(timestamp, self.__tmp_graph_name), yn_timestamp_query=False)

        LOG.info("Calculate Delta - Deletions")
        deletions = self.__starvers_engine.query(get_delta_query_deletions_template(timestamp, self.__tmp_graph_name), yn_timestamp_query=False)

        return insertions, deletions


    def clean_up(self):
        self.__starvers_engine.sparql_post.setQuery(get_drop_graph_template(self.__tmp_graph_name))
        self.__starvers_engine.sparql_post.query()


    def load_rdf_data(self, graph_name: str = None):
        self.__starvers_engine.sparql_post.setQuery(get_load_template(self.__rdf_dataset_url, graph_name))
        self.__starvers_engine.sparql_post.query()
        