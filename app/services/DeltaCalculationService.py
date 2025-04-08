
from abc import ABC, abstractmethod
import logging
import os
import shutil
import pandas as pd
from starvers.starvers import TripleStoreEngine

from app.models.TrackingTaskModel import TrackingTaskDto
from app.utils.FileService import download_file, skolemize_blank_nodes_in_file
from app.utils.HelperService import get_timestamp
from app.utils.graphdb.GraphDatabaseUtils import get_delta_query_deletions_template, get_delta_query_insertions_template, get_drop_graph_template, get_query_all_template, import_serverfile

LOG = logging.getLogger(__name__)

class DeltaCalculationService(ABC):
    @abstractmethod
    def set_version_timestamp():
        pass

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
    def __init__(self, starvers_engine: TripleStoreEngine, tracking_task: TrackingTaskDto) -> None:
        super().__init__()
        self.__starvers_engine = starvers_engine
        self.tracking_task = tracking_task


    def set_version_timestamp(self, version_timestamp):
        self.__version_timestamp = version_timestamp


    def prepare(self):
        self.load_rdf_data(self.tracking_task.name_temp())


    def calculate_delta(self):
        LOG.info("Get latest Versions")
        latest = self.__starvers_engine.query(get_query_all_template(self.tracking_task.name_temp()), yn_timestamp_query=False) #no versioning necessary
        versioned = self.__starvers_engine.query(get_query_all_template(), self.__version_timestamp)

        LOG.info("Calculate Delta - Insertions & Deletions")
        insertions, deletions = self.__calculate_delta(versioned, latest)

        return insertions, deletions

    
    def clean_up(self):
        self.__starvers_engine.sparql_post.setQuery(get_drop_graph_template(self.tracking_task.name_temp()))
        self.__starvers_engine.sparql_post.query()


    def load_rdf_data(self, graph_name: str = None):
        base_path = f"./evaluation/{self.tracking_task.name}/{get_timestamp(self.__version_timestamp)}/"
        os.makedirs(os.path.dirname(base_path), exist_ok=True)

        snapshot_path = f"{base_path}{self.tracking_task.name}_{get_timestamp()}.raw.nt"
        processed_path = f"{base_path}{self.tracking_task.name}_{get_timestamp()}.nt"

        # download file into graph db server
        download_file(self.tracking_task.rdf_dataset_url, snapshot_path)

        #cleanup file
        skolemize_blank_nodes_in_file(snapshot_path, processed_path)

        #copy to graphdb server file directory
        shutil.copy2(processed_path, f'/opt/graphdb/home/graphdb-import/{self.tracking_task.name}.nt')

        #start import server file
        import_serverfile(f"{self.tracking_task.name}.nt", self.tracking_task.name, graph_name)
    
    
    def __calculate_delta(self, df_versioned: pd.DataFrame, df_latest: pd.DataFrame):
        delta = df_versioned[['s', 'p', 'o']].merge(df_latest[['s', 'p', 'o']], on=['s', 'p', 'o'], how='outer', indicator=True)

        # Rows only in df_versioned (deletions)
        deletions = delta[delta['_merge'] == 'left_only'].drop(columns=['_merge'])
        # Rows only in df_latest (insertions)
        insertions = delta[delta['_merge'] == 'right_only'].drop(columns=['_merge'])

        return insertions, deletions
    

class SparqlDeltaQueryService(DeltaCalculationService):
    def __init__(self, starvers_engine: TripleStoreEngine, tracking_task: TrackingTaskDto) -> None:
        super().__init__()
        self.__starvers_engine = starvers_engine
        self.tracking_task = tracking_task

    
    def set_version_timestamp(self, version_timestamp):
        self.__version_timestamp = version_timestamp


    def prepare(self):
        self.load_rdf_data(self.tracking_task.name_temp())


    def calculate_delta(self):
        LOG.info("Calculate Delta - Insertions")
        insertions = self.__starvers_engine.query(get_delta_query_insertions_template(self.__version_timestamp, self.tracking_task.name_temp()), yn_timestamp_query=False)

        LOG.info("Calculate Delta - Deletions")
        deletions = self.__starvers_engine.query(get_delta_query_deletions_template(self.__version_timestamp, self.tracking_task.name_temp()), yn_timestamp_query=False)

        return insertions, deletions


    def clean_up(self):
        import_path = f"/opt/graphdb/home/graphdb-import/{self.tracking_task.name}.nt"
        os.remove(import_path)

        self.__starvers_engine.sparql_post.setQuery(get_drop_graph_template(self.tracking_task.name_temp()))
        self.__starvers_engine.sparql_post.query()


    def load_rdf_data(self, graph_name: str = None):
        base_path = f"./evaluation/{self.tracking_task.name}/{get_timestamp(self.__version_timestamp)}/"
        os.makedirs(os.path.dirname(base_path), exist_ok=True)

        snapshot_path = f"{base_path}{self.tracking_task.name}_{get_timestamp()}.raw.nt"
        processed_path = f"{base_path}{self.tracking_task.name}_{get_timestamp()}.nt"
        import_path = f"/opt/graphdb/home/graphdb-import/{self.tracking_task.name}.nt"

        # download file into graph db server
        download_file(self.tracking_task.rdf_dataset_url, snapshot_path)

        #cleanup file
        skolemize_blank_nodes_in_file(snapshot_path, processed_path)

        #copy to graphdb server file directory
        shutil.copy2(processed_path, import_path)

        #start import server file
        import_serverfile(f"{self.tracking_task.name}.nt", self.tracking_task.name, graph_name)