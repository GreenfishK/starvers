
from abc import ABC, abstractmethod
import os
import re
import shutil
import pandas as pd
from starvers.starvers import TripleStoreEngine

from app.LoggingConfig import get_tracking_logger
from app.models.TrackingTaskModel import TrackingTaskDto
from app.utils.FileService import download_file, skolemize_blank_nodes_in_file
from app.utils.HelperService import convert_to_df, get_timestamp
from app.utils.graphdb.GraphDatabaseUtils import get_construct_all_template, get_construct_all_versioned_template, get_delta_query_deletions_template, get_delta_query_insertions_template, get_drop_graph_template, get_query_all_template, import_serverfile, poll_import_status

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
        self.LOG = get_tracking_logger(tracking_task.name)
        self.__starvers_engine = starvers_engine
        self.tracking_task = tracking_task


    def set_version_timestamp(self, version_timestamp):
        self.__version_timestamp = version_timestamp


    def prepare(self):
        self.load_rdf_data(self.tracking_task.name_temp())


    def calculate_delta(self):
        self.LOG.info("Get latest versions to calculate delta")
        self.__starvers_engine.sparql_get_with_post.setReturnFormat('n3') # set to n3 return data
        self.__starvers_engine.sparql_get_with_post.addCustomHttpHeader('Accept', 'application/n-triples')

        self.__starvers_engine.sparql_get_with_post.setQuery(get_construct_all_template(self.tracking_task.name_temp())) #no versioning necessary
        latest_text = self.__starvers_engine.sparql_get_with_post.query().convert().decode("utf-8")
        
        self.__starvers_engine.sparql_get_with_post.setQuery(get_construct_all_versioned_template(self.__version_timestamp))
        versioned_text = self.__starvers_engine.sparql_get_with_post.query().convert().decode("utf-8")

        self.__starvers_engine.sparql_get_with_post.setReturnFormat('json') # return to default behaviour
        self.__starvers_engine.sparql_get_with_post.clearCustomHttpHeader('Accept')
        
        self.LOG.info("Convert latest available dump into rdf dataframe incl cleanup")
        latest_text = re.sub(r'[\u0000-\u0008\u0009\u000B\u000C\u000E-\u001F\u007F\u00A0\u2028\u2029]', ' ', latest_text)
        latest = convert_to_df(latest_text)
        self.LOG.info("Convert latest version dump into rdf dataframe incl cleanup")
        versioned_text = re.sub(r'[\u0000-\u0008\u0009\u000B\u000C\u000E-\u001F\u007F\u00A0\u2028\u2029]', ' ', versioned_text)
        versioned = convert_to_df(versioned_text)

        self.LOG.info("Calculate delta - Insertions & Deletions")
        insertions, deletions = self.__calculate_delta(versioned, latest)

        return insertions, deletions

    
    def clean_up(self):
        self.LOG.info(f"Clean up - remove temp graph {self.tracking_task.name_temp()}")
        self.__starvers_engine.sparql_post.setQuery(get_drop_graph_template(self.tracking_task.name_temp()))
        self.__starvers_engine.sparql_post.query()


    def load_rdf_data(self, graph_name: str = None):
        base_path = f"./evaluation/{self.tracking_task.name}/{get_timestamp(self.__version_timestamp)}/"
        os.makedirs(os.path.dirname(base_path), exist_ok=True)

        snapshot_path = f"{base_path}{self.tracking_task.name}_{get_timestamp()}.raw.nt"
        processed_path = f"{base_path}{self.tracking_task.name}_{get_timestamp()}.nt"

        # download file into graph db server
        for attempt in range(2):
            self.LOG.info(f"Download rdf data ({attempt+1}. attempt)")
            try:
                download_file(self.tracking_task.rdf_dataset_url, snapshot_path)
                break  # success
            except Exception as e:
                if attempt == 1:
                    self.LOG.info("Download failed after 2 attempts.")
                    raise
                self.LOG.warning("Retrying after error: %s", e)

        #cleanup file
        self.LOG.info("Skolemize rdf data")
        skolemize_blank_nodes_in_file(snapshot_path, processed_path)

        #copy to graphdb server file directory
        self.LOG.info("Copy rdf data into graphdb-import directory")
        shutil.copy2(processed_path, f'/graphdb-import/{self.tracking_task.name}.nt')

        #start import server file
        import_serverfile(f"{self.tracking_task.name}.nt", self.tracking_task.name, graph_name)
        poll_import_status(f"{self.tracking_task.name}.nt", self.tracking_task.name)

    
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
        self.LOG = get_tracking_logger(tracking_task.name)
        self.__starvers_engine = starvers_engine
        self.tracking_task = tracking_task

    
    def set_version_timestamp(self, version_timestamp):
        self.__version_timestamp = version_timestamp


    def prepare(self):
        self.load_rdf_data(self.tracking_task.name_temp())


    def calculate_delta(self):
        self.__starvers_engine.sparql_get_with_post.setReturnFormat('N3') # set to n3 return data
        self.__starvers_engine.sparql_get_with_post.addCustomHttpHeader('Accept', 'application/n-triples')
    
        self.LOG.info("Calculate Delta - Insertions")
        self.__starvers_engine.sparql_get_with_post.setQuery(get_delta_query_insertions_template(self.__version_timestamp, self.tracking_task.name_temp()))
        insertions_text = self.__starvers_engine.sparql_get_with_post.query().convert().decode("utf-8")

        self.LOG.info("Calculate Delta - Deletions")
        self.__starvers_engine.sparql_get_with_post.setQuery(get_delta_query_deletions_template(self.__version_timestamp, self.tracking_task.name_temp()))
        deletions_text = self.__starvers_engine.sparql_get_with_post.query().convert().decode("utf-8")

        self.__starvers_engine.sparql_get_with_post.setReturnFormat('JSON') # return to default behaviour
        self.__starvers_engine.sparql_get_with_post.clearCustomHttpHeader('Accept')

        insertions = convert_to_df(insertions_text)
        deletions = convert_to_df(deletions_text)

        return insertions, deletions


    def clean_up(self):
        self.LOG.info(f"Clean up - remove temp graph {self.tracking_task.name_temp()}")
        self.__starvers_engine.sparql_post.setQuery(get_drop_graph_template(self.tracking_task.name_temp()))
        self.__starvers_engine.sparql_post.query()


    def load_rdf_data(self, graph_name: str = None):
        base_path = f"./evaluation/{self.tracking_task.name}/{get_timestamp(self.__version_timestamp)}/"
        os.makedirs(os.path.dirname(base_path), exist_ok=True)

        snapshot_path = f"{base_path}{self.tracking_task.name}_{get_timestamp(self.__version_timestamp)}.raw.nt"
        processed_path = f"{base_path}{self.tracking_task.name}_{get_timestamp(self.__version_timestamp)}.nt"
        import_path = f"/graphdb-import/{self.tracking_task.name}.nt"

        # download file into graph db server
        for attempt in range(2):
            self.LOG.info(f"Download rdf data ({attempt+1}. attempt)")
            try:
                download_file(self.tracking_task.rdf_dataset_url, snapshot_path)
                break  # success
            except Exception as e:
                if attempt == 1:
                    self.LOG.info("Download failed after 2 attempts.")
                    raise
                self.LOG.warning("Retrying after error: %s", e)

        #cleanup file
        self.LOG.info("Skolemize rdf data")
        skolemize_blank_nodes_in_file(snapshot_path, processed_path)

        #copy to graphdb server file directory
        self.LOG.info("Copy rdf data into graphdb-import directory")
        shutil.copy2(processed_path, import_path)

        #start import server file
        import_serverfile(f"{self.tracking_task.name}.nt", self.tracking_task.name, graph_name)
        poll_import_status(f"{self.tracking_task.name}.nt", self.tracking_task.name)
