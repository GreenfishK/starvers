
from abc import ABC, abstractmethod
import os
import re
import shutil
import pandas as pd
from starvers.starvers import TripleStoreEngine

from app.LoggingConfig import get_logger
from app.models.TrackingTaskModel import TrackingTaskDto
from app.utils.FileService import download_file, skolemize_blank_nodes_in_file
from app.utils.HelperService import convert_to_df, get_timestamp
from app.utils.graphdb.GraphDatabaseUtils import get_construct_all_template, get_construct_all_versioned_template, get_delta_query_deletions_template, get_delta_query_insertions_template, get_drop_graph_template, import_serverfile, poll_import_status

class DeltaCalculationService(ABC):
    def __init__(self, repository_name):
        super().__init__()
        self.repository_name = repository_name

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
    def __init__(self, starvers_engine: TripleStoreEngine, tracking_task: TrackingTaskDto, repository_name: str) -> None:
        super().__init__(repository_name)
        self.LOG = get_logger(__name__, f"tracking_{tracking_task.name}.log")
        self.__starvers_engine = starvers_engine
        self.tracking_task = tracking_task


    def set_version_timestamp(self, version_timestamp):
        self.__version_timestamp = version_timestamp


    def prepare(self, local_file: bool = False):
        self.load_rdf_data(self.tracking_task.name_temp(), local_file)


    def calculate_delta(self):
        self.LOG.info(f"Repository name: {self.repository_name}: Get latest versions to calculate delta")
        self.__starvers_engine.sparql_get_with_post.setReturnFormat('n3') # set to n3 return data
        self.__starvers_engine.sparql_get_with_post.addCustomHttpHeader('Accept', 'application/n-triples')

        self.__starvers_engine.sparql_get_with_post.setQuery(get_construct_all_template(self.tracking_task.name_temp())) #no versioning necessary
        latest_text = self.__starvers_engine.sparql_get_with_post.query().convert().decode("utf-8")
        
        self.__starvers_engine.sparql_get_with_post.setQuery(get_construct_all_versioned_template(self.__version_timestamp))
        versioned_text = self.__starvers_engine.sparql_get_with_post.query().convert().decode("utf-8")

        self.__starvers_engine.sparql_get_with_post.setReturnFormat('json') # return to default behaviour
        self.__starvers_engine.sparql_get_with_post.clearCustomHttpHeader('Accept')
        
        self.LOG.info(f"Repository name: {self.repository_name}: Convert latest available dump into rdf dataframe incl cleanup")
        latest_text = re.sub(r'[\u0000-\u0008\u0009\u000B\u000C\u000E-\u001F\u007F\u00A0\u2028\u2029]', ' ', latest_text)
        latest = convert_to_df(latest_text)
        self.LOG.info(f"Repository name: {self.repository_name}: Convert latest version dump into rdf dataframe incl cleanup")
        versioned_text = re.sub(r'[\u0000-\u0008\u0009\u000B\u000C\u000E-\u001F\u007F\u00A0\u2028\u2029]', ' ', versioned_text)
        versioned = convert_to_df(versioned_text)

        self.LOG.info(f"Repository name: {self.repository_name}: Calculate delta - Insertions & Deletions")
        insertions, deletions = self.__calculate_delta(versioned, latest)

        return insertions, deletions

    
    def clean_up(self):
        self.LOG.info(f"Repository name: {self.repository_name}: Clean up - remove temp graph {self.tracking_task.name_temp()}")
        self.__starvers_engine.sparql_post.setQuery(get_drop_graph_template(self.tracking_task.name_temp()))
        self.__starvers_engine.sparql_post.query()


    def load_rdf_data(self, graph_name: str = None, local_file: bool = False):
        base_path = f"./evaluation/{self.tracking_task.name}/{get_timestamp(self.__version_timestamp)}/"
        os.makedirs(os.path.dirname(base_path), exist_ok=True)
        snapshot_path = f"{base_path}{self.tracking_task.name}_{get_timestamp(self.__version_timestamp)}.raw.nt"
        processed_path = f"{base_path}{self.tracking_task.name}_{get_timestamp(self.__version_timestamp)}.nt"
        import_path = f"/graphdb-import/{self.tracking_task.name}.nt"
        
        if not local_file:
            # download file into graph db server
            for attempt in range(2):
                self.LOG.info(f"Repository name: {self.repository_name}: Download rdf data ({attempt+1}. attempt)")
                try:
                    download_file(self.tracking_task.rdf_dataset_url, snapshot_path)
                    break  # success
                except Exception as e:
                    if attempt == 1:
                        self.LOG.info("Repository name: {self.repository_name}: Download failed after 2 attempts.")
                        raise
                    self.LOG.warning("Repository name: {self.repository_name}: Retrying after error: %s", e)
        else:   
            self.LOG.info(f"Local rdf data provided. Copy {self.tracking_task.name}_{get_timestamp(self.__version_timestamp)}.raw.nt into snapshot path")
            shutil.copy2(f"./evaluation/{self.tracking_task.name}/{self.tracking_task.name}_{get_timestamp(self.__version_timestamp)}.raw.nt", snapshot_path)

        #cleanup file
        self.LOG.info(f"Repository name: {self.repository_name}: Skolemize rdf data")
        skolemize_blank_nodes_in_file(snapshot_path, processed_path)

        #copy to graphdb server file directory
        self.LOG.info(f"Repository name: {self.repository_name}: Copy rdf data into graphdb-import directory")
        shutil.copy2(processed_path, import_path)

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
    def __init__(self, starvers_engine: TripleStoreEngine, tracking_task: TrackingTaskDto, repository_name: str) -> None:
        super().__init__(repository_name)
        self.LOG = get_logger(__name__, f"tracking_{tracking_task.name}.log")
        self.__starvers_engine = starvers_engine
        self.tracking_task = tracking_task

    
    def set_version_timestamp(self, version_timestamp):
        self.__version_timestamp = version_timestamp


    def prepare(self, local_file: bool = False):
        self.load_rdf_data(self.tracking_task.name_temp(), local_file)


    def calculate_delta(self):
        self.__starvers_engine.sparql_get_with_post.setReturnFormat('n3')
        self.__starvers_engine.sparql_get_with_post.addCustomHttpHeader('Accept', 'application/n-triples')
    
        self.LOG.info(f"Repository name: {self.repository_name}: Calculate Delta - Insertions")
        self.__starvers_engine.sparql_get_with_post.setQuery(get_delta_query_insertions_template(self.__version_timestamp, self.tracking_task.name_temp()))
        insertions_text = self.__starvers_engine.sparql_get_with_post.query().convert().decode("utf-8")
        # TODO: fix warnings:
        # /usr/local/lib/python3.10/site-packages/SPARQLWrapper/Wrapper.py:348: SyntaxWarning: Ignore format 'N3'; current instance supports: json, xml, turtle, n3, rdf, rdf+xml, csv, tsv, json-ld.
        # warnings.warn([2025-07-28 16:51:33,135] [INFO] app.services.DeltaCalculationService: Calculate Delta - Insertions
        # /usr/local/lib/python3.10/site-packages/SPARQLWrapper/Wrapper.py:794: RuntimeWarning: Sending Accept header '*/*' because unexpected returned format 'json' in a 'CONSTRUCT' SPARQL query form
        # warnings.warn(/usr/local/lib/python3.10/site-packages/SPARQLWrapper/Wrapper.py:1179: RuntimeWarning: Format requested was JSON, but N3 (application/n-triples;charset=UTF-8) has been returned by the endpoint

        self.LOG.info(f"Repository name: {self.repository_name}: Calculate Delta - Deletions")
        self.__starvers_engine.sparql_get_with_post.setQuery(get_delta_query_deletions_template(self.__version_timestamp, self.tracking_task.name_temp()))
        deletions_text = self.__starvers_engine.sparql_get_with_post.query().convert().decode("utf-8")

        self.__starvers_engine.sparql_get_with_post.setReturnFormat('JSON') # return to default behaviour
        self.__starvers_engine.sparql_get_with_post.clearCustomHttpHeader('Accept')

        insertions = convert_to_df(insertions_text)
        deletions = convert_to_df(deletions_text)

        return insertions, deletions


    def clean_up(self):
        self.LOG.info(f"Repository name: {self.repository_name}: Clean up - remove temp graph {self.tracking_task.name_temp()}")
        self.__starvers_engine.sparql_post.setQuery(get_drop_graph_template(self.tracking_task.name_temp()))
        self.__starvers_engine.sparql_post.query()


    def load_rdf_data(self, graph_name: str = None, local_file: bool = False):
        base_path = f"./evaluation/{self.tracking_task.name}/{get_timestamp(self.__version_timestamp)}/"
        os.makedirs(os.path.dirname(base_path), exist_ok=True)
        snapshot_path = f"{base_path}{self.tracking_task.name}_{get_timestamp(self.__version_timestamp)}.raw.nt"
        processed_path = f"{base_path}{self.tracking_task.name}_{get_timestamp(self.__version_timestamp)}.nt"
        import_path = f"/graphdb-import/{self.tracking_task.name}.nt"
        
        if not local_file:
            # download file into graph db server
            for attempt in range(2):
                self.LOG.info(f"Repository name: {self.repository_name}: Download rdf data ({attempt+1}. attempt)")
                try:
                    download_file(self.tracking_task.rdf_dataset_url, snapshot_path)
                    break  # success
                except Exception as e:
                    if attempt == 1:
                        self.LOG.info("Repository name: {self.repository_name}: Download failed after 2 attempts.")
                        raise
                    self.LOG.warning("Repository name: {self.repository_name}: Retrying after error: %s", e)
        else:   
            self.LOG.info(f"Repository name: {self.repository_name}: Local rdf data provided. Copy {self.tracking_task.name}_{get_timestamp(self.__version_timestamp)}.raw.nt into snapshot path")
            shutil.copy2(f"./evaluation/{self.tracking_task.name}/{self.tracking_task.name}_{get_timestamp(self.__version_timestamp)}.raw.nt", snapshot_path)

        #cleanup file
        self.LOG.info(f"Repository name: {self.repository_name}: Skolemize rdf data")
        skolemize_blank_nodes_in_file(snapshot_path, processed_path)

        #copy to graphdb server file directory
        self.LOG.info(f"Repository name: {self.repository_name}: Copy rdf data into graphdb-import directory")
        shutil.copy2(processed_path, import_path)

        #start import server file
        import_serverfile(f"{self.tracking_task.name}.nt", self.tracking_task.name, graph_name)
        poll_import_status(f"{self.tracking_task.name}.nt", self.tracking_task.name)
