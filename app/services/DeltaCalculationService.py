
from abc import ABC, abstractmethod
import os
import shutil
import pandas as pd
from typing import List, Tuple
from starvers.starvers import TripleStoreEngine

from app.LoggingConfig import get_logger
from app.models.TrackingTaskModel import TrackingTaskDto
from app.utils.FileService import download_file, skolemize_blank_nodes_in_file
from app.utils.HelperService import get_timestamp, convert_n3_to_list
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


    def calculate_delta(self) -> Tuple[List[str],List[str]]:
        # set to n3 return data. Warning(!): Works with CONSTRUCT queries but GraphDB won't return n3 for SELECT queries
        self.__starvers_engine.sparql_get_with_post.setReturnFormat('n3') 
        self.__starvers_engine.sparql_get_with_post.addCustomHttpHeader('Accept', 'application/n-triples')

        # New dump
        self.LOG.info(f"Repository name: {self.repository_name}: Query new dump from a named graph from the triple store.")
        self.__starvers_engine.sparql_get_with_post.setQuery(get_construct_all_template(self.tracking_task.name_temp())) #no versioning necessary
        latest_n3_str = self.__starvers_engine.sparql_get_with_post.query().convert().decode("utf-8")

        # Latest version
        self.LOG.info(f"Repository name: {self.repository_name}: Query latest version from triple store.")
        self.__starvers_engine.sparql_get_with_post.setQuery(get_construct_all_versioned_template(self.__version_timestamp))
        versioned_n3_str = self.__starvers_engine.sparql_get_with_post.query().convert().decode("utf-8")

        self.__starvers_engine.sparql_get_with_post.setReturnFormat('json') # return to default behaviour
        self.__starvers_engine.sparql_get_with_post.clearCustomHttpHeader('Accept')

        self.LOG.info(f"Repository name: {self.repository_name}: Calculate delta - Insertions & Deletions")
        insertions_n3, deletions_n3 = self.__calculate_delta_n3(versioned_n3_str, latest_n3_str)

        return insertions_n3, deletions_n3
    
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

        # cleanup file
        self.LOG.info(f"Repository name: {self.repository_name}: Skolemize {self.tracking_task.name}_{get_timestamp(self.__version_timestamp)}.raw.nt")
        skolemize_blank_nodes_in_file(snapshot_path, processed_path)

        # copy to graphdb server file directory
        self.LOG.info(f"Repository name: {self.repository_name}: Copy {self.tracking_task.name}_{get_timestamp(self.__version_timestamp)}.nt into {import_path}")
        shutil.copy2(processed_path, import_path)

        # start import server file
        import_serverfile(f"{self.tracking_task.name}.nt", self.tracking_task.name, graph_name)
        poll_import_status(f"{self.tracking_task.name}.nt", self.tracking_task.name)

    
    def __calculate_delta(self, df_versioned: pd.DataFrame, df_latest: pd.DataFrame):
        delta = df_versioned[['s', 'p', 'o']].merge(df_latest[['s', 'p', 'o']], on=['s', 'p', 'o'], how='outer', indicator=True)

        # Rows only in df_versioned (deletions)
        deletions = delta[delta['_merge'] == 'left_only'].drop(columns=['_merge'])
        # Rows only in df_latest (insertions)
        insertions = delta[delta['_merge'] == 'right_only'].drop(columns=['_merge'])

        return insertions, deletions
    
    def __calculate_delta_n3(self, versioned_n3: str, latest_n3: str) -> Tuple[List[str], List[str]]:
        # Convert n3 to lists and then to sets of triples
        versioned_triples = set(convert_n3_to_list(versioned_n3))
        latest_triples = set(convert_n3_to_list(latest_n3))

        # Calculate differences
        insertions = latest_triples - versioned_triples
        deletions = versioned_triples - latest_triples

        # Convert sets to lists        
        insertions_n3 = list(insertions)
        deletions_n3 = list(deletions)

        return insertions_n3, deletions_n3
    

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

        # Insertions
        self.LOG.info(f"Repository name: {self.repository_name}: Calculate Delta with SPARQL - Insertions")
        self.__starvers_engine.sparql_get_with_post.setQuery(get_delta_query_insertions_template(self.__version_timestamp, self.tracking_task.name_temp())) #no versioning necessary
        insertions_n3_str = self.__starvers_engine.sparql_get_with_post.query().convert().decode("utf-8")
        self.LOG.info(f"Repository name: {self.repository_name}: Convert n3 string of insertions to list.")
        insertions_n3 = convert_n3_to_list(insertions_n3_str)

        # Deletions
        self.LOG.info(f"Repository name: {self.repository_name}: Calculate Delta with SPARQL - Deletions")
        self.__starvers_engine.sparql_get_with_post.setQuery(get_delta_query_deletions_template(self.__version_timestamp, self.tracking_task.name_temp())) #no versioning necessary
        deletions_n3_str = self.__starvers_engine.sparql_get_with_post.query().convert().decode("utf-8")
        self.LOG.info(f"Repository name: {self.repository_name}: Convert n3 string of deletions to list.")
        deletions_n3 = convert_n3_to_list(deletions_n3_str)

        # Return to default behavior
        self.__starvers_engine.sparql_get_with_post.setReturnFormat('json') 
        self.__starvers_engine.sparql_get_with_post.clearCustomHttpHeader('Accept')

        return insertions_n3, deletions_n3


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
        self.LOG.info(f"Repository name: {self.repository_name}: Skolemize {self.tracking_task.name}_{get_timestamp(self.__version_timestamp)}.raw.nt")
        skolemize_blank_nodes_in_file(snapshot_path, processed_path)

        #copy to graphdb server file directory
        self.LOG.info(f"Repository name: {self.repository_name}: Copy {self.tracking_task.name}_{get_timestamp(self.__version_timestamp)}.nt into {import_path}")
        shutil.copy2(processed_path, import_path)

        #start import server file
        import_serverfile(f"{self.tracking_task.name}.nt", self.tracking_task.name, graph_name)
        poll_import_status(f"{self.tracking_task.name}.nt", self.tracking_task.name)
