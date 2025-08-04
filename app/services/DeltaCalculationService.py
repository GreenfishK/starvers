
from abc import ABC, abstractmethod
import os
import shutil
import pandas as pd
from typing import List, Tuple
from starvers.starvers import TripleStoreEngine

from app.LoggingConfig import get_logger
from app.models.TrackingTaskModel import TrackingTaskDto
from app.utils.HelperService import get_timestamp, to_list, download_file, normalize_and_skolemize
from app.utils.graphdb.GraphDatabaseUtils import get_construct_all_template, get_construct_all_versioned_template, get_delta_query_deletions_template, get_delta_query_insertions_template, get_drop_graph_template, import_serverfile, poll_import_status

class DeltaCalculationService(ABC):
    def __init__(self, starvers_engine: TripleStoreEngine, tracking_task: TrackingTaskDto, repository_name: str):
        super().__init__()
        self.LOG = get_logger(__name__, f"tracking_{tracking_task.name}.log")
        self._starvers_engine = starvers_engine
        self.tracking_task = tracking_task
        self.repository_name = repository_name
        self.version_timestamp = None
        self.dumps_path = None
        self.base_path = None
        self.snapshot_path = None
        self.processed_path = None

    @abstractmethod
    def prepare(self, local_file: bool = False):
        pass

    @abstractmethod
    def calculate_delta(self):
        pass

    def set_version_timestamp(self, version_timestamp):
        self.version_timestamp = version_timestamp

    def set_paths(self, version_timestamp):
        self.dumps_path = f"./evaluation/{self.tracking_task.name}"
        self.base_path = f"./evaluation/{self.tracking_task.name}/{get_timestamp(version_timestamp)}"
        self.snapshot_path = f"{self.base_path}/{self.tracking_task.name}_{get_timestamp(version_timestamp)}.raw.nt"
        self.processed_path = f"{self.base_path}/{self.tracking_task.name}_{get_timestamp(version_timestamp)}.nt"
        self.import_path = f"/graphdb-import/{self.tracking_task.name}.nt"
    
    @abstractmethod
    def clean_up(self):
        pass

    def download_data(self, local_file: bool = False):
        os.makedirs(self.base_path, exist_ok=True)
        snapshot_path = f"{self.base_path}/{self.tracking_task.name}_{get_timestamp(self.version_timestamp)}.raw.nt"

        if not local_file:
            for attempt in range(2):
                self.LOG.info(f"Repository name: {self.repository_name}: Download rdf data dump into {snapshot_path} ({attempt+1}. attempt)")
                try:
                    download_file(self.tracking_task.rdf_dataset_url, snapshot_path)
                    break
                except Exception as e:
                    if attempt == 1:
                        self.LOG.info(f"Repository name: {self.repository_name}: Download failed after 2 attempts.")
                        raise
                    self.LOG.warning(f"Repository name: {self.repository_name}: Retrying after error: %s", e)
        else:
            self.LOG.info(f"Repository name: {self.repository_name}: Local rdf data with path {self.tracking_task.name}_{get_timestamp(self.version_timestamp)}.raw.nt provided. Copy it into {snapshot_path}")
            shutil.copy2(f"{self.dumps_path}/{self.tracking_task.name}_{get_timestamp(self.version_timestamp)}.raw.nt", snapshot_path)


    def preprocess(self):
        self.LOG.info(f"Repository name: {self.repository_name}: Normalize and skolemize {self.tracking_task.name}_{get_timestamp(self.version_timestamp)}.raw.nt")
        normalize_and_skolemize(self.snapshot_path, self.processed_path)


    def load_rdf_data(self, graph_name: str = None):
        os.makedirs(self.base_path, exist_ok=True)

        # Compy into import directory
        self.LOG.info(f"Repository name: {self.repository_name}: Copy {self.tracking_task.name}_{get_timestamp(self.version_timestamp)}.nt into import directory: {self.import_path}")
        shutil.copy2(self.processed_path, self.import_path)

        # Import into GraphDB
        import_serverfile(f"{self.tracking_task.name}.nt", self.tracking_task.name, graph_name)
        poll_import_status(f"{self.tracking_task.name}.nt", self.tracking_task.name)


class IterativeDeltaQueryService(DeltaCalculationService):
    def __init__(self, starvers_engine: TripleStoreEngine, tracking_task: TrackingTaskDto, repository_name: str) -> None:
        super().__init__(starvers_engine, tracking_task, repository_name)


    def prepare(self, version_timestamp, local_file: bool = False):
        self.set_version_timestamp(version_timestamp)
        self.set_paths(version_timestamp)
        
        # Download data
        self.download_data(local_file)

        # Skolemize and normalize (remove control characters)
        self.preprocess()


    def calculate_delta(self) -> Tuple[List[str],List[str]]:
        # New dump
        self.LOG.info(f"Repository name: {self.repository_name}: Load new dump into string.")
        with open(self.processed_path, "r", encoding="utf-8") as rdf_file:
            latest_n3_str = rdf_file.read()

        # Latest version
        self.LOG.info(f"Repository name: {self.repository_name}: Query latest version from triple store.")
        self._starvers_engine.sparql_get_with_post.setReturnFormat('n3') 
        self._starvers_engine.sparql_get_with_post.addCustomHttpHeader('Accept', 'application/n-triples')
        self._starvers_engine.sparql_get_with_post.setQuery(get_construct_all_versioned_template(self.version_timestamp))
        versioned_n3_str = self._starvers_engine.sparql_get_with_post.query().convert().decode("utf-8")

        # return to default behaviour
        self._starvers_engine.sparql_get_with_post.setReturnFormat('json') 
        self._starvers_engine.sparql_get_with_post.clearCustomHttpHeader('Accept')

        # Split file into lines, put them into a list, and then turn into a set
        self.LOG.info(f"Repository name: {self.repository_name}: Calculate delta - Insertions & Deletions")
        versioned_triples = set(to_list(versioned_n3_str))
        latest_triples = set(to_list(latest_n3_str))

        # Calculate differences
        insertions = latest_triples - versioned_triples
        deletions = versioned_triples - latest_triples

        # Convert sets to lists        
        insertions_n3 = list(insertions)
        deletions_n3 = list(deletions)

        return insertions_n3, deletions_n3


    def clean_up(self):
        pass


class SparqlDeltaQueryService(DeltaCalculationService):
    def __init__(self, starvers_engine: TripleStoreEngine, tracking_task: TrackingTaskDto, repository_name: str) -> None:
        super().__init__(starvers_engine, tracking_task, repository_name)


    def prepare(self, version_timestamp, local_file: bool = False):
        self.set_version_timestamp(version_timestamp)
        self.set_paths(version_timestamp)
        
        # Download data
        self.download_data(local_file)

        # Skolemize and normalize (remove special characters)
        self.preprocess()
        
        # Ingest data
        self.load_rdf_data(self.tracking_task.name_temp())


    def calculate_delta(self):
        self._starvers_engine.sparql_get_with_post.setReturnFormat('n3')
        self._starvers_engine.sparql_get_with_post.addCustomHttpHeader('Accept', 'application/n-triples')

        # Insertions
        self.LOG.info(f"Repository name: {self.repository_name}: Calculate Delta with SPARQL - Insertions")
        self._starvers_engine.sparql_get_with_post.setQuery(get_delta_query_insertions_template(self.version_timestamp, self.tracking_task.name_temp())) #no versioning necessary
        insertions_n3_str = self._starvers_engine.sparql_get_with_post.query().convert().decode("utf-8")
        self.LOG.info(f"Repository name: {self.repository_name}: Convert n3 string of insertions to list.")
        insertions_n3 = to_list(insertions_n3_str)

        # Deletions
        self.LOG.info(f"Repository name: {self.repository_name}: Calculate Delta with SPARQL - Deletions")
        self._starvers_engine.sparql_get_with_post.setQuery(get_delta_query_deletions_template(self.version_timestamp, self.tracking_task.name_temp())) #no versioning necessary
        deletions_n3_str = self._starvers_engine.sparql_get_with_post.query().convert().decode("utf-8")
        self.LOG.info(f"Repository name: {self.repository_name}: Convert n3 string of deletions to list.")
        deletions_n3 = to_list(deletions_n3_str)

        # Return to default behavior
        self._starvers_engine.sparql_get_with_post.setReturnFormat('json') 
        self._starvers_engine.sparql_get_with_post.clearCustomHttpHeader('Accept')

        return insertions_n3, deletions_n3


    def clean_up(self):
        self.LOG.info(f"Repository name: {self.repository_name}: Clean up - remove temp graph {self.tracking_task.name_temp()}")
        self._starvers_engine.sparql_post.setQuery(get_drop_graph_template(self.tracking_task.name_temp()))
        self._starvers_engine.sparql_post.query()