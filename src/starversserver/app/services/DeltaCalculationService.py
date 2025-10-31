from abc import ABC, abstractmethod
from typing import List, Tuple
from app.utils.starvers.starvers import TripleStoreEngine
import datetime

from app.LoggingConfig import get_logger
from app.models.TrackingTaskModel import TrackingTaskDto
from app.utils.HelperService import to_list
from app.persistance.graphdb.GraphDatabaseUtils import get_construct_all_versioned_template, get_delta_query_deletions_template, get_delta_query_insertions_template, get_drop_graph_template

class DeltaCalculationService(ABC):
    def __init__(self, starvers_engine: TripleStoreEngine, tracking_task: TrackingTaskDto, repository_name: str):
        super().__init__()
        self.LOG = get_logger(__name__, f"tracking_{tracking_task.name}.log")
        self._starvers_engine = starvers_engine
        self.tracking_task = tracking_task
        self.repository_name = repository_name


    @abstractmethod
    def calculate_delta(self, version_timestamp: datetime.datetime, *args, **kwargs) -> Tuple[List[str],List[str]]:
        pass

    @abstractmethod
    def clean_up(self):
        pass


class IterativeDeltaQueryService(DeltaCalculationService):
    def __init__(self, starvers_engine: TripleStoreEngine, tracking_task: TrackingTaskDto, repository_name: str) -> None:
        super().__init__(starvers_engine, tracking_task, repository_name)


    def calculate_delta(self, version_timestamp: datetime.datetime, processed_path: str = "") -> Tuple[List[str],List[str]]:

        # New dump
        self.LOG.info(f"Repository name: {self.repository_name}: Load new dump into string.")
        with open(processed_path, "r", encoding="utf-8") as rdf_file:
            latest_n3_str = rdf_file.read()

        # Latest version
        self.LOG.info(f"Repository name: {self.repository_name}: Query latest version from triple store.")
        self._starvers_engine.sparql_get_with_post.setReturnFormat('n3') 
        self._starvers_engine.sparql_get_with_post.addCustomHttpHeader('Accept', 'application/n-triples')
        self._starvers_engine.sparql_get_with_post.setQuery(get_construct_all_versioned_template(version_timestamp))
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


    def calculate_delta(self, version_timestamp: datetime.datetime):
        self._starvers_engine.sparql_get_with_post.setReturnFormat('n3')
        self._starvers_engine.sparql_get_with_post.addCustomHttpHeader('Accept', 'application/n-triples')

        # Insertions
        self.LOG.info(f"Repository name: {self.repository_name}: Calculate Delta with SPARQL - Insertions")
        self._starvers_engine.sparql_get_with_post.setQuery(get_delta_query_insertions_template(version_timestamp, self.tracking_task.name_temp())) #no versioning necessary
        insertions_n3_str = self._starvers_engine.sparql_get_with_post.query().convert().decode("utf-8")
        self.LOG.info(f"Repository name: {self.repository_name}: Convert n3 string of insertions to list.")
        insertions_n3 = to_list(insertions_n3_str)

        # Deletions
        self.LOG.info(f"Repository name: {self.repository_name}: Calculate Delta with SPARQL - Deletions")
        self._starvers_engine.sparql_get_with_post.setQuery(get_delta_query_deletions_template(version_timestamp, self.tracking_task.name_temp())) #no versioning necessary
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