from abc import ABC, abstractmethod
import os
import time
from uuid import UUID
from datetime import datetime
import logging
from starvers.starvers import TripleStoreEngine

from app.AppConfig import Settings
from app.enums.DeltaTypeEnum import DeltaType
from app.models.DeltaEventModel import DeltaEvent
from app.services.DeltaCalculationService import IterativeDeltaQueryService, SparqlDeltaQueryService
from app.utils.HelperService import convert_df_to_n3, convert_df_to_triples
from app.utils.graphdb.GraphDatabaseUtils import get_query_all_template

LOG = logging.getLogger(__name__)

class VersioningService(ABC):
    @abstractmethod
    def run_initial_versioning():
        pass

    @abstractmethod
    def query():
        pass
    
    @abstractmethod
    def get_latest_version():
        pass

    @abstractmethod
    def run_versioning():
        pass

class StarVersService(VersioningService):
    def __init__(self, repository_name: str, dataset_id: UUID, rdf_dataset_url: str, delta_type: DeltaType) -> None:
        self.repository_name = repository_name
        self.dataset_id = dataset_id
        self.delta_type = delta_type

        self.__graph_db_get_endpoint = Settings().graph_db_url_get_endpoint.replace('{:repo_name}', self.repository_name)
        self.__graph_db_post_endpoint = Settings().graph_db_url_post_endpoint.replace('{:repo_name}', self.repository_name)
        self.__starvers_engine = TripleStoreEngine(self.__graph_db_get_endpoint, self.__graph_db_post_endpoint, skip_connection_test=True)
        
        match delta_type:
            case DeltaType.ITERATIVE:
                self.__delta_query_service = IterativeDeltaQueryService(self.__starvers_engine, rdf_dataset_url, repository_name + "_tmp_versioning")
            case DeltaType.SPARQL:
                self.__delta_query_service = SparqlDeltaQueryService(self.__starvers_engine, rdf_dataset_url, repository_name + "_tmp_versioning")


    def run_initial_versioning(self, version_timestamp):
        self.__delta_query_service.load_rdf_data()
        self.__starvers_engine.version_all_triples(initial_timestamp=version_timestamp)


    def query(self, query: str, timestamp: datetime = None, query_as_timestamped: bool = True):
        LOG.info(f"Query at timestamp={timestamp} from repository {self.repository_name} with uuid={self.dataset_id}")
        return self.__starvers_engine.query(query, timestamp, query_as_timestamped)
    

    def get_latest_version(self):
        query = get_query_all_template()
        return convert_df_to_triples(self.query(query))
    

    def run_versioning(self, version_timestamp) -> DeltaEvent:
        try:
            timing_overall = time.time_ns()

            timing_prepare = time.time_ns()
            self.__delta_query_service.prepare()
            timing_prepare = time.time_ns() - timing_prepare

            timing_delta = time.time_ns()
            insertions, deletions = self.__delta_query_service.calculate_delta(version_timestamp)  
            timing_delta = time.time_ns() - timing_delta

            LOG.info(f"Found {len(insertions.index)} insertions and {len(deletions.index)} deletions for dataset with uuid={self.dataset_id}")
            
            timing_versioning = time.time_ns()
            insertions_n3 = convert_df_to_n3(insertions)
            deletions_n3 = convert_df_to_n3(deletions)
            self.__starvers_engine.insert(insertions_n3, timestamp=version_timestamp)
            self.__starvers_engine.outdate(deletions_n3, timestamp=version_timestamp)
            timing_versioning = time.time_ns() - timing_versioning

            self.__delta_query_service.clean_up()
            timing_overall = time.time_ns() - timing_overall

            if (Settings().evaluation_mode):
                path = f"./evaluation/{self.repository_name}/"
                os.makedirs(os.path.dirname(path), exist_ok=True)

                # Persist Timings
                timestamp = int(datetime.timestamp(datetime.now()))
                with open(f"{path}{self.repository_name}_timings.csv", "a+") as timing_file:
                    timing_file.write(f"{timestamp}, {len(insertions_n3)}, {len(deletions_n3)}, {timing_prepare}, {timing_delta}, {timing_versioning}, {timing_versioning}, {timing_overall}")
                    timing_file.write('\n')
                
                if len(insertions_n3) > 0 or len(deletions_n3) > 0:
                    # Persist Inserts, Deletions
                    with open(f"{path}{self.repository_name}_{timestamp}.delta", "a+") as dump_file:
                        dump_file.writelines(map(lambda x: "- " + x + '\n', deletions_n3))
                        dump_file.writelines(map(lambda x: "+ " + x + '\n', insertions_n3))
            
            if len(insertions_n3) > 0 or len(deletions_n3) > 0:
                return DeltaEvent(
                    id=self.dataset_id,
                    repository_name=self.repository_name,
                    delta_type=self.delta_type,
                    totalInsertions=len(insertions_n3),
                    totalDeletions=len(deletions_n3),
                    insertions=insertions_n3,
                    deletions=deletions_n3,
                    versioning_duration_ms=timing_overall,
                    timestamp=version_timestamp
                )
            
            return None
        except:
            self.__delta_query_service.clean_up()
            return None