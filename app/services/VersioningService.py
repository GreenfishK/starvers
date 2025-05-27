from abc import ABC, abstractmethod
import os
import shutil
import time
from datetime import datetime
import logging
from starvers.starvers import TripleStoreEngine
from rdflib import Literal
from rdflib.namespace import XSD

from app.AppConfig import Settings
from app.LoggingConfig import get_tracking_logger
from app.enums.DeltaTypeEnum import DeltaType
from app.models.DeltaEventModel import DeltaEvent
from app.models.TrackingTaskModel import TrackingTaskDto
from app.services.DeltaCalculationService import IterativeDeltaQueryService, SparqlDeltaQueryService
from app.utils.HelperService import convert_df_to_n3, convert_df_to_triples, get_timestamp, convert_select_query_to_df
from app.utils.graphdb.GraphDatabaseUtils import get_query_all_template, get_count_triples_template

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
    def __init__(self, tracking_task: TrackingTaskDto) -> None:
        self.LOG = get_tracking_logger(tracking_task.name)
        self.tracking_task = tracking_task
        self.local_file = False

        self.__graph_db_get_endpoint = Settings().graph_db_url_get_endpoint.replace('{:repo_name}', tracking_task.name)
        self.__graph_db_post_endpoint = Settings().graph_db_url_post_endpoint.replace('{:repo_name}', tracking_task.name)
        self.__starvers_engine = TripleStoreEngine(self.__graph_db_get_endpoint, self.__graph_db_post_endpoint, skip_connection_test=True)
        
        match tracking_task.delta_type:
            case DeltaType.ITERATIVE:
                self.__delta_query_service = IterativeDeltaQueryService(self.__starvers_engine, tracking_task)
            case DeltaType.SPARQL:
                self.__delta_query_service = SparqlDeltaQueryService(self.__starvers_engine, tracking_task)


    def _cnt_triples(self, version_timestamp):
        self.LOG.info(f"Counting triples in the snapshot with timestamp '{version_timestamp}'")
        cnt_triples_query = get_count_triples_template(version_timestamp)
        self.__starvers_engine.sparql_get_with_post.setQuery(cnt_triples_query)
        cnt_triples = self.__starvers_engine.sparql_get_with_post.query()
        cnt_triples_df = convert_select_query_to_df(cnt_triples)
        cnt_triples = cnt_triples_df["cnt_triples"].values[0].split('^^')[0].strip('"') 
        self.LOG.info(f"Number of triples: {cnt_triples}")

        return cnt_triples

    def run_initial_versioning(self, version_timestamp):
        self.LOG.info(f"Start initial versioning task [{version_timestamp}]")
        self.__delta_query_service.set_version_timestamp(version_timestamp)
        
        if self.local_file:
            self.__delta_query_service.load_rdf_data(local_file=True)
        else:
            self.__delta_query_service.load_rdf_data()
        self.__starvers_engine.version_all_triples(initial_timestamp=version_timestamp)

        os.makedirs(os.path.dirname(f"./evaluation/{self.tracking_task.name}/"), exist_ok=True)

        with open(f"./evaluation/{self.tracking_task.name}/{self.tracking_task.name}_timings.csv", "w") as timing_file:
            timing_file.write("timestamp, insertions, deletions, time_prepare_ns, time_delta_ns, time_versioning_ns, time_overall_ns, cnt_triples\n")
        
        # Persist Timings
        path = f"./evaluation/{self.tracking_task.name}/"

        cnt_triples = self._cnt_triples(version_timestamp)

        with open(f"{path}{self.tracking_task.name}_timings.csv", "a+") as timing_file:
            timing_file.write(f"{get_timestamp(version_timestamp)}, {cnt_triples}, 0, 0, 0, 0, 0, {cnt_triples}")
            timing_file.write('\n')
        self.LOG.info(f"Finished initial versioning task [{version_timestamp}]")

    def query(self, query: str, timestamp: datetime = None, query_as_timestamped: bool = True):
        if timestamp is not None and query_as_timestamped:
            self.LOG.info(f"Execute timestamped query with timestamp={timestamp}")
        else:
            self.LOG.info("Execute query without timestamp")
        return self.__starvers_engine.query(query, timestamp, query_as_timestamped)
    

    def get_latest_version(self):
        query = get_query_all_template()
        return convert_df_to_triples(self.query(query))
    

    def run_versioning(self, version_timestamp) -> DeltaEvent:
        self.LOG.info(f"Start versioning task [{version_timestamp}]")
        try:
            timing_overall = time.time_ns()

            timing_prepare = time.time_ns()
            self.__delta_query_service.set_version_timestamp(version_timestamp)
            if self.local_file:
                self.__delta_query_service.prepare(local_file=True)
            else:
                self.__delta_query_service.prepare()
            timing_prepare = time.time_ns() - timing_prepare

            timing_delta = time.time_ns()
            insertions, deletions = self.__delta_query_service.calculate_delta()  
            timing_delta = time.time_ns() - timing_delta

            self.LOG.info(f"Found {len(insertions.index)} insertions and {len(deletions.index)} deletions")
            
            timing_versioning = time.time_ns()
            insertions_n3 = convert_df_to_n3(insertions)
            deletions_n3 = convert_df_to_n3(deletions)
            self.__starvers_engine.insert(insertions_n3, timestamp=version_timestamp)
            self.__starvers_engine.outdate(deletions_n3, timestamp=version_timestamp)
            timing_versioning = time.time_ns() - timing_versioning

            self.__delta_query_service.clean_up()
            timing_overall = time.time_ns() - timing_overall

            cnt_triples = self._cnt_triples(version_timestamp)

            path = f"./evaluation/{self.tracking_task.name}/"
            # Persist Timings
            with open(f"{path}{self.tracking_task.name}_timings.csv", "a+") as timing_file:
                timing_file.write(f"{get_timestamp(version_timestamp)}, {len(insertions_n3)}, {len(deletions_n3)}, {timing_prepare}, {timing_delta}, {timing_versioning}, {timing_overall}, {cnt_triples}")
                timing_file.write('\n')
            
            if len(insertions_n3) > 0 or len(deletions_n3) > 0:
                # Persist Inserts, Deletions
                with open(f"{path}{get_timestamp(version_timestamp)}/{self.tracking_task.name}_{get_timestamp(version_timestamp)}.delta", "a+") as dump_file:
                    dump_file.writelines(map(lambda x: "- " + x + '\n', deletions_n3))
                    dump_file.writelines(map(lambda x: "+ " + x + '\n', insertions_n3))
            shutil.make_archive(f"{path}{get_timestamp(version_timestamp)}", "zip", f"{path}{get_timestamp(version_timestamp)}")
            shutil.rmtree(f"{path}{get_timestamp(version_timestamp)}")

            if len(insertions_n3) > 0 or len(deletions_n3) > 0:
                self.LOG.info(f"Tracked {len(insertions_n3)} insertions and {len(deletions_n3)} deletions")
                return DeltaEvent(
                    id=self.tracking_task.id,
                    repository_name=self.tracking_task.name,
                    delta_type=self.tracking_task.delta_type,
                    totalInsertions=len(insertions_n3),
                    totalDeletions=len(deletions_n3),
                    insertions=insertions_n3,
                    deletions=deletions_n3,
                    versioning_duration_ms=timing_overall,
                    timestamp=version_timestamp
                )
            
            self.LOG.info("No changes tracked")
            self.LOG.info(f"Finished versioning task [{version_timestamp}]")
            return None
        except Exception as e:
            self.LOG.error(f"Versioning task failed with error {e}")
            self.LOG.info("Versioning task will be rescheduled...")
            self.__delta_query_service.clean_up()
            return None