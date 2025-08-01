from abc import ABC, abstractmethod
import os
import shutil
import time
from datetime import datetime
from starvers.starvers import TripleStoreEngine

from app.AppConfig import Settings
from app.LoggingConfig import get_logger
from app.enums.DeltaTypeEnum import DeltaType
from app.models.DeltaEventModel import DeltaEvent
from app.models.TrackingTaskModel import TrackingTaskDto
from app.services.DeltaCalculationService import IterativeDeltaQueryService, SparqlDeltaQueryService
from app.utils.HelperService import convert_df_to_triples, get_timestamp
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
        # TODO: consider removing or implementing propertly and using
        pass

    @abstractmethod
    def run_versioning():
        pass

class StarVersService(VersioningService):
    def __init__(self, tracking_task: TrackingTaskDto, repository_name: str) -> None:
        self.LOG = get_logger(__name__, f"tracking_{tracking_task.name}.log")
        self.tracking_task = tracking_task
        self.local_file = False
        self.repository_name = repository_name

        self.__graph_db_get_endpoint = Settings().graph_db_url_get_endpoint.replace('{:repo_name}', tracking_task.name)
        self.__graph_db_post_endpoint = Settings().graph_db_url_post_endpoint.replace('{:repo_name}', tracking_task.name)
        self.__starvers_engine = TripleStoreEngine(self.__graph_db_get_endpoint, self.__graph_db_post_endpoint, skip_connection_test=True)
        
        match tracking_task.delta_type:
            case DeltaType.ITERATIVE:
                self.__delta_query_service = IterativeDeltaQueryService(self.__starvers_engine, tracking_task, repository_name)
            case DeltaType.SPARQL:
                self.__delta_query_service = SparqlDeltaQueryService(self.__starvers_engine, tracking_task, repository_name)


    def _cnt_triples(self, version_timestamp):
        self.LOG.info(f"Repository name: {self.repository_name}: Counting triples in the snapshot with timestamp '{version_timestamp}'")
        
        cnt_triples_query = get_count_triples_template(version_timestamp)
        cnt_triples_df = self.__starvers_engine.query(cnt_triples_query, yn_timestamp_query=False)
        cnt_triples = cnt_triples_df["cnt_triples"].values[0]
        if isinstance(cnt_triples, str):
            cnt_triples = cnt_triples.split('^^')[0].strip('"')
            
        self.LOG.info(f"Repository name: {self.repository_name}: Number of triples: {cnt_triples}")

        return cnt_triples

    def run_initial_versioning(self, version_timestamp):
        self.LOG.info(f"Repository name: {self.repository_name}: Start initial versioning task [{version_timestamp}]")
        self.__delta_query_service.set_version_timestamp(version_timestamp)
        self.__delta_query_service.set_paths(version_timestamp)
        
        # Download data
        self.__delta_query_service.download_data(self.local_file)

        # Skolemize
        self.__delta_query_service.skolemize_blank_nodes()
        
        # Ingest data
        self.__delta_query_service.load_rdf_data(self.tracking_task.name_temp())
        
        # Version data
        self.LOG.info(f"Repository name: {self.repository_name}: Initialize triples with [{version_timestamp}] and artificial end timestamp.")
        self.__starvers_engine.version_all_triples(initial_timestamp=version_timestamp)

        # Persist Timings and ingest statistics
        path = f"./evaluation/{self.tracking_task.name}"
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(f"{path}/{self.tracking_task.name}_timings.csv", "w") as timing_file:
            timing_file.write("timestamp, insertions, deletions, time_prepare_ns, time_delta_ns, time_versioning_ns, time_overall_ns, cnt_triples\n")
        
        cnt_triples = self._cnt_triples(version_timestamp)
        with open(f"{path}/{self.tracking_task.name}_timings.csv", "a+") as timing_file:
            timing_file.write(f"{get_timestamp(version_timestamp)}, {cnt_triples}, 0, 0, 0, 0, 0, {cnt_triples}")
            timing_file.write('\n')
        self.LOG.info(f"Repository name: {self.repository_name}: Finished initial versioning task [{version_timestamp}]")

        # Zip and remove tmp directory that gets created in load_rdf_data
        tmp_dir = f"{path}/{get_timestamp(version_timestamp)}"
        
        # zip tmp_dir and save the zip file in the same directory as tmp_dir
        shutil.make_archive(tmp_dir, 'zip', tmp_dir)
        self.LOG.info(f"Repository name: {self.repository_name}: Zipped initial snapshot: {tmp_dir}.zip")
        
        # Remove tmp_dir
        if os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir)
            self.LOG.info(f"Repository name: {self.repository_name}: Removed temporary directory: {tmp_dir}")


    def query(self, query: str, timestamp: datetime = None, query_as_timestamped: bool = True):
        if timestamp is not None and query_as_timestamped:
            self.LOG.info(f"Repository name: {self.repository_name}: Execute timestamped query with timestamp={timestamp}")
        else:
            self.LOG.info("Repository name: {self.repository_name}: Execute query without timestamp")
        return self.__starvers_engine.query(query, timestamp, query_as_timestamped)
    

    def get_latest_version(self):
        # TODO: consider removing this method or implementing propertly and using
        query = get_query_all_template()
        return convert_df_to_triples(self.query(query))
    

    def run_versioning(self, version_timestamp) -> DeltaEvent:
        self.LOG.info(f"Repository name: {self.repository_name}: Version timestamp: {version_timestamp}")
        try:
            timing_overall = time.time_ns()

            timing_prepare = time.time_ns()
            self.__delta_query_service.prepare(version_timestamp, self.local_file)
            timing_prepare = time.time_ns() - timing_prepare

            timing_delta = time.time_ns()
            insertions_n3, deletions_n3 = self.__delta_query_service.calculate_delta()  
            timing_delta = time.time_ns() - timing_delta
            self.LOG.info(f"Repository name: {self.repository_name}: Found {len(insertions_n3)} insertions and {len(deletions_n3)} deletions")
            
            timing_versioning = time.time_ns()
            self.__starvers_engine.insert(insertions_n3, timestamp=version_timestamp)
            self.__starvers_engine.outdate(deletions_n3, timestamp=version_timestamp)
            timing_versioning = time.time_ns() - timing_versioning

            self.__delta_query_service.clean_up()
            timing_overall = time.time_ns() - timing_overall
            
            # Persist Timings
            path = f"./evaluation/{self.tracking_task.name}/"
            cnt_triples = self._cnt_triples(version_timestamp)
            with open(f"{path}{self.tracking_task.name}_timings.csv", "a+") as timing_file:
                timing_file.write(f"{get_timestamp(version_timestamp)}, {len(insertions_n3)}, {len(deletions_n3)}, {timing_prepare}, {timing_delta}, {timing_versioning}, {timing_overall}, {cnt_triples}")
                timing_file.write('\n')
            
            # Persist deltas, if there are any
            if len(insertions_n3) > 0 or len(deletions_n3) > 0:
                # Persist Inserts, Deletions
                with open(f"{path}{get_timestamp(version_timestamp)}/{self.tracking_task.name}_{get_timestamp(version_timestamp)}.delta", "a+") as dump_file:
                    dump_file.writelines(map(lambda x: "- " + x + '\n', deletions_n3))
                    dump_file.writelines(map(lambda x: "+ " + x + '\n', insertions_n3))
            shutil.make_archive(f"{path}{get_timestamp(version_timestamp)}", "zip", f"{path}{get_timestamp(version_timestamp)}")
            shutil.rmtree(f"{path}{get_timestamp(version_timestamp)}")

            if len(insertions_n3) > 0 or len(deletions_n3) > 0:
                self.LOG.info(f"Repository name: {self.repository_name}: Tracked {len(insertions_n3)} insertions and {len(deletions_n3)} deletions")
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
            
            self.LOG.info(f"Repository name: {self.repository_name}: No changes tracked")
            self.LOG.info(f"Repository name: {self.repository_name}: Finished versioning task [{version_timestamp}]")
            
            return None
        except Exception as e:
            self.LOG.error(f"Repository name: {self.repository_name}: Versioning task failed with error {e}")
            self.LOG.info(f"Repository name: {self.repository_name}: Versioning task will be rescheduled...")
            self.__delta_query_service.clean_up()
            
            return None