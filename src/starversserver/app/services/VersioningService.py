from abc import ABC, abstractmethod
import os
import shutil
import time
from datetime import datetime
from typing import Optional
from app.utils.starvers.starvers import TripleStoreEngine
import pandas as pd

from app.AppConfig import Settings
from app.LoggingConfig import get_logger
from app.models.DeltaEventModel import DeltaEvent
from app.models.TrackingTaskModel import TrackingTaskDto
from app.services.DeltaCalculationService import IterativeDeltaQueryService, SparqlDeltaQueryService
from app.utils.HelperService import get_timestamp, obtain_nt, normalize_and_skolemize
from app.persistance.graphdb.GraphDatabaseUtils import get_count_triples_template, import_serverfile, poll_import_status

class VersioningService(ABC):
    @abstractmethod
    def run_initial_versioning(self, version_timestamp: datetime):   
        pass

    @abstractmethod
    def query(self, query: str, timestamp: Optional[datetime] = None, query_as_timestamped: bool = True) -> pd.DataFrame:    
        pass

    @abstractmethod
    def run_versioning(self, version_timestamp: datetime) -> Optional[DeltaEvent]:
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
        
        self.__delta_iterative = IterativeDeltaQueryService(self.__starvers_engine, tracking_task, repository_name)
        self.__delta_sparql = SparqlDeltaQueryService(self.__starvers_engine, tracking_task, repository_name)

        self._version_timestamp: Optional[datetime] = None


    def _set_paths(self):
        if self._version_timestamp is None:
            raise ValueError("version_timestamp must not be None")
        version_timestamp_str = get_timestamp(self._version_timestamp)

        self.base_path = f"/data/evaluation/{self.tracking_task.name}/{version_timestamp_str}"
        self.snapshot_path = f"{self.base_path}/{self.tracking_task.name}_{version_timestamp_str}.raw.nt"
        self.processed_path = f"{self.base_path}/{self.tracking_task.name}_{version_timestamp_str}.nt"
        self.import_path = f"/graphdb-import/{self.tracking_task.name}.nt"
        self.dumps_path = f"/data/evaluation/{self.tracking_task.name}"


    def _cnt_triples(self, version_timestamp: datetime) -> int:
        self.LOG.info(f"Repository name: {self.repository_name}: Counting triples in the snapshot with timestamp '{version_timestamp}'")
        
        cnt_triples_query = get_count_triples_template(version_timestamp)
        cnt_triples_df = self.__starvers_engine.query(cnt_triples_query, yn_timestamp_query=False)
        cnt_triples = cnt_triples_df["cnt_triples"].iloc[0]
        if isinstance(cnt_triples, str):
            cnt_triples = int(cnt_triples.split('^^')[0].strip('"'))
            
        self.LOG.info(f"Repository name: {self.repository_name}: Number of triples: {cnt_triples}")

        return cnt_triples


    def _download_data(self, local_file: bool = False):
        if self._version_timestamp is None:
            raise ValueError("version_timestamp must not be None")
        os.makedirs(self.base_path, exist_ok=True)

        if not local_file:
            for attempt in range(2):
                self.LOG.info(f"Repository name: {self.repository_name}: Download rdf data dump into {self.snapshot_path} ({attempt+1}. attempt)")
                try:
                    obtain_nt(self.tracking_task.rdf_dataset_url, self.snapshot_path)
                    break
                except Exception as e:
                    if attempt == 1:
                        self.LOG.info(f"Repository name: {self.repository_name}: Download failed after 2 attempts.")
                        raise
                    self.LOG.warning(f"Repository name: {self.repository_name}: Retrying after error: %s", e)
        else:
            self.LOG.info(f"Repository name: {self.repository_name}: Local rdf data with path {self.tracking_task.name}_{get_timestamp(self._version_timestamp)}.raw.nt provided. Copy it into {self.snapshot_path}")
            shutil.copy2(f"{self.dumps_path}/{self.tracking_task.name}_{get_timestamp(self._version_timestamp)}.raw.nt", self.snapshot_path)


    def _preprocess(self):
        if self._version_timestamp is None:
            raise ValueError("version_timestamp must not be None")
        self.LOG.info(f"Repository name: {self.repository_name}: Normalize and skolemize {self.tracking_task.name}_{get_timestamp(self._version_timestamp)}.raw.nt")
        normalize_and_skolemize(self.snapshot_path, self.processed_path)


    def _load_rdf_data(self, graph_name: str = ""):
        if self._version_timestamp is None:
            raise ValueError("version_timestamp must not be None")
        os.makedirs(self.base_path, exist_ok=True)

        # Compy into import directory
        self.LOG.info(f"Repository name: {self.repository_name}: Copy {self.tracking_task.name}_{get_timestamp(self._version_timestamp)}.nt into import directory: {self.import_path}")
        shutil.copy2(self.processed_path, self.import_path)

        # Import into GraphDB
        import_serverfile(f"{self.tracking_task.name}.nt", self.tracking_task.name, graph_name)
        poll_import_status(f"{self.tracking_task.name}.nt", self.tracking_task.name)


    def run_initial_versioning(self, version_timestamp: datetime):
        self.LOG.info(f"Repository name: {self.repository_name}: Start initial versioning task [{version_timestamp}]")        
        self._version_timestamp = version_timestamp
        self._set_paths()

        # Download data
        # Equal for both delta types
        self._download_data(self.local_file)

        # Preprocess
        # Equal for both delta types
        self._preprocess()
        
        # Ingest data
        # Equal for both delta types during initial versioning
        self._load_rdf_data(self.tracking_task.name_temp())
        
        # Version data
        # Equal for both delta types during initial versioning
        self.LOG.info(f"Repository name: {self.repository_name}: Initialize triples with [{version_timestamp}] and artificial end timestamp.")
        self.__starvers_engine.version_all_triples(initial_timestamp=version_timestamp)

        # Persist Timings and ingest statistics
        tmp_work_dir = f"/data/evaluation/{self.tracking_task.name}"
        os.makedirs(os.path.dirname(tmp_work_dir), exist_ok=True)
        header="timestamp, insertions, deletions, time_prepare_ns, time_delta_ns, time_versioning_ns, time_overall_ns, cnt_triples\n"
        with open(f"{tmp_work_dir}/{self.tracking_task.name}_timings.csv", "w") as timing_file:
            timing_file.write(header)
        with open(f"{tmp_work_dir}/{self.tracking_task.name}_timings_sparql.csv", "w") as timing_file:
            timing_file.write(header)
        
        cnt_triples = self._cnt_triples(version_timestamp)
        version_timestamp_str = get_timestamp(version_timestamp)
        init_row=f"{version_timestamp_str}, 0, 0, 0, 0, 0, 0, {cnt_triples}\n"
        with open(f"{tmp_work_dir}/{self.tracking_task.name}_timings.csv", "a+") as timing_file:
            timing_file.write(init_row)
        with open(f"{tmp_work_dir}/{self.tracking_task.name}_timings_sparql.csv", "a+") as timing_file:
            timing_file.write(init_row)
        self.LOG.info(f"Repository name: {self.repository_name}: Finished initial versioning task [{version_timestamp}]")

        # Zip and remove tmp directory that gets created in load_rdf_data
        tmp_dir = f"{tmp_work_dir}/{version_timestamp_str}"
        
        # zip tmp_dir and save the zip file in the same directory as tmp_dir
        shutil.make_archive(tmp_dir, 'zip', tmp_dir)
        self.LOG.info(f"Repository name: {self.repository_name}: Zipped initial snapshot: {tmp_dir}.zip")
        
        # Remove tmp_dir
        if os.path.exists(tmp_dir):
            shutil.rmtree(tmp_dir)
            self.LOG.info(f"Repository name: {self.repository_name}: Removed temporary directory: {tmp_dir}")

    def query(self, query: str, timestamp: Optional[datetime] = None, query_as_timestamped: bool = True) -> pd.DataFrame:
        if timestamp is not None and query_as_timestamped:
            self.LOG.info(f"Repository name: {self.repository_name}: Execute timestamped query with timestamp={timestamp}")
        else:
            self.LOG.info("Repository name: {self.repository_name}: Execute query without timestamp")
        return self.__starvers_engine.query(query, timestamp, query_as_timestamped)
    

    def run_versioning(self, version_timestamp: datetime) -> Optional[DeltaEvent]:
        self.LOG.info(f"Repository name: {self.repository_name}: Version timestamp: {version_timestamp}")
        self._version_timestamp = version_timestamp
        self._set_paths()

        try:
            timing_overall = 0

            timing_prepare_iterative = time.time_ns()
            # Download from remote repository
            self._download_data(self.local_file)

            # Skolemize and normalize (remove control characters)
            self._preprocess()
            timing_prepare_iterative = time.time_ns() - timing_prepare_iterative

            # Additional load operation for SPARQL method
            timing_prepare_sparql = time.time_ns()
            self._load_rdf_data(self.tracking_task.name_temp())
            timing_prepare_sparql = time.time_ns() - timing_prepare_sparql + timing_prepare_iterative

            # Delta calculation for SPARQL method
            timing_delta_sparql = time.time_ns()
            insertions_n3_sparql, deletions_n3_sparql = self.__delta_sparql.calculate_delta(version_timestamp)  
            timing_delta_sparql = time.time_ns() - timing_delta_sparql
            self.LOG.info(f"Repository name: {self.repository_name}: Found {len(insertions_n3_sparql)} insertions and {len(deletions_n3_sparql)} deletions")

            # Drop temporary graph from triple store
            timing_cleanup_sparql = time.time_ns()
            self.__delta_sparql.clean_up()
            timing_cleanup_sparql = time.time_ns() - timing_cleanup_sparql

            # Delta calculation for ITERATIVE method
            timing_delta_iterative = time.time_ns()
            insertions_n3_iterative, deletions_n3_iterative = self.__delta_iterative.calculate_delta(version_timestamp, self.processed_path)  
            timing_delta_iterative = time.time_ns() - timing_delta_iterative
            self.LOG.info(f"Repository name: {self.repository_name}: Found {len(insertions_n3_iterative)} insertions and {len(deletions_n3_iterative)} deletions")
            
            # check that set cardinalities are the same
            if not (len(insertions_n3_iterative) == len(insertions_n3_sparql) and len(deletions_n3_iterative) == len(deletions_n3_sparql)):
                self.LOG.error(
                    f"Repository name: {self.repository_name}: Mismatch in delta calculation! "
                    f"The SPARQL method has {len(insertions_n3_sparql)} insertions while the ITERATIVE method has {len(insertions_n3_iterative)}. "
                    f"The SPARQL method has {len(deletions_n3_sparql)} deletions while the ITERATIVE method has {len(deletions_n3_iterative)}."
                )

            insertions_n3 = insertions_n3_iterative
            deletions_n3 = deletions_n3_iterative

            timing_versioning = time.time_ns()
            self.__starvers_engine.insert(insertions_n3, timestamp=version_timestamp)
            self.__starvers_engine.outdate(deletions_n3, timestamp=version_timestamp)
            timing_versioning = time.time_ns() - timing_versioning

            timing_overall_sparql = timing_prepare_sparql + timing_delta_sparql + timing_cleanup_sparql + timing_versioning
            timing_overall_iterative = timing_prepare_iterative + timing_delta_iterative + timing_versioning
            
            # Persist Timings
            self.LOG.info(f"Repository name: {self.repository_name}: Persisting timings and statistics")
            tmp_work_dir = f"/data/evaluation/{self.tracking_task.name}/"
            cnt_triples = self._cnt_triples(version_timestamp)
            
            version_timestamp_str = get_timestamp(version_timestamp)
            # SPARQL method timings
            with open(f"{tmp_work_dir}{self.tracking_task.name}_timings_sparql.csv", "a+") as timing_file:
                timing_file.write(f"{version_timestamp_str}, {len(insertions_n3_sparql)}, {len(deletions_n3_sparql)}, {timing_prepare_sparql}, {timing_delta_sparql}, {timing_versioning}, {timing_overall_sparql}, {cnt_triples}\n")

            # Iterative method timings
            with open(f"{tmp_work_dir}{self.tracking_task.name}_timings.csv", "a+") as timing_file:
                timing_file.write(f"{version_timestamp_str}, {len(insertions_n3_iterative)}, {len(deletions_n3_iterative)}, {timing_prepare_iterative}, {timing_delta_iterative}, {timing_versioning}, {timing_overall_iterative}, {cnt_triples}\n")
            
            # Persist deltas, if there are any
            if len(insertions_n3_sparql) > 0 or len(deletions_n3_sparql) > 0:
                # Persist Inserts, Deletions
                with open(f"{tmp_work_dir}{version_timestamp_str}/{self.tracking_task.name}_{version_timestamp_str}_sparql.delta", "a+") as dump_file:
                    dump_file.writelines(map(lambda x: "- " + x + '\n', deletions_n3))
                    dump_file.writelines(map(lambda x: "+ " + x + '\n', insertions_n3))

            if len(insertions_n3_iterative) > 0 or len(deletions_n3_iterative) > 0:
                # Persist Inserts, Deletions
                with open(f"{tmp_work_dir}{version_timestamp_str}/{self.tracking_task.name}_{version_timestamp_str}_iterative.delta", "a+") as dump_file:
                    dump_file.writelines(map(lambda x: "- " + x + '\n', deletions_n3))
                    dump_file.writelines(map(lambda x: "+ " + x + '\n', insertions_n3))
            
            # zip files
            self.LOG.info(f"Repository name: {self.repository_name}: Creating zip {tmp_work_dir}{version_timestamp_str}")
            shutil.make_archive(f"{tmp_work_dir}{version_timestamp_str}", "zip", f"{tmp_work_dir}{version_timestamp_str}")
            shutil.rmtree(f"{tmp_work_dir}{version_timestamp_str}")

            if len(insertions_n3) > 0 or len(deletions_n3) > 0:
                self.LOG.info(f"Repository name: {self.repository_name}: Tracked {len(insertions_n3)} insertions and {len(deletions_n3)} deletions")
            else:
                self.LOG.info(f"Repository name: {self.repository_name}: No changes tracked")
                self.LOG.info(f"Repository name: {self.repository_name}: Finished versioning task [{version_timestamp}]")
                
            return DeltaEvent(
                id=self.tracking_task.id,
                repository_name=self.tracking_task.name,
                totalInsertions=len(insertions_n3),
                totalDeletions=len(deletions_n3),
                insertions=insertions_n3,
                deletions=deletions_n3,
                versioning_duration_ms=timing_overall,
                timestamp=version_timestamp
                )
        except Exception as e:
            self.LOG.error(f"Repository name: {self.repository_name}: Versioning task failed with error {e}")
            self.LOG.info(f"Repository name: {self.repository_name}: Versioning task will be rescheduled...")
            self.__delta_sparql.clean_up()
            
            return None