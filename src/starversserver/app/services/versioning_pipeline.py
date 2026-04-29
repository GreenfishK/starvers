"""
versioning_pipeline.py

VersioningPipeline manages the full lifecycle of one versioning cycle:
  download → preprocess → load into GraphDB → delta calculation → insert/outdate.

It also persists per-run timing CSVs and delta files for later analysis.

Two delta strategies run in parallel for evaluation purposes:
  - SPARQL method  (SparqlDeltaCalculator)
  - Iterative method (IterativeDeltaCalculator)
The iterative result is used as the authoritative delta applied to the triple store.
"""

import os
import shutil
import time
from datetime import datetime
import traceback
from typing import Optional

from app.utils.starvers.starvers import TripleStoreEngine
from app.AppConfig import Settings
from app.LoggingConfig import get_logger
# Models
from app.models.DeltaEventModel import DeltaEvent
from app.models.TrackingTaskModel import TrackingTaskDto
# Services
from app.services.delta_calculator import SparqlDeltaCalculator, IterativeDeltaCalculator
# Helpers
from app.utils.HelperService import get_timestamp, obtain_nt, normalize_and_skolemize
# Persistence
from app.persistance.graphdb.GraphDatabaseUtils import (
    get_count_triples_template,
    import_serverfile,
    poll_import_status,
)
# Exceptions
from app.exceptions.VersioningFailedException import VersioningFailedException


class VersioningPipeline:
    """
    Stateful pipeline for one tracked RDF dataset.
    Holds the SPARQL engine and both delta calculators between polling cycles.
    """

    def __init__(self, tracking_task: TrackingTaskDto, repository_name: str):
        self.tracking_task = tracking_task
        self.repository_name = repository_name
        self.LOG = get_logger(__name__)
        self.local_file = False # Set to True to skip downloading and use a local file instead (for evaluation)

        graph_db_get  = Settings().graph_db_url_get_endpoint.replace("{:repo_name}", tracking_task.name)
        graph_db_post = Settings().graph_db_url_post_endpoint.replace("{:repo_name}", tracking_task.name)
        self._sparql_engine = TripleStoreEngine(graph_db_get, graph_db_post, skip_connection_test=True)

        self._iterative_calculator = IterativeDeltaCalculator(self._sparql_engine, tracking_task, repository_name)
        self._sparql_calculator    = SparqlDeltaCalculator(self._sparql_engine, tracking_task, repository_name)

        # Paths are set fresh each cycle via _set_cycle_paths()
        self._version_timestamp: Optional[datetime] = None
        self.base_path       = ""
        self.snapshot_path   = ""
        self.processed_path  = ""
        self.import_path     = ""
        self.work_dir        = ""

    # ---------------------------------------------------------------------------
    # Public: initial and incremental versioning
    # ---------------------------------------------------------------------------

    def run_initial_versioning(self, version_timestamp: datetime):
        """
        First-time load: download, preprocess, ingest into GraphDB, then stamp all
        triples with the given timestamp. No delta is calculated.
        """
        try:
            self.LOG.info(f"[{self.repository_name}] Starting initial versioning at {version_timestamp}.")
            self._set_cycle_paths(version_timestamp)

            self._download_snapshot()
            self._preprocess_snapshot()
            self._load_into_graphdb(graph_name=self.tracking_task.name_temp())

            # Stamp all loaded triples with the initial timestamp
            self.LOG.info(f"[{self.repository_name}] Stamping all triples with initial timestamp.")
            self._sparql_engine.version_all_triples(initial_timestamp=version_timestamp)

            # Record a zero-delta timing row and count triples for the initial snapshot
            cnt_triples = self._count_versioned_triples(version_timestamp)
            self._write_initial_timing_files(version_timestamp, cnt_triples)

            # Archive the working directory
            self._archive_and_clean(self.base_path)
            self.LOG.info(f"[{self.repository_name}] Initial versioning complete.")
        except TimeoutError as e:
            self.LOG.error(f"[{self.repository_name}] Initial versioning failed due to timeout: {e}")
            raise e
        except Exception as e:
            self.LOG.error(f"[{self.repository_name}] Initial versioning failed: {e}")
            raise VersioningFailedException(self.repository_name, str(e))

    def run_versioning(self, version_timestamp: datetime) -> DeltaEvent:
        """
        Incremental run: download latest snapshot, compute delta against the stored
        version, apply insertions/deletions to the triple store, and save timings.
        """
        self.LOG.info(f"[{self.repository_name}] Starting incremental versioning at {version_timestamp}.")
        self._set_cycle_paths(version_timestamp)

        try:
            # --- Preparation (shared between both delta methods) ---
            t_prepare_start = time.time_ns()
            self._download_snapshot()
            self._preprocess_snapshot()
            t_prepare_iterative = time.time_ns() - t_prepare_start

            # Load into a temporary graph (needed by the SPARQL method)
            t_load_start = time.time_ns()
            self._load_into_graphdb(graph_name=self.tracking_task.name_temp())
            t_prepare_sparql = t_prepare_iterative + (time.time_ns() - t_load_start)

            # --- Delta calculation: SPARQL method ---
            t_delta_sparql_start = time.time_ns()
            insertions_sparql, deletions_sparql = self._sparql_calculator.calculate_delta(version_timestamp)
            t_delta_sparql = time.time_ns() - t_delta_sparql_start
            self.LOG.info(f"[{self.repository_name}] SPARQL delta: +{len(insertions_sparql)} / -{len(deletions_sparql)}")

            t_cleanup_start = time.time_ns()
            self._sparql_calculator.clean_up()
            t_cleanup_sparql = time.time_ns() - t_cleanup_start

            # --- Delta calculation: Iterative method ---
            t_delta_iterative_start = time.time_ns()
            insertions_iterative, deletions_iterative = self._iterative_calculator.calculate_delta(
                version_timestamp, self.processed_path
            )
            t_delta_iterative = time.time_ns() - t_delta_iterative_start
            self.LOG.info(f"[{self.repository_name}] Iterative delta: +{len(insertions_iterative)} / -{len(deletions_iterative)}")

            # Warn if the two methods disagree (for evaluation, we still continue)
            self._warn_if_delta_mismatch(insertions_sparql, deletions_sparql, insertions_iterative, deletions_iterative)

            # --- Apply the iterative delta to the triple store ---
            t_versioning_start = time.time_ns()
            self._sparql_engine.insert(insertions_iterative, timestamp=version_timestamp)
            self._sparql_engine.outdate(deletions_iterative, timestamp=version_timestamp)
            t_versioning = time.time_ns() - t_versioning_start

            # --- Persist timing rows and delta files ---
            cnt_triples = self._count_versioned_triples(version_timestamp)
            self._write_incremental_timing_files(
                version_timestamp, cnt_triples,
                insertions_sparql, deletions_sparql, t_prepare_sparql, t_delta_sparql, t_cleanup_sparql, t_versioning,
                insertions_iterative, deletions_iterative, t_prepare_iterative, t_delta_iterative,
            )
            self._write_delta_files(version_timestamp, insertions_iterative, deletions_iterative, insertions_sparql, deletions_sparql)
            self._archive_and_clean(self.base_path)

            self.LOG.info(f"[{self.repository_name}] Incremental versioning complete.")

            return DeltaEvent(
                id=self.tracking_task.id,
                repository_name=self.tracking_task.name,
                totalInsertions=len(insertions_iterative),
                totalDeletions=len(deletions_iterative),
                insertions=insertions_iterative,
                deletions=deletions_iterative,
                versioning_duration_ms=0,  # caller may compute if needed
                timestamp=version_timestamp,
            )
        except TimeoutError as e:
            self.LOG.error(f"[{self.repository_name}] Incremental versioning failed due to timeout: {e}")
            raise e
        except Exception as e:
            self.LOG.error(f"[{self.repository_name}] Incremental versioning failed: {e}\n{traceback.format_exc()}")
            self._sparql_calculator.clean_up()
            raise VersioningFailedException(self.repository_name, str(e))

    # ---------------------------------------------------------------------------
    # Pipeline steps
    # ---------------------------------------------------------------------------

    def _set_cycle_paths(self, version_timestamp: datetime):
        """Compute all file paths for this versioning cycle."""
        self._version_timestamp = version_timestamp
        ts_str = get_timestamp(version_timestamp)
        name   = self.tracking_task.name

        self.work_dir       = f"/data/evaluation/{name}"
        self.base_path      = f"{self.work_dir}/{ts_str}"
        self.snapshot_path  = f"{self.base_path}/{name}_{ts_str}.raw.nt"
        self.processed_path = f"{self.base_path}/{name}_{ts_str}.nt"
        self.import_path    = f"/graphdb-import/{name}.nt"

    def _download_snapshot(self):
        """Download the remote RDF dump to snapshot_path, retrying once on failure."""
        os.makedirs(self.base_path, exist_ok=True)
        
        if not self.local_file:
            for attempt in range(2):
                self.LOG.info(f"[{self.repository_name}] Downloading snapshot (attempt {attempt + 1}).")
                try:
                    obtain_nt(self.tracking_task.rdf_dataset_url, self.snapshot_path)
                    return
                except Exception as e:
                    if attempt == 1:
                        raise
                    self.LOG.warning(f"[{self.repository_name}] Download failed, retrying: {e}")
        else:
            self.LOG.info(f"[{self.repository_name}] Copying local snapshot file {self.base_path}/{self.tracking_task.name}_{get_timestamp(self._version_timestamp)}.raw.nt to {self.snapshot_path}.")
            shutil.copy2(f"{self.work_dir}/{self.tracking_task.name}_{get_timestamp(self._version_timestamp)}.raw.nt",self.snapshot_path)
    
    def _preprocess_snapshot(self):
        """Normalise and skolemize the raw N-Triples file."""
        self.LOG.info(f"[{self.repository_name}] Normalising and skolemizing snapshot.")
        normalize_and_skolemize(self.snapshot_path, self.processed_path)

    def _load_into_graphdb(self, graph_name: str):
        """Copy the processed file into the GraphDB import directory and trigger an import."""
        self.LOG.info(f"[{self.repository_name}] Importing snapshot into GraphDB graph '{graph_name}'.")
        
        shutil.copy2(self.processed_path, self.import_path)
        import_serverfile(f"{self.tracking_task.name}.nt", self.tracking_task.name, graph_name)
        poll_import_status(f"{self.tracking_task.name}.nt", self.tracking_task.name)

    def _count_versioned_triples(self, version_timestamp: datetime) -> int:
        """
        Query the triple store for the number of triples at the given timestamp.
        """
        query = get_count_triples_template(version_timestamp)
        result_df = self._sparql_engine.query(query, yn_timestamp_query=False)
 
        raw = result_df["cnt_triples"].iloc[0]
        # SPARQL may return typed literals like "42"^^xsd:integer — strip the type annotation
        count = int(raw.split("^^")[0].strip('"')) if isinstance(raw, str) else int(raw)
        self.LOG.info(f"[{self.repository_name}] Triple count at {version_timestamp}: {count}")
        
        return count

    # ---------------------------------------------------------------------------
    # Timing and delta file I/O
    # ---------------------------------------------------------------------------

    def _write_initial_timing_files(self, version_timestamp: datetime, cnt_triples: int):
        """Create timing CSV files and write the zero-delta initial row."""
        os.makedirs(self.work_dir, exist_ok=True)
        header  = "timestamp,insertions,deletions,time_prepare_ns,time_delta_ns,time_versioning_ns,time_overall_ns,cnt_triples\n"
        ts_str  = get_timestamp(version_timestamp)
        init_row = f"{ts_str},0,0,0,0,0,0,{cnt_triples}\n"

        for filename in [f"{self.tracking_task.name}_timings.csv", f"{self.tracking_task.name}_timings_sparql.csv"]:
            path = f"{self.work_dir}/{filename}"
            with open(path, "w") as f:
                f.write(header)
                f.write(init_row)

    def _write_incremental_timing_files(
        self, version_timestamp, cnt_triples,
        ins_sparql, del_sparql, t_prep_sparql, t_delta_sparql, t_cleanup_sparql, t_version,
        ins_iter, del_iter, t_prep_iter, t_delta_iter,
    ):
        """Append one timing row per method to the respective CSV files."""
        ts_str = get_timestamp(version_timestamp)

        t_overall_sparql    = t_prep_sparql + t_delta_sparql + t_cleanup_sparql + t_version
        t_overall_iterative = t_prep_iter   + t_delta_iter   + t_version

        sparql_row    = f"{ts_str},{len(ins_sparql)},{len(del_sparql)},{t_prep_sparql},{t_delta_sparql},{t_version},{t_overall_sparql},{cnt_triples}\n"
        iterative_row = f"{ts_str},{len(ins_iter)},{len(del_iter)},{t_prep_iter},{t_delta_iter},{t_version},{t_overall_iterative},{cnt_triples}\n"

        with open(f"{self.work_dir}/{self.tracking_task.name}_timings_sparql.csv", "a") as f:
            f.write(sparql_row)
        with open(f"{self.work_dir}/{self.tracking_task.name}_timings.csv", "a") as f:
            f.write(iterative_row)

    def _write_delta_files(self, version_timestamp, ins_iter, del_iter, ins_sparql, del_sparql):
        """Write +/- prefixed delta files if there are any changes."""
        ts_str   = get_timestamp(version_timestamp)
        name     = self.tracking_task.name

        if ins_sparql or del_sparql:
            with open(f"{self.base_path}/{name}_{ts_str}_sparql.delta", "a") as f:
                f.writelines(f"- {t}\n" for t in del_sparql)
                f.writelines(f"+ {t}\n" for t in ins_sparql)

        if ins_iter or del_iter:
            with open(f"{self.base_path}/{name}_{ts_str}_iterative.delta", "a") as f:
                f.writelines(f"- {t}\n" for t in del_iter)
                f.writelines(f"+ {t}\n" for t in ins_iter)

    def _archive_and_clean(self, directory: str):
        """Zip the versioning artefacts directory and remove the original folder."""
        if os.path.exists(directory):
            shutil.make_archive(directory, "zip", directory)
            shutil.rmtree(directory)
            self.LOG.info(f"[{self.repository_name}] Archived and removed {directory}.")

    # ---------------------------------------------------------------------------
    # Sanity check
    # ---------------------------------------------------------------------------

    def _warn_if_delta_mismatch(self, ins_sparql, del_sparql, ins_iter, del_iter):
        """Log an error when the two delta methods produce different cardinalities."""
        if len(ins_sparql) != len(ins_iter) or len(del_sparql) != len(del_iter):
            self.LOG.error(
                f"[{self.repository_name}] Delta mismatch! "
                f"SPARQL: +{len(ins_sparql)}/-{len(del_sparql)}  "
                f"Iterative: +{len(ins_iter)}/-{len(del_iter)}"
            )
