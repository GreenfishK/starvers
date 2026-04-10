"""
polling_task.py

PollingTask encapsulates a single recurring versioning job for one RDF dataset.

Lifecycle:
  1. Created by TaskScheduler with an initial delay of 0.
  2. run() is called by the thread pool.
  3. If the dataset's next_run is in the future (e.g. from a persisted schedule),
     the task re-queues itself for that future time without doing work.
  4. Otherwise it executes the versioning pipeline, updates next_run in the DB,
     re-computes metrics, and re-queues itself.
  5. On any unrecoverable error the task marks the dataset inactive and stops.
"""

import time
import traceback
import requests
from datetime import datetime
from typing import Callable
from uuid import UUID

from app.persistance.Database import Session, engine
from app.models.DatasetModel import Dataset
from app.models.TrackingTaskModel import TrackingTaskDto
from app.services.versioning_pipeline import VersioningPipeline
from app.services.metrics_service import MetricsService
from app.persistance.graphdb.GraphDatabaseUtils import create_engine
from app.LoggingConfig import get_logger

LOG = get_logger(__name__)


class PollingTask:
    def __init__(
        self,
        dataset_id: UUID,
        repository_name: str,
        latest_timestamp: datetime,
        period: int,
        is_initial: bool = True,
        time_func: Callable[[], float] = time.time,
        **kwargs,
    ):
        self.dataset_id = dataset_id
        self.repository_name = repository_name
        self.period = period
        self.latest_timestamp = latest_timestamp

        self._is_initial = is_initial
        self._time_func = time_func
        self._executor_ctx = kwargs["executor_ctx"]
        self._versioning_pipeline: VersioningPipeline | None = None

        # Schedule immediately
        self.next_run = self._time_func()
        LOG.info(f"[{self.repository_name}] PollingTask created. initial={is_initial}, latest_ts={latest_timestamp}")

    def __lt__(self, other: "PollingTask") -> bool:
        return self.next_run < other.next_run

    def __repr__(self) -> str:
        return f"PollingTask(repo={self.repository_name}, period={self.period}s, next_run={time.ctime(self.next_run)})"

    # ---------------------------------------------------------------------------
    # Main entry point called by the thread pool
    # ---------------------------------------------------------------------------

    def run(self):
        LOG.info(f"[{self.repository_name}] Polling task triggered. period={self.period}s")
        next_delay = None
        stopped = False

        try:
            with Session(engine) as session:
                dataset = self._load_dataset(session)

                # If the DB says next_run is still in the future, skip this cycle
                if self._is_run_premature(dataset):
                    next_delay = dataset.next_run.timestamp() - self._time_func()
                else:
                    # Run the versioning pipeline and measure how long it takes
                    version_timestamp = datetime.now()
                    
                    LOG.info(f"[{self.repository_name}] Starting measuring time for run at {version_timestamp}.")
                    start_ns = time.time_ns()
                    stopped = self._execute_versioning(session, dataset, version_timestamp)
                    elapsed_s = (time.time_ns() - start_ns) / 1_000_000_000
                    LOG.info(f"[{self.repository_name}] Versioning run completed in {elapsed_s:.2f}s.")
                    
                    # Persist next_run and compute metrics unless versioning failed
                    if not stopped:
                        next_delay = self._finalize_run(session, dataset, version_timestamp, elapsed_s)

        except Exception as e:
            LOG.error(f"[{self.repository_name}] Polling task failed: {e}\n{traceback.format_exc()}")
            stopped = True

        # Either stop permanently or re-queue for the next cycle
        if stopped:
            self._mark_dataset_inactive()
        else:
            self._reschedule(next_delay)

    # ---------------------------------------------------------------------------
    # Helpers called from run()
    # ---------------------------------------------------------------------------

    def _load_dataset(self, session: Session) -> Dataset:
        dataset = session.get(Dataset, self.dataset_id)
        if not dataset:
            raise ValueError(f"Dataset '{self.repository_name}' not found in database.")
        return dataset

    def _is_run_premature(self, dataset: Dataset) -> bool:
        """Return True if the DB-persisted next_run is still in the future."""
        if dataset.next_run is None:
            return False
        scheduled_at = dataset.next_run.timestamp()
        if scheduled_at > self._time_func():
            self._is_initial = False
            self.next_run = scheduled_at
            return True
        return False

    def _execute_versioning(self, session: Session, dataset: Dataset, version_timestamp: datetime) -> bool:
        """Run the versioning pipeline. Returns True if the task should stop after this."""
        LOG.info(f"[{self.repository_name}] Executing versioning pipeline.")
        
        if not dataset.active:
            LOG.info(f"[{self.repository_name}] Dataset is inactive. Skipping versioning.")
            return True

        # Lazy-initialise the pipeline (holds SPARQL connection state)
        if self._versioning_pipeline is None:
            tracking_task = TrackingTaskDto(
                id=dataset.id,
                name=dataset.repository_name,
                rdf_dataset_url=dataset.rdf_dataset_url,
            )
            LOG.info(f"[{self.repository_name}] Initializing versioning pipeline.")
            self._versioning_pipeline = VersioningPipeline(tracking_task, self.repository_name)

        # Initial run: load data with no delta; subsequent runs: compute and apply delta
        if self._is_initial:
            LOG.info(f"[{self.repository_name}] Running initial versioning.")
            self._versioning_pipeline.run_initial_versioning(version_timestamp)
        else:
            LOG.info(f"[{self.repository_name}] Running incremental versioning.")
            delta = self._versioning_pipeline.run_versioning(version_timestamp)

            if delta.totalInsertions > 0 or delta.totalDeletions > 0:
                LOG.info(f"[{self.repository_name}] Changes detected: +{delta.totalInsertions} / -{delta.totalDeletions}")
                dataset.last_modified = version_timestamp
                session.commit()
                session.refresh(dataset)

                if dataset.notification_webhook:
                    LOG.info(f"[{self.repository_name}] Notifying webhook.")
                    requests.post(dataset.notification_webhook, json=delta.model_dump(mode="json"))
            else:
                LOG.info(f"[{self.repository_name}] No changes detected.")

        return False  # do not stop

    def _finalize_run(
        self, session: Session, dataset: Dataset, version_timestamp: datetime, elapsed_s: float
    ) -> float:
        """Persist next_run, update metrics, advance internal state. Returns delay until next run."""
        # Advance the internal schedule
        self._is_initial = False
        self.next_run = self._time_func() + self.period - elapsed_s
        next_delay = self.period - elapsed_s

        # Persist next_run in the DB so a restart can honour it
        dataset.next_run = datetime.fromtimestamp(self.next_run)
        session.commit()
        session.refresh(dataset)

        # Compute class and property metrics via SPARQL
        sparql_engine = create_engine(dataset.repository_name)
        metrics = MetricsService(sparql_engine, session)
        metrics.update_static_core_triples(dataset, self.repository_name)
        metrics.update_version_oblivious_triples(dataset, self.repository_name)
        metrics.update_class_statistics(self.dataset_id, self.repository_name, version_timestamp, self.latest_timestamp)
        metrics.update_property_statistics(self.dataset_id, self.repository_name, version_timestamp, self.latest_timestamp)

        self.latest_timestamp = version_timestamp
        return next_delay

    def _mark_dataset_inactive(self):
        """Permanently deactivate this dataset in the DB and stop rescheduling."""
        LOG.warning(
            f"[{self.repository_name}] Stopping task permanently. "
            "Fix the error and re-register the dataset to resume tracking."
        )
        with Session(engine) as session:
            dataset = session.get(Dataset, self.dataset_id)
            if dataset:
                dataset.active = False
                session.commit()

    def _reschedule(self, next_delay: float | None):
        """Put this task back into the scheduler queue."""
        if next_delay is None or next_delay <= 0 or self.next_run <= self._time_func():
            LOG.info(f"[{self.repository_name}] Next run is overdue — rescheduling immediately.")
            self._executor_ctx._enqueue(self, self.repository_name, 0)
        else:
            LOG.info(f"[{self.repository_name}] Next run at {datetime.fromtimestamp(self.next_run)} (in {next_delay:.1f}s).")
            self._executor_ctx._enqueue(self, self.repository_name, next_delay)
