from __future__ import annotations

import time
from datetime import datetime
from uuid import UUID
import requests
from typing import Callable


from app.persistance.Database import Session, engine
from app.models.DatasetModel import Dataset
from app.models.TrackingTaskModel import TrackingTaskDto
from app.services.VersioningService import StarVersService
from app.services.MetricsService import MetricsService
from app.persistance.graphdb.GraphDatabaseUtils import create_engine
from app.LoggingConfig import get_logger

LOG = get_logger(__name__)

class PollingTask():
    def __init__(self, dataset_id: UUID, repository_name: str, latest_timestamp: datetime, period: int, time_func: Callable[[], float] = time.time, is_initial: bool=True, *args, **kwargs):
        super().__init__()
        self.dataset_id = dataset_id
        self.repository_name = repository_name
        self.period = period
        self.args = args
        self.kwargs = kwargs
        self.__stopped = False
        self.__time_func = time_func
        self.__is_initial = is_initial
        self.next_run = self.__time_func()
        self.__versioning_wrapper = None
        self.latest_timestamp = latest_timestamp
        LOG.info(f"Repository name: {self.repository_name}: Polling task created with next_run={self.next_run} latest_timestamp={self.latest_timestamp}.")
        if is_initial:
            LOG.info(f"Repository name {self.repository_name}: This is the initial version of this dataset.")

    @property
    def is_initial_run(self) -> bool:
        return self.__is_initial
    
    @property
    def executor_ctx(self):
        return self.kwargs['executor_ctx']
    
    @property
    def time_func(self):
        return self.__time_func

    def set_next_run(self, time_taken: float = 0) -> None:
        self.__is_initial = False
        self.next_run = self.__time_func() + self.period - time_taken

    def __lt__(self, other: PollingTask) -> bool:
        return self.next_run < other.next_run

    def __repr__(self) -> str:
        return f"Repository name: {self.repository_name}: Polling Task: Periodic: {self.period} seconds (s), Next run: {time.ctime(self.next_run)})"

    def run(self):
        LOG.info(f"Repository name: {self.repository_name}: Running polling task for dataset with period={self.period} seconds (s)")
        next_delay = None

        try:
            with Session(engine) as session:
                # --- Fetch dataset ---
                dataset = session.get(Dataset, self.dataset_id)
                if not dataset:
                    raise ValueError(f"Dataset with repo name '{self.repository_name}' not found in the database.")

                # --- Check if run is due ---
                if dataset.next_run is not None and dataset.next_run > datetime.fromtimestamp(self.__time_func()):
                    self.__is_initial = False
                    self.next_run = dataset.next_run.timestamp()
                    next_delay = dataset.next_run.timestamp() - self.__time_func()
                else:
                    # --- Execute run ---
                    version_timestamp = datetime.now()
                    start_time = time.time_ns()
                    self.__stopped = self.__run(session, dataset, version_timestamp)
                    time_taken = (time.time_ns() - start_time) / 1_000_000_000

                    # --- Schedule next run ---
                    self.set_next_run(time_taken)
                    next_delay = self.period - time_taken
                    dataset.next_run = datetime.fromtimestamp(self.next_run)
                    LOG.info(f"Repository name: {self.repository_name}: Updating next_run timestamp in database.")
                    session.commit()
                    session.refresh(dataset)

                    # --- Update metrics ---
                    if not self.__stopped:
                        sparql_engine = create_engine(dataset.repository_name)
                        metrics_service = MetricsService(sparql_engine, session)
                        metrics_service.update_static_core_triples(dataset, self.repository_name)
                        metrics_service.update_version_oblivious_triples(dataset, self.repository_name)
                        metrics_service.update_class_statistics(self.dataset_id, self.repository_name, version_timestamp, self.latest_timestamp)
                        metrics_service.update_property_statistics(self.dataset_id, self.repository_name, version_timestamp, self.latest_timestamp)
                        self.latest_timestamp = version_timestamp

        except Exception as e:
            LOG.error(f"Repository name: {self.repository_name}: Polling task failed with error {e}.")
            self.__stopped = True

        # --- Reschedule or stop ---
        if self.__stopped:
            LOG.info(f"Repository name: {self.repository_name}: Stopped tracking task for dataset. Setting 'active' to False in database. Fix the error and re-start the task to continue tracking.")
            
            # Make dataset inactive in database
            with Session(engine) as session:
                dataset = session.get(Dataset, self.dataset_id) 
                if dataset:
                    dataset.active = False
                    session.commit()
            return

        if next_delay is None or next_delay < 0 or self.next_run <= self.__time_func():
            LOG.info(f"Repository name: {self.repository_name}: Next run is in the past. Scheduling task immediately.")
            self.executor_ctx._put(self, self.repository_name, 0)
        else:
            LOG.info(f"Repository name: {self.repository_name}: Scheduled task to run at {datetime.fromtimestamp(self.next_run)} with delay {next_delay}.")
            self.executor_ctx._put(self, self.repository_name, next_delay)


            
    def __run(self, session: Session, dataset: Dataset, version_timestamp: datetime) -> bool:
        LOG.info(f"Repository name: {self.repository_name}: Start tracking task for dataset.")

        if not dataset.active:
            LOG.info(f"Repository name: {self.repository_name}: Dataset is not active. Skipping tracking task for dataset.")
            return True

        if self.__versioning_wrapper is None:
            tracking_task = TrackingTaskDto(
                id=dataset.id,
                name=dataset.repository_name,
                rdf_dataset_url=dataset.rdf_dataset_url)
                        
            self.__versioning_wrapper = StarVersService(tracking_task, self.repository_name)

        if (self.__is_initial): # if initial no diff is necessary
            LOG.info(f"Repository name: {self.repository_name}: Initial versioning of dataset.")

            # push triples into repository
            self.__versioning_wrapper.run_initial_versioning(version_timestamp)

        else: 
            LOG.info(f"Repository name: {self.repository_name}: Continue versioning dataset.")
            # get diff to previous version using StarVers
            delta_event = self.__versioning_wrapper.run_versioning(version_timestamp)

            if delta_event.totalInsertions > 0 or delta_event.totalDeletions > 0:
                LOG.info(f"Repository name: {self.repository_name}: Changes for dataset detected")
                
                # Update last modified timestamp in database
                dataset.last_modified = version_timestamp
                session.commit()
                session.refresh(dataset)

                if dataset.notification_webhook is not None:
                    LOG.info(f"Repository name: {self.repository_name}: Notified webhook for dataset")
                    requests.post(dataset.notification_webhook, json=delta_event.model_dump(mode='json'))
            else:
                LOG.info(f"Repository name: {self.repository_name}: No changes for dataset detected")
                LOG.info(f"Repository name: {self.repository_name}: Finished tracking task for dataset")


        LOG.info(f"Repository name: {self.repository_name}: Finished tracking task for dataset id={self.dataset_id}")

        return False
