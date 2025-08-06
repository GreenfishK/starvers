import time
from datetime import datetime
from uuid import UUID
import requests
from io import StringIO
import pandas as pd

from app.Database import Session, engine
from app.models.DatasetModel import Dataset, Snapshot
from app.models.TrackingTaskModel import TrackingTaskDto
from app.services.VersioningService import StarVersService
from app.utils.graphdb.GraphDatabaseUtils import get_snapshot_metrics_template, create_engine
from app.LoggingConfig import get_logger


LOG = get_logger(__name__)

class PollingTask():
    def __init__(self, dataset_id: UUID, repository_name, latest_timestamp, period: int, *args, time_func=time.time, is_initial=True, **kwargs):
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


    @property
    def is_initial_run(self) -> bool:
        return self.__is_initial
    
    @property
    def executor_ctx(self):
        return self.kwargs['executor_ctx']
    
    @property
    def time_func(self):
        return self.__time_func

    def __get_next_run(self) -> int:
        return self.__time_func() + self.period

    def set_next_run(self, time_taken: int = 0) -> None:
        self.__is_initial = False
        self.next_run = self.__get_next_run() - time_taken

    def __lt__(self, other) -> bool:
        return self.next_run < other.next_run

    def __repr__(self) -> str:
        return f"Repository name: {self.repository_name}: Polling Task: Periodic: {self.period} seconds (s), Next run: {time.ctime(self.next_run)})"

    def run(self):
        LOG.info(f"Repository name: {self.repository_name}: Running polling task for dataset with period={self.period} seconds (s)")
        start_time = time.time_ns()
        next_delay = -1
        try:
            with Session(engine) as session:
                dataset = session.get(Dataset, self.dataset_id)
                if dataset.next_run is None or dataset.next_run <= datetime.fromtimestamp(self.__time_func()):
                    # Versioning
                    version_timestamp = datetime.now()
                    self.__stopped = self.__run(session, dataset, version_timestamp)
                    end_time = time.time_ns()
                    time_taken = (end_time - start_time) / 1_000_000_000 # convert ns to s
                    self.set_next_run(time_taken)
                    next_delay = self.period - time_taken

                    # Update dataset table: next run time
                    LOG.info(f"Repository name: {self.repository_name}: Updating 'dataset' table: next run time")
                    dataset.next_run = datetime.fromtimestamp(self.next_run)
                    session.commit()
                    session.refresh(dataset)
                    
                    # Setup connection to GraphDB for retrieving snapshot metrics
                    sparql_engine = create_engine(dataset.repository_name)

                    # Retrieve metrics from GraphDB via SPARQL query in the csv format
                    LOG.info(f"Repository name: {self.repository_name}: Querying snapshot metrics from GraphDB")
                    if self.latest_timestamp:
                        query = get_snapshot_metrics_template(ts_current=version_timestamp, ts_prev=self.latest_timestamp)
                    else:
                        query = get_snapshot_metrics_template(ts_current=version_timestamp, ts_prev=version_timestamp)
                    sparql_engine.setQuery(query)
                    response = sparql_engine.query().convert() 
                    csv_text = response.decode('utf-8')
                    df_metrics = pd.read_csv(StringIO(csv_text))

                    snapshots = []
                    for _, row in df_metrics.iterrows():
                        LOG.info(f'Parent class: {row["parent_onto_class"]}; Type: {type(row["parent_onto_class"])}')
                        snapshot = Snapshot(
                            dataset_id=self.dataset_id,
                            snapshot_ts=version_timestamp,
                            snapshot_ts_prev=self.latest_timestamp if self.latest_timestamp else version_timestamp,
                            onto_class=row["onto_class"],
                            parent_onto_class=row["parent_onto_class"] if pd.notna(row["parent_onto_class"]) else None,
                            cnt_class_instances_current=row["cnt_class_instances_current"],
                            cnt_class_instances_prev=row["cnt_class_instances_prev"],
                            cnt_classes_added=row["cnt_classes_added"],
                            cnt_classes_deleted=row["cnt_classes_deleted"]
                        )
                        snapshots.append(snapshot)

                    if snapshots:
                        LOG.info(f"Repository name: {self.repository_name}: Inserting {len(df_metrics)} computed metrics into 'snapshot' table.")
                        session.add_all(snapshots)
                        session.commit()
                        for snap in snapshots:
                            session.refresh(snap) 
                    else:
                        LOG.info(f"Repository name: {self.repository_name}: Query returned no metrics. Nothing will be inserted.")
                else:
                    self.__is_initial = False
                    self.next_run = dataset.next_run.timestamp()
                    next_delay = dataset.next_run.timestamp() - self.__time_func()
                    
        except Exception as e:
            LOG.error(e)
        finally:
            if self.__stopped:
                LOG.info(f'Repository name: {self.repository_name}: Stopped tracking task for dataset.')
                return
            
            # Re-schedule task
            LOG.info(f"Repository name: {self.repository_name}: Re-scheduling task for dataset to run at {datetime.fromtimestamp(self.next_run)} (next run time).")
            if next_delay < 0 or self.next_run <= self.__time_func():
                self.executor_ctx._put(self, 0)
            else:
                self.executor_ctx._put(self, next_delay)
            

            
    def __run(self, session: Session, dataset: Dataset, version_timestamp: datetime) -> bool:
        LOG.info(f"Repository name: {self.repository_name}: Start tracking task for dataset.")

        if not dataset.active:
            LOG.info(f"Repository name: {self.repository_name}: Dataset is not active. Skipping tracking task for dataset.")
            return True

        if self.__versioning_wrapper is None:
            tracking_task = TrackingTaskDto(
                id=dataset.id,
                name=dataset.repository_name,
                rdf_dataset_url=dataset.rdf_dataset_url,
                delta_type=dataset.delta_type)
                        
            self.__versioning_wrapper = StarVersService(tracking_task, self.repository_name)

        if (self.__is_initial): # if initial no diff is necessary
            LOG.info(f"Repository name: {self.repository_name}: Initial versioning of dataset.")

            # push triples into repository
            self.__versioning_wrapper.run_initial_versioning(version_timestamp)

        else: 
            LOG.info(f"Repository name: {self.repository_name}: Continue versioning dataset.")
            # get diff to previous version using StarVers
            delta_event = self.__versioning_wrapper.run_versioning(version_timestamp)

            if delta_event is not None:
                LOG.info(f"Repository name: {self.repository_name}: Changes for dataset detected")

                if dataset.notification_webhook is not None:
                    LOG.info(f"Repository name: {self.repository_name}: Notified webhook for dataset")
                    requests.post(dataset.notification_webhook, json=delta_event.model_dump(mode='json'))
            else:
                LOG.info(f"Repository name: {self.repository_name}: No changes for dataset detected")
                LOG.info(f"Repository name: {self.repository_name}: Finished tracking task for dataset")

                return
        
        dataset.last_modified = version_timestamp
        session.commit()
        session.refresh(dataset)

        LOG.info(f"Repository name: {self.repository_name}: Finished tracking task for dataset id={self.dataset_id}")

        return False
