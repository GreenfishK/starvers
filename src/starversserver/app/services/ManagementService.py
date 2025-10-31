from sqlmodel import Session, select

from sqlalchemy import desc
from uuid import UUID
from typing import List
from datetime import datetime
from typing import Optional, Tuple
import pandas as pd

from app.models.DatasetModel import Dataset, DatasetCreate, Snapshot
from app.services import ScheduledThreadPoolExecutor
from app.persistance.graphdb.GraphDatabaseUtils import create_repository
from app.exceptions.DatasetNotFoundException import DatasetNotFoundException
from app.LoggingConfig import get_logger

LOG = get_logger(__name__)

polling_executor: ScheduledThreadPoolExecutor.ScheduledThreadPoolExecutor = ScheduledThreadPoolExecutor.ScheduledThreadPoolExecutor(10)
polling_executor.start()

def get_all(session: Session) -> List[Dataset]:
    datasets = session.exec(select(Dataset).where(Dataset.active)).all()
    return datasets
    
def get_by_id(id: UUID, session: Session) -> Dataset:
    dataset = session.get(Dataset, id)

    if not dataset:
        raise DatasetNotFoundException(id=id)
    return dataset


def get_dataset_metadata_by_repo_name(repo_name: str, session: Session) -> Optional[Tuple[str, str, int]]:
    statement = (
        select(Dataset.repository_name, Dataset.rdf_dataset_url, Dataset.polling_interval,
         Dataset.next_run, Dataset.cnt_triples_static_core, Dataset.cnt_triples_version_oblivious)
        .where(Dataset.repository_name == repo_name)
    )
    result = session.exec(statement).first()
    return result 

def get_snapshot_stats_by_repo_name_and_snapshot_ts(repo_name: str, snapshot_ts: datetime, session: Session):
    # Find the latest snapshot_ts <= given snapshot_ts
    ts_stmt = (
        select(Snapshot.snapshot_ts)
        .join(Dataset, Snapshot.dataset_id == Dataset.id)
        .where(Dataset.repository_name == repo_name)
        .where(Snapshot.snapshot_ts <= snapshot_ts)
        .order_by(Snapshot.snapshot_ts.desc())
        .limit(1)
    )
    latest_ts_result = session.exec(ts_stmt).first()
    if not latest_ts_result:
        return pd.DataFrame()

    actual_ts = latest_ts_result

    stmt = (
        select(
            Snapshot.onto_class,
            Snapshot.onto_class_label,
            Snapshot.parent_onto_class,
            Snapshot.snapshot_ts,
            Snapshot.cnt_class_instances_current,
            Snapshot.cnt_class_instances_prev,
            Snapshot.cnt_classes_added,
            Snapshot.cnt_classes_deleted,
            Snapshot.onto_property,
            Snapshot.onto_property_label,
            Snapshot.parent_property,
            Snapshot.cnt_property_instances_current,
            Snapshot.cnt_property_instances_prev,
            Snapshot.cnt_properties_added,
            Snapshot.cnt_properties_deleted
        )
        .join(Dataset, Snapshot.dataset_id == Dataset.id)
        .where(Dataset.repository_name == repo_name)
        .where(Snapshot.snapshot_ts == actual_ts)
    )
    results = session.exec(stmt).all()
    df = pd.DataFrame(results, columns=[
        "onto_class", "onto_class_label", "parent_onto_class", "snapshot_ts",
        "cnt_class_instances_current", "cnt_class_instances_prev",
        "cnt_classes_added", "cnt_classes_deleted", "onto_property", "onto_property_label",
        "parent_property", "cnt_property_instances_current", "cnt_property_instances_prev",
        "cnt_properties_added", "cnt_properties_deleted"
    ])

    return df

def get_id_by_repo_name(repo_name: str, session: Session) -> UUID:
    statement = (
        select(Dataset.id)
        .where(Dataset.repository_name == repo_name)
    )
    result = session.exec(statement).first()

    if result is None:
        raise DatasetNotFoundException(name=repo_name)

    return result

def get_latest_snapshot_timestamp(session: Session, dataset_id: UUID):
    statement = (
        select(Snapshot.snapshot_ts)
        .where(Snapshot.dataset_id == dataset_id)
        .order_by(desc(Snapshot.snapshot_ts))
        .limit(1)
    )
    result = session.exec(statement).first()
    latest_timestamp = result if result else None

    return latest_timestamp

def add(dataset: DatasetCreate, session: Session) -> List[Dataset]:
    db_dataset = Dataset.model_validate(dataset)

    session.add(db_dataset)
    session.commit()
    session.refresh(db_dataset)

    __start(session, db_dataset)

    return db_dataset

def delete(id: UUID, session: Session) -> Dataset:
    db_dataset = get_by_id(id, session)
    if (db_dataset.active):
        db_dataset.active = False
        db_dataset.last_modified = datetime.now()
        session.add(db_dataset)
        session.commit()
        session.refresh(db_dataset)
    return db_dataset


def delete_all(session: Session) -> List[Dataset]:
    db_datasets = get_all(session)

    for db_dataset in db_datasets:
        if (db_dataset.active):
            db_dataset.active = False
            db_dataset.last_modified = datetime.now()
            session.add(db_dataset)
    session.commit()

    return db_datasets

def restart(session: Session):
    # Query active dataset from database
    active_graphs = get_all(session)

    # Restart active versioning tasks
    LOG.info(f'Restart {len(active_graphs)} active versioning task')
    for graph in active_graphs:
        __start(session, graph, False)
        pass

def __start(session: Session, dataset: Dataset, initial_run:bool=True):
    # Create triple store repository if it does not exist
    create_repository(dataset.repository_name)

    # Polling, delta calculation, versioning
    LOG.info(f"Repository name: {dataset.repository_name}: Query latest timestamp from snapshot table.")
    latest_timestamp = get_latest_snapshot_timestamp(session, dataset.id)
    
    LOG.info(f"Repository name: {dataset.repository_name}: Latest timestamp: {latest_timestamp}")
    polling_executor.schedule_polling_at_fixed_rate(dataset.id, dataset.repository_name, latest_timestamp, dataset.polling_interval, initial_run=initial_run)