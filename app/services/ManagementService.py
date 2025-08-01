from sqlmodel import Session, select
from sqlalchemy import desc
from uuid import UUID
from typing import List
from datetime import datetime

from app.models.DatasetModel import Dataset, DatasetCreate, Snapshot
from app.services import ScheduledThreadPoolExecutor
from app.utils.graphdb.GraphDatabaseUtils import create_repository
from app.utils.exceptions.DatasetNotFoundException import DatasetNotFoundException
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

from typing import Optional, Tuple

def get_dataset_metadata_by_repo_name(repo_name: str, session: Session) -> Optional[Tuple[str, str, int]]:
    statement = (
        select(Dataset.repository_name, Dataset.rdf_dataset_url, Dataset.polling_interval, Dataset.next_run)
        .where(Dataset.repository_name == repo_name)
        .where(Dataset.active)
    )
    result = session.exec(statement).first()
    return result 

def get_id_by_repo_name(repo_name: str, session: Session) -> str:
    statement = (
        select(Dataset.id)
        .where(Dataset.repository_name == repo_name)
    )
    result = session.exec(statement).first()

    if result is None:
        raise DatasetNotFoundException(name=repo_name)

    return str(result)

def get_latest_snapshot_timestamp(session: Session, dataset_id: str):
    statement = (
        select(Snapshot.snapshot_ts)
        .where(Snapshot.dataset_id == dataset_id)
        .order_by(desc(Snapshot.snapshot_ts))
        .limit(1)
    )
    result = session.exec(statement).first()
    latest_timestamp = result[0] if result else None

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

def __start(session: Session, dataset: Dataset, initial_run=True):
    # Create triple store repository if it does not exist
    create_repository(dataset.repository_name)

    # Polling, delta calculation, versioning
    LOG.info(f"Repository name: {dataset.repository_name}: Query latest timestamp from snapshot table.")
    latest_timestamp = get_latest_snapshot_timestamp(session, dataset.id)
    
    LOG.info(f"Repository name: {dataset.repository_name}: Latest timestamp: {latest_timestamp}")
    polling_executor.schedule_polling_at_fixed_rate(dataset.id, dataset.repository_name, latest_timestamp, dataset.polling_interval, dataset.delta_type, initial_run=initial_run)