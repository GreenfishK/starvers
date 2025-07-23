from sqlmodel import Session, select
from uuid import UUID
from typing import List
from datetime import datetime

from app.LoggingConfig import get_logger
from app.models.DatasetModel import Dataset, DatasetCreate
from app.services import ScheduledThreadPoolExecutor
from app.utils.graphdb.GraphDatabaseUtils import create_repository
from app.utils.exceptions.DatasetNotFoundException import DatasetNotFoundException

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

def add(dataset: DatasetCreate, session: Session) -> List[Dataset]:
    db_dataset = Dataset.model_validate(dataset)

    session.add(db_dataset)
    session.commit()
    session.refresh(db_dataset)

    __start(db_dataset)

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
    active_graphs = get_all(session)
    LOG.info(f'Restart {len(active_graphs)} active versioning task')
    for graph in active_graphs:
        __start(graph, False)
        pass

def __start(dataset: Dataset, initial_run=True):
    create_repository(dataset.repository_name)
    polling_executor.schedule_polling_at_fixed_rate(dataset.id, dataset.polling_interval, dataset.delta_type, initial_run=initial_run)
