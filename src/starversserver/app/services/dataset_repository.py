"""
dataset_repository.py

All database read/write operations for Dataset and Snapshot models.
No business logic lives here — only data access.
"""

from datetime import datetime
from typing import Optional, Tuple
from uuid import UUID

import pandas as pd
from sqlalchemy import desc
from sqlmodel import Session, select

from app.models.DatasetModel import Dataset, DatasetCreate, Snapshot
from app.exceptions.DatasetNotFoundException import DatasetNotFoundException
from app.LoggingConfig import get_logger

LOG = get_logger(__name__)


# ---------------------------------------------------------------------------
# Dataset reads
# ---------------------------------------------------------------------------

def get_all_active_datasets(session: Session) -> list[Dataset]:
    return session.exec(select(Dataset).where(Dataset.active)).all()


def get_dataset_by_id(dataset_id: UUID, session: Session) -> Dataset:
    dataset = session.get(Dataset, dataset_id)
    if not dataset:
        raise DatasetNotFoundException(id=dataset_id)
    return dataset


def get_dataset_id_by_repo_name(repo_name: str, session: Session) -> UUID:
    result = session.exec(
        select(Dataset.id).where(Dataset.repository_name == repo_name)
    ).first()
    if result is None:
        raise DatasetNotFoundException(name=repo_name)
    return result


def get_dataset_metadata_by_repo_name(repo_name: str, session: Session) -> Optional[Tuple]:
    """Return a lightweight metadata tuple for the given repository name."""
    return session.exec(
        select(
            Dataset.repository_name,
            Dataset.rdf_dataset_url,
            Dataset.polling_interval,
            Dataset.next_run,
            Dataset.cnt_triples_static_core,
            Dataset.cnt_triples_version_oblivious,
        ).where(Dataset.repository_name == repo_name)
    ).first()


def get_latest_snapshot_timestamp(session: Session, dataset_id: UUID) -> Optional[datetime]:
    result = session.exec(
        select(Snapshot.snapshot_ts)
        .where(Snapshot.dataset_id == dataset_id)
        .order_by(desc(Snapshot.snapshot_ts))
        .limit(1)
    ).first()
    return result


# ---------------------------------------------------------------------------
# Snapshot reads
# ---------------------------------------------------------------------------

def get_snapshot_stats(repo_name: str, snapshot_ts: datetime, session: Session) -> pd.DataFrame:
    """
    Return a DataFrame with class and property metrics for the latest snapshot
    at or before the given timestamp.
    """
    # Find the closest snapshot timestamp that does not exceed the requested one
    actual_ts = session.exec(
        select(Snapshot.snapshot_ts)
        .join(Dataset, Snapshot.dataset_id == Dataset.id)
        .where(Dataset.repository_name == repo_name)
        .where(Snapshot.snapshot_ts <= snapshot_ts)
        .order_by(Snapshot.snapshot_ts.desc())
        .limit(1)
    ).first()

    if not actual_ts:
        return pd.DataFrame()

    # Retrieve all metric columns for that timestamp
    results = session.exec(
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
            Snapshot.cnt_properties_deleted,
        )
        .join(Dataset, Snapshot.dataset_id == Dataset.id)
        .where(Dataset.repository_name == repo_name)
        .where(Snapshot.snapshot_ts == actual_ts)
    ).all()

    return pd.DataFrame(results, columns=[
        "onto_class", "onto_class_label", "parent_onto_class", "snapshot_ts",
        "cnt_class_instances_current", "cnt_class_instances_prev",
        "cnt_classes_added", "cnt_classes_deleted",
        "onto_property", "onto_property_label", "parent_property",
        "cnt_property_instances_current", "cnt_property_instances_prev",
        "cnt_properties_added", "cnt_properties_deleted",
    ])


# ---------------------------------------------------------------------------
# Dataset writes
# ---------------------------------------------------------------------------

def create_dataset(dataset_create: DatasetCreate, session: Session) -> Dataset:
    dataset = Dataset.model_validate(dataset_create)
    session.add(dataset)
    session.commit()
    session.refresh(dataset)
    return dataset


def mark_dataset_inactive(dataset_id: UUID, session: Session) -> Dataset:
    dataset = get_dataset_by_id(dataset_id, session)
    if dataset.active:
        dataset.active = False
        dataset.last_modified = datetime.now()
        session.add(dataset)
        session.commit()
        session.refresh(dataset)
    return dataset


def mark_all_datasets_inactive(session: Session) -> list[Dataset]:
    datasets = get_all_active_datasets(session)
    for dataset in datasets:
        dataset.active = False
        dataset.last_modified = datetime.now()
        session.add(dataset)
    session.commit()
    return datasets
