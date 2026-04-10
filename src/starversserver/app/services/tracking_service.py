"""
tracking_service.py

High-level orchestration for dataset tracking lifecycle:
register, deactivate, restart.

All database read/write operations are delegated to dataset_repository.py.
Scheduling is delegated to task_scheduler.py.
"""

from uuid import UUID

from sqlmodel import Session

from app.models.DatasetModel import Dataset, DatasetCreate
from app.services import dataset_repository
from app.services.task_scheduler import scheduler
from app.persistance.graphdb.GraphDatabaseUtils import create_repository
from app.LoggingConfig import get_logger

LOG = get_logger(__name__)


# ---------------------------------------------------------------------------
# Public API: register, deactivate, restart
# ---------------------------------------------------------------------------

def register_dataset(dataset_create: DatasetCreate, session: Session) -> Dataset:
    """
    Persist a new dataset and kick off its first tracking run immediately.
    """
    dataset = dataset_repository.create_dataset(dataset_create, session)
    LOG.info(f"[{dataset.repository_name}] Dataset registered. Starting initial tracking.")
    _initialize_and_schedule(dataset, session, initial_run=True)
    return dataset


def deactivate_dataset(dataset_id: UUID, session: Session) -> Dataset:
    """
    Mark a single dataset as inactive. The running PollingTask will notice and stop itself.
    """
    dataset = dataset_repository.mark_dataset_inactive(dataset_id, session)
    LOG.info(f"[{dataset.repository_name}] Dataset marked inactive.")
    return dataset


def deactivate_all_datasets(session: Session) -> list[Dataset]:
    """
    Mark all active datasets as inactive (e.g. on a clean shutdown request).
    """
    datasets = dataset_repository.mark_all_datasets_inactive(session)
    LOG.info(f"Deactivated {len(datasets)} dataset(s).")
    return datasets


def restart_active_tracking_tasks(session: Session):
    """
    Re-schedule all datasets that were active before a server restart.
    Called once at application startup.
    """
    active_datasets = dataset_repository.get_all_active_datasets(session)
    LOG.info(f"Restarting tracking for {len(active_datasets)} active dataset(s).")

    for dataset in active_datasets:
        _initialize_and_schedule(dataset, session, initial_run=False)


# ---------------------------------------------------------------------------
# Internal: GraphDB setup + scheduler handoff
# ---------------------------------------------------------------------------

def _initialize_and_schedule(dataset: Dataset, session: Session, initial_run: bool):
    """
    Ensure the GraphDB repository exists and is configured, then hand the
    dataset off to the scheduler for periodic polling.
    """
    # Create the triple store repository if it does not exist yet
    create_repository(dataset.repository_name)

    # Look up the latest snapshot so the polling task knows where to resume
    latest_snapshot_ts = dataset_repository.get_latest_snapshot_timestamp(session, dataset.id)
    LOG.info(f"[{dataset.repository_name}] Latest snapshot timestamp: {latest_snapshot_ts}.")

    # Hand off to the scheduler
    scheduler.schedule(
        dataset_id=dataset.id,
        repository_name=dataset.repository_name,
        latest_timestamp=latest_snapshot_ts,
        poll_interval_seconds=dataset.polling_interval,
        initial_run=initial_run,
    )
