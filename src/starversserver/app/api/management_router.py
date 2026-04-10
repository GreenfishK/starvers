"""
management_router.py

REST endpoints for registering, inspecting, and deactivating RDF dataset tracking tasks.
All business logic is delegated to tracking_service and dataset_repository.
"""

from typing import List
from uuid import UUID

from fastapi import APIRouter, Depends
from sqlmodel import Session

from app.persistance.Database import get_session
from app.models.DatasetModel import DatasetRead, DatasetCreate
from app.services.tracking_service import register_dataset, deactivate_dataset, deactivate_all_datasets
from app.services.dataset_repository import get_all_active_datasets, get_dataset_by_id

tag = "management"

router = APIRouter(prefix="/management", tags=[tag])

tag_metadata = {
    "name": tag,
    "description": "Manage versioning of RDF datasets — register, inspect, and deactivate tracking tasks.",
}


# ---------------------------------------------------------------------------
# Read endpoints
# ---------------------------------------------------------------------------

@router.get("/", response_model=List[DatasetRead], status_code=200)
async def list_all_datasets(session: Session = Depends(get_session)):
    """Return all currently active tracked datasets."""
    return get_all_active_datasets(session)


@router.get("/{dataset_id}", response_model=DatasetRead, status_code=200)
async def get_dataset(dataset_id: UUID, session: Session = Depends(get_session)):
    """Return a single dataset by its ID."""
    return get_dataset_by_id(dataset_id, session)


# ---------------------------------------------------------------------------
# Register endpoints
# ---------------------------------------------------------------------------

@router.post("/", response_model=DatasetRead, status_code=201)
async def register_single_dataset(dataset: DatasetCreate, session: Session = Depends(get_session)):
    """Register a new RDF dataset and start tracking it immediately."""
    return register_dataset(dataset, session)


@router.post("/bulk", response_model=List[DatasetRead], status_code=201)
async def register_datasets_bulk(datasets: List[DatasetCreate], session: Session = Depends(get_session)):
    """Register multiple RDF datasets and start tracking each one immediately."""
    return [register_dataset(dataset, session) for dataset in datasets]


# ---------------------------------------------------------------------------
# Deactivate endpoints
# ---------------------------------------------------------------------------

@router.delete("/all", response_model=List[DatasetRead], status_code=200)
async def deactivate_all(session: Session = Depends(get_session)):
    """Deactivate tracking for all datasets."""
    return deactivate_all_datasets(session)


@router.delete("/{dataset_id}", response_model=DatasetRead, status_code=200)
async def deactivate_single_dataset(dataset_id: UUID, session: Session = Depends(get_session)):
    """Deactivate tracking for a single dataset by its ID."""
    return deactivate_dataset(dataset_id, session)
