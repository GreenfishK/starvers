from typing import List
from fastapi import APIRouter, Depends
from uuid import UUID

from sqlmodel import Session
from app.persistance.Database import get_session

from app.models.DatasetModel import DatasetRead, DatasetCreate
from app.services.ManagementService import delete_all, get_all, get_by_id, add, delete

tag = "management"

router = APIRouter(
    prefix="/management",
    tags=[tag]
)

tag_metadata = {
    "name": tag,
    "description": "Manage versionings of specific rdf dataset. This covers creating, reading, editing, and deleting versioning tasks.",
}

@router.get("/", response_model=List[DatasetRead], status_code=200)
async def get_all_rdf_datasets(session: Session = Depends(get_session)):
    return get_all(session)


@router.get("/{id}", response_model=DatasetRead, status_code=200)
async def get_rdf_dataset_by_id(id: UUID, session: Session = Depends(get_session)):
    return get_by_id(id, session)


@router.post("/", response_model=DatasetRead, status_code=201)
async def start_rdf_dataset_versioning(dataset: DatasetCreate, session: Session = Depends(get_session)):
    return add(dataset, session)

@router.post("/all", response_model=List[DatasetRead], status_code=201)
async def start_rdf_dataset_versioning_bulk(datasets: List[DatasetCreate], session: Session = Depends(get_session)):
    bulk = []
    for dataset in datasets:
        bulk.append(add(dataset, session))
    return bulk

@router.delete("/all", response_model=List[DatasetRead], status_code=200)
async def stop_all_rdf_dataset_versionings(session: Session = Depends(get_session)):
    return delete_all(session)

@router.delete("/{id}", response_model=DatasetRead, status_code=200)
async def stop_rdf_dataset_versioning_by_id(id: UUID, session: Session = Depends(get_session)):
    return delete(id, session)
