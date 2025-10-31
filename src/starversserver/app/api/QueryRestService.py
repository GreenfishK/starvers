from fastapi import APIRouter, Body, Depends, Query

from datetime import datetime
from typing import Annotated
from uuid import UUID

from sqlmodel import Session

from app.persistance.Database import get_session
from app.models.TrackingTaskModel import TrackingTaskDto
from app.services.VersioningService import StarVersService
from app.services.ManagementService import get_by_id

tag = "query"

router = APIRouter(
    prefix="/query",
    tags=[tag]
)

tag_metadata = {
    "name": tag,
    "description": "Perform queries against versioned datasets - latest version or at a certain point in the past",
}

@router.get("/{id}")
async def query_knowlegde_graph_by_id(
    id: UUID, 
    query: Annotated[str, Body()],
    timestamp: Annotated[datetime | None, Query()] = None, 
    query_as_timestamped: Annotated[bool, Query()] = True,
    session: Session = Depends(get_session)):

    dataset = get_by_id(id, session) 

    tracking_task = TrackingTaskDto(
        id=dataset.id,
        name=dataset.repository_name,
        rdf_dataset_url=dataset.rdf_dataset_url)
    starvers = StarVersService(tracking_task, dataset.repository_name)

    return starvers.query(query, timestamp, query_as_timestamped)
