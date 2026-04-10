"""
query_router.py

REST endpoints for executing SPARQL queries against versioned RDF datasets.
Supports querying the latest version or any past snapshot via an optional timestamp.
"""

from datetime import datetime
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Body, Depends, Query
from sqlmodel import Session

from app.persistance.Database import get_session
from app.models.TrackingTaskModel import TrackingTaskDto
from app.services.dataset_repository import get_dataset_by_id
from app.services.versioning_pipeline import VersioningPipeline

tag = "query"

router = APIRouter(prefix="/query", tags=[tag])

tag_metadata = {
    "name": tag,
    "description": "Execute SPARQL queries against versioned datasets — at the latest version or at a specific point in time.",
}


@router.get("/{dataset_id}")
async def query_dataset(
    dataset_id: UUID,
    query: Annotated[str, Body()],
    timestamp: Annotated[datetime | None, Query()] = None,
    query_as_timestamped: Annotated[bool, Query()] = True,
    session: Session = Depends(get_session),
):
    """
    Execute a SPARQL query against a tracked dataset.

    - **timestamp**: if provided, the query is evaluated against the snapshot at that point in time.
    - **query_as_timestamped**: when False the query runs without time-bounding (returns the raw graph).
    """
    dataset = get_dataset_by_id(dataset_id, session)

    tracking_task = TrackingTaskDto(
        id=dataset.id,
        name=dataset.repository_name,
        rdf_dataset_url=dataset.rdf_dataset_url,
    )
    pipeline = VersioningPipeline(tracking_task, dataset.repository_name)

    return pipeline.query(query, timestamp, query_as_timestamped)
