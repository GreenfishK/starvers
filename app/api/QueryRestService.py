import logging
from fastapi import APIRouter, Body, Depends, Query

from datetime import datetime
from typing import Annotated
from uuid import UUID

from pytest import Session

from app.Database import get_session
from app.services.StarVersService import StarVersService
from app.services.ManagementService import get_by_id

router = APIRouter(
    prefix="/query",
    tags=["query"]
)

@router.get("/{id}")
async def query_knowlegde_graph_by_id(
    id: UUID,
    query: Annotated[str, Body()],
    timestamp: Annotated[datetime | None, Query()] = None,
    query_as_timestamped: Annotated[bool | None, Query()] = True,
    session: Session = Depends(get_session)):

    kg =  get_by_id(id, session)
    starvers = StarVersService(kg.repository_name, kg.id)

    return starvers.query(query, timestamp, query_as_timestamped)
