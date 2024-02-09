from fastapi import APIRouter, Body, Query

from datetime import datetime
from typing import Annotated
from uuid import UUID

router = APIRouter(
    prefix="/query",
    tags=["query"]
)

@router.get("/{id}")
async def query_knowlegde_graph_by_id(
    id: UUID,
    query: Annotated[str, Body()],
    start_datetime: Annotated[datetime | None, Query()] = None,
    end_datetime: Annotated[datetime | None, Query()] = None
):
    return ""
