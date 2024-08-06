from typing import List
from fastapi import APIRouter, Depends
from uuid import UUID

from sqlmodel import Session
from app.Database import get_session

from app.models.KnowledgeGraphModel import KnowledgeGraphRead, KnowledgeGraphCreate
from app.services.ManagementService import delete_all, get_all, get_by_id, add, delete

tag = "management"

router = APIRouter(
    prefix="/management",
    tags=[tag]
)

tag_metadata = {
    "name": tag,
    "description": "Manage versionings of specific knowledge graph. This covers creating, reading, editing, and deleting versioning tasks.",
}

@router.get("/", response_model=List[KnowledgeGraphRead], status_code=200)
async def get_all_knowledge_graphs(session: Session = Depends(get_session)):
    return get_all(session)


@router.get("/{id}", response_model=KnowledgeGraphRead, status_code=200)
async def get_knowledge_graph_by_id(id: UUID, session: Session = Depends(get_session)):
    return get_by_id(id, session)


@router.post("/", response_model=KnowledgeGraphRead, status_code=201)
async def start_knowledge_graph_versioning(knowledgeGraph: KnowledgeGraphCreate, session: Session = Depends(get_session)):
    return add(knowledgeGraph, session)

@router.delete("/all", response_model=List[KnowledgeGraphRead], status_code=200)
async def stop_all_knowledge_graph_versionings(session: Session = Depends(get_session)):
    return delete_all(session)

@router.delete("/{id}", response_model=KnowledgeGraphRead, status_code=200)
async def stop_knowledge_graph_versioning_by_id(id: UUID, session: Session = Depends(get_session)):
    return delete(id, session)
