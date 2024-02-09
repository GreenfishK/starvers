from typing import List
from fastapi import APIRouter, Depends
from uuid import UUID

from sqlmodel import Session
from app.Database import get_session

from app.models.KnowledgeGraphModel import KnowledgeGraphRead, KnowledgeGraphCreate
from app.services.ManagementService import get_all, get_by_id, add, delete

router = APIRouter(
    prefix="/management",
    tags=["management"]
)

@router.get("/", response_model=List[KnowledgeGraphRead], status_code=200)
async def get_all_knowledge_graphs(session: Session = Depends(get_session)):
    return get_all(session)


@router.get("/{id}", response_model=KnowledgeGraphRead, status_code=200)
async def get_knowledge_graph_by_id(id: UUID, session: Session = Depends(get_session)):
    return get_by_id(id, session)


@router.post("/", response_model=KnowledgeGraphRead, status_code=201)
async def start_knowledge_graph_versioning(knowledgeGraph: KnowledgeGraphCreate, session: Session = Depends(get_session)):
    return add(knowledgeGraph, session)

@router.delete("/{id}", status_code=200)
async def stop_knowledge_graph_versioning_by_id(id: UUID, session: Session = Depends(get_session)):
    return delete(id, session)
