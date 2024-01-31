from concurrent.futures import ThreadPoolExecutor
from sqlmodel import Session, select
from uuid import UUID
from typing import List
from datetime import datetime
import logging

from app.database import engine
from app.models.knowledge_graph import KnowledgeGraph, KnowledgeGraphCreate
from app.services import ScheduledThreadPoolExecutor
from app.utils.graphdb.graph_database import create_repository
from app.utils.exceptions.knowledge_graph_not_found_exception import KnowledgeGraphNotFoundException

LOG = logging.getLogger(__name__)

polling_executor: ScheduledThreadPoolExecutor.ScheduledThreadPoolExecutor = ScheduledThreadPoolExecutor.ScheduledThreadPoolExecutor(10)
polling_executor.start()

def get_all() -> List[KnowledgeGraph]:
    with Session(engine) as session:
        knowledgeGraphs = session.exec(select(KnowledgeGraph).where(KnowledgeGraph.active)).all()
        return knowledgeGraphs
    
def get_by_id(id: UUID) -> KnowledgeGraph:
    with Session(engine) as session:
        knowledgeGraph = session.get(KnowledgeGraph, id)

        if not knowledgeGraph:
            raise KnowledgeGraphNotFoundException(id=id)
        return knowledgeGraph

def add(knowledgeGraph: KnowledgeGraphCreate) -> List[KnowledgeGraph]:
    with Session(engine) as session:
        db_knowledge_graph = KnowledgeGraph.model_validate(knowledgeGraph)

        session.add(db_knowledge_graph)
        session.commit()
        session.refresh(db_knowledge_graph)

        #create repository for
        create_repository(db_knowledge_graph.name.replace(' ', '_')) # assume not existing ot at least is empty

        polling_executor.schedule_polling_at_fixed_rate(db_knowledge_graph.id, db_knowledge_graph.poll_interval)

        return db_knowledge_graph

def delete(id: UUID) -> KnowledgeGraph:
    with Session(engine) as session:    
        db_knowledge_graph = get_by_id(id)
        if (db_knowledge_graph.active):
            db_knowledge_graph.active = False
            db_knowledge_graph.last_modified_date = datetime.now()
            session.add(db_knowledge_graph)
            session.commit()
            session.refresh(db_knowledge_graph)
        return db_knowledge_graph

def update(knowledgeGraph: KnowledgeGraph) -> KnowledgeGraph:
    with Session(engine) as session:    
        db_knowledge_graph = get_by_id(knowledgeGraph.id)

        if (db_knowledge_graph.active):
            db_knowledge_graph = knowledgeGraph
            session.add(knowledgeGraph)
            session.commit()
            session.refresh(knowledgeGraph)
        return knowledgeGraph