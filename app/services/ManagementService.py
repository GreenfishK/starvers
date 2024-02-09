from sqlmodel import Session, select
from uuid import UUID
from typing import List
from datetime import datetime
import logging

from app.models.KnowledgeGraphModel import KnowledgeGraph, KnowledgeGraphCreate
from app.services import ScheduledThreadPoolExecutor
from app.utils.graphdb.GraphDatabaseUtils import create_repository
from app.utils.exceptions.GraphNotFoundException import KnowledgeGraphNotFoundException

LOG = logging.getLogger(__name__)

polling_executor: ScheduledThreadPoolExecutor.ScheduledThreadPoolExecutor = ScheduledThreadPoolExecutor.ScheduledThreadPoolExecutor(10)
polling_executor.start()

def get_all(session: Session) -> List[KnowledgeGraph]:
    knowledgeGraphs = session.exec(select(KnowledgeGraph).where(KnowledgeGraph.active)).all()
    return knowledgeGraphs
    
def get_by_id(id: UUID, session: Session) -> KnowledgeGraph:
    knowledgeGraph = session.get(KnowledgeGraph, id)

    if not knowledgeGraph:
        raise KnowledgeGraphNotFoundException(id=id)
    return knowledgeGraph

def add(knowledgeGraph: KnowledgeGraphCreate, session: Session) -> List[KnowledgeGraph]:
    db_knowledge_graph = KnowledgeGraph.model_validate(knowledgeGraph)

    session.add(db_knowledge_graph)
    session.commit()
    session.refresh(db_knowledge_graph)

    __start(db_knowledge_graph)

    return db_knowledge_graph

def delete(id: UUID, session: Session) -> KnowledgeGraph:
    db_knowledge_graph = get_by_id(id, session)
    if (db_knowledge_graph.active):
        db_knowledge_graph.active = False
        db_knowledge_graph.last_modified = datetime.now()
        session.add(db_knowledge_graph)
        session.commit()
        session.refresh(db_knowledge_graph)
    return db_knowledge_graph

def restart(session: Session):
    active_graphs = get_all(session)
    LOG.info(f'Restart {len(active_graphs)} active versioning task')
    for graph in active_graphs:
        __start(graph, False)
        pass

def __start(knowledgeGraph: KnowledgeGraph, initial_run=True):
    #create repository for
    create_repository(knowledgeGraph.name.replace(' ', '_'))

    polling_executor.schedule_polling_at_fixed_rate(knowledgeGraph.id, knowledgeGraph.poll_interval, initial_run=initial_run)
