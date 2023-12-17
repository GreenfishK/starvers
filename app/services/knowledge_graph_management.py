from sqlmodel import Session, select
from uuid import UUID
from typing import List
from datetime import datetime

from app.database import engine
from app.models.knowledge_graph import KnowledgeGraph, KnowledgeGraphCreate

class KnowledgeGraphNotFoundException(Exception):
    def __init__(self, id: UUID):
        self.id = id


class KnowledgeGraphManagement():
    def ___init__(self):
        pass

    def get_all(self) -> List[KnowledgeGraph]:
        with Session(engine) as session:
            knowledgeGraphs = session.exec(select(KnowledgeGraph).where(KnowledgeGraph.active)).all()
            return knowledgeGraphs
        
    def get_by_id(self, id: UUID) -> KnowledgeGraph:
        with Session(engine) as session:
            knowledgeGraph = session.get(KnowledgeGraph, id)

            if not knowledgeGraph:
                raise KnowledgeGraphNotFoundException(id=id)
            return knowledgeGraph

    def add(self, knowledgeGraph: KnowledgeGraphCreate) -> List[KnowledgeGraph]:
        with Session(engine) as session:
            db_knowledge_graph = KnowledgeGraph.model_validate(knowledgeGraph)
            session.add(db_knowledge_graph)
            session.commit()
            session.refresh(db_knowledge_graph)
            return db_knowledge_graph

    def delete(self, id: UUID) -> KnowledgeGraph:
        with Session(engine) as session:    
            db_knowledge_graph = self.get_by_id(id)
            if (db_knowledge_graph.active):
                db_knowledge_graph.active = False
                db_knowledge_graph.last_modified_date = datetime.now()
                session.add(db_knowledge_graph)
                session.commit()
                session.refresh(db_knowledge_graph)
            return db_knowledge_graph