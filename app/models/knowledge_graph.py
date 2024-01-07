from typing import Optional
from sqlmodel import Field, SQLModel
from uuid import UUID, uuid4
from datetime import datetime

class KnowledgeGraphBase(SQLModel):
    name: str;
    ressource_url: str;
    poll_intervall: int;
    created_date: Optional[datetime] = Field(default=datetime.now());
    last_modified_date: Optional[datetime] = Field(default=datetime.now());
    active: Optional[bool] = Field(default=True);

class KnowledgeGraph(KnowledgeGraphBase, table=True):
    id: Optional[UUID] = Field(default_factory=uuid4, primary_key=True);


class KnowledgeGraphCreate(KnowledgeGraphBase):
    pass


class KnowledgeGraphRead(KnowledgeGraphBase):
    id: UUID