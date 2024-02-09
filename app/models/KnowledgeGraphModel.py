from typing import Optional
from sqlmodel import Field, SQLModel
from uuid import UUID, uuid4
from datetime import datetime

class KnowledgeGraphBase(SQLModel):
    name: str
    ressource_url: str
    poll_interval: int # polling intervall in seconds
    created_at: Optional[datetime] = Field(default_factory=datetime.now)
    last_modified: Optional[datetime] = Field(default_factory=datetime.now)
    active: Optional[bool] = Field(default=True)

class KnowledgeGraph(KnowledgeGraphBase, table=True):
    id: Optional[UUID] = Field(default_factory=uuid4, primary_key=True)


class KnowledgeGraphCreate(KnowledgeGraphBase):
    pass


class KnowledgeGraphRead(KnowledgeGraphBase):
    id: UUID