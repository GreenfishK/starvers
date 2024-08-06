from typing import Optional
from pydantic_core import Url
from sqlmodel import AutoString, Field, SQLModel
from uuid import UUID, uuid4
from datetime import datetime

from app.enums.DeltaQueryEnum import DeltaQuery

class KnowledgeGraphBase(SQLModel):
    name: str
    repository_name: str

    rdf_store_url: Url = Field(sa_type=AutoString)
    polling_interval: int # polling intervall in seconds
    delta_type: int = Field(default=DeltaQuery.ITERATIVE)

    notification_webhook: Optional[Url] = Field(sa_type=AutoString)

    created_at: Optional[datetime] = Field(default_factory=datetime.now)
    last_modified: Optional[datetime] = Field(default_factory=datetime.now)
    active: Optional[bool] = Field(default=True)

class KnowledgeGraph(KnowledgeGraphBase, table=True):
    id: Optional[UUID] = Field(default_factory=uuid4, primary_key=True)


class KnowledgeGraphCreate(KnowledgeGraphBase):
    pass


class KnowledgeGraphRead(KnowledgeGraphBase):
    id: UUID