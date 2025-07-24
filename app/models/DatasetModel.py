from typing import Annotated, Optional
from pydantic import AfterValidator, HttpUrl
from sqlalchemy import Enum
from sqlmodel import AutoString, Field, SQLModel
from uuid import UUID, uuid4
from datetime import datetime

from app.enums.DeltaTypeEnum import DeltaType

HttpUrlString = Annotated[HttpUrl, AfterValidator(str)]


class DatasetBase(SQLModel):
    name: str
    repository_name: str
    rdf_dataset_url: HttpUrlString = Field(sa_type=AutoString)
    polling_interval: int # polling intervall in seconds
    delta_type: DeltaType = Field(sa_type=Enum(DeltaType), default=DeltaType.ITERATIVE)
    notification_webhook: Optional[HttpUrlString] = Field(sa_type=AutoString, default=None)
    created_at: Optional[datetime] = Field(default_factory=datetime.now)
    last_modified: Optional[datetime] = Field(default_factory=datetime.now)
    active: Optional[bool] = Field(default=True)
    next_run: Optional[datetime] = Field(default=None)

class Dataset(DatasetBase, table=True):
    id: Optional[UUID] = Field(default_factory=uuid4, primary_key=True)

class Snapshot(SQLModel, table=True):
    id: Optional[UUID] = Field(default_factory=uuid4, primary_key=True)
    dataset_id: UUID = Field(foreign_key="dataset.id")
    cnt_triples: int
    snapshot_ts: datetime = Field(default_factory=datetime.now)
    cnt_classes: int
    cnt_properties: int
    cnt_instances: int



class DatasetCreate(DatasetBase):
    pass


class DatasetRead(DatasetBase):
    id: UUID