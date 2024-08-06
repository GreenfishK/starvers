from uuid import UUID

from pydantic import BaseModel

class DeltaEvent(BaseModel):
    id: UUID
    name: str
    repository_name: str

    inserts: int
    deletions: int