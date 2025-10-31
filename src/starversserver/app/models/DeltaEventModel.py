from datetime import datetime
from typing import List
from uuid import UUID

from pydantic import BaseModel

class DeltaEvent(BaseModel):
    id: UUID
    repository_name: str
    totalInsertions: int
    totalDeletions: int
    insertions: List[str]
    deletions: List[str]
    versioning_duration_ms: int
    timestamp: datetime