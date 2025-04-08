from datetime import datetime
from typing import List

from pydantic import BaseModel

from app.models import TrackingTaskModel

class DeltaEvent(BaseModel):
    tracking_task: TrackingTaskModel

    totalInsertions: int
    totalDeletions: int

    insertions: List[str]
    deletions: List[str]

    versioning_duration_ms: int
    timestamp: datetime