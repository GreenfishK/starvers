from uuid import UUID
from pydantic import dataclasses

from app.enums.DeltaTypeEnum import DeltaType

@dataclasses.dataclass(config=dict(arbitrary_types_allowed=True))
class TrackingTaskDto:
    id: UUID
    name: str
    rdf_dataset_url: str
    delta_type: DeltaType

    def name_temp(self) -> str:
        return self.name + "_tmp_versioning"