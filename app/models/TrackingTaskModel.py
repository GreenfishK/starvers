import dataclasses
from uuid import UUID

from app.enums.DeltaTypeEnum import DeltaType

@dataclasses.dataclass
class TrackingTaskDto:
    id: UUID
    name: str
    rdf_dataset_url: str
    delta_type: DeltaType

    def name_temp(self) -> str:
        return self.name + "_tmp_versioning"
