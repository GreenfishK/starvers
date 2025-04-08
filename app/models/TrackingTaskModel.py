from uuid import UUID
from dataclasses import dataclass

from app.enums.DeltaTypeEnum import DeltaType

@dataclass
class TrackingTaskDto:
    id: UUID
    name: str
    rdf_dataset_url: str
    delta_type: DeltaType

    def name_temp(self) -> str:
        return self.name + "_tmp_versioning"