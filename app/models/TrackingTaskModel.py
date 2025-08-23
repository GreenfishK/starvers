from uuid import UUID
from dataclasses import dataclass


@dataclass
class TrackingTaskDto:
    id: UUID
    name: str
    rdf_dataset_url: str

    def name_temp(self) -> str:
        return self.name + "_tmp_versioning"