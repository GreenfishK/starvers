from uuid import UUID

class DatasetNotFoundException(Exception):
    def __init__(self, id: UUID):
        self.id = id