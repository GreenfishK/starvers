from uuid import UUID

class DatasetNotFoundException(Exception):
    def __init__(self, id: UUID = None, name: str = None):
        self.id = id
        self.name = name