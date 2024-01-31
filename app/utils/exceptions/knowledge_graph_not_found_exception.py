from uuid import UUID

class KnowledgeGraphNotFoundException(Exception):
    def __init__(self, id: UUID):
        self.id = id