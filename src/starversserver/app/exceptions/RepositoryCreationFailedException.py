class GraphRepositoryCreationFailedException(Exception):
    def __init__(self, repository_name: str, error: str):
        self.reposotory_name = repository_name
        self.error = error