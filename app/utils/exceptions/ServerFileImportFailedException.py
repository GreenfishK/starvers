class ServerFileImportFailedException(Exception):
    def __init__(self, repository_name: str, error: str):
        self.repository_name = repository_name
        self.error = error