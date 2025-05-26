# module for used graph database
# replace marked sections with own code if necessary
import datetime
import os
import time
import requests
from functools import lru_cache
from app.AppConfig import Settings
from app.LoggingConfig import get_logger, get_tracking_logger
from app.utils.exceptions.RepositoryCreationFailedException import GraphRepositoryCreationFailedException
from app.utils.exceptions.ServerFileImportFailedException import ServerFileImportFailedException


DEFAULT_GRAPH_NAME = 'http://rdf4j.org/schema/rdf4j#nil'
BASE_GRAPH_URI = 'http://example.org/'

# Implementation for Graph DB
def create_repository(repository_name: str): # add URL and description?
    repoConfig = __load_repo_config_file()
    repoConfig = repoConfig.replace('{:name}', repository_name)
    repoConfig = repoConfig.replace('{:description}', "Repository for versioned " + repository_name)

    get_logger(__name__).info(f"Create graphdb repository with name {repository_name}")
    response = requests.post(f"{Settings().graph_db_url}/rest/repositories", files=dict(config=repoConfig))
    if (response.status_code != 201):
        if (response.text.find('already exists.') > -1):
            get_logger(__name__).warning(f'[{response.status_code}] {response.text}')
        else:
            raise GraphRepositoryCreationFailedException(repository_name, response.text)
        
def import_serverfile(file_name: str, repository_name: str, graph_name: str = None):
    get_tracking_logger(repository_name).info(f"Load serverfile {file_name} into graphdb repository {repository_name}")
    payload = {
        "fileNames": [file_name],
        "importSettings": {
            "name": file_name,
            "replaceGraphs": ["default"],
            "context": ""
        }
    }

    if graph_name: #import into named graph
        payload["importSettings"]["context"] = "http://example.org/" + graph_name
        payload["importSettings"]["replaceGraphs"] = ["http://example.org/" + graph_name]

    response = requests.post(f"{Settings().graph_db_url}/rest/repositories/{repository_name}/import/server", json=payload)
    if (response.status_code != 202):
        get_tracking_logger(repository_name).error(f"Error loading serverfile {file_name} into graphdb repository {repository_name} with exception {response.text}")
        raise ServerFileImportFailedException(repository_name, response.text)
    

def poll_import_status(file_name: str, repository_name: str):
    get_tracking_logger(repository_name).info(f"Awaiting import of serverfile {file_name} into graphdb repository {repository_name}")
    while True:
        try:
            response = requests.get(f"{Settings().graph_db_url}/rest/repositories/{repository_name}/import/server")
            response.raise_for_status()
            import_tasks = response.json()

            # Suche nach dem Task mit dem gewünschten Dateinamen
            task = next((t for t in import_tasks if t["name"] == file_name), None)

            if not task:
                get_tracking_logger(repository_name).error(f"Import for serverfile {file_name} not found!")
                raise ServerFileImportFailedException(repository_name, "Import not found!")

            status = task["status"]

            if status == "DONE":
                get_tracking_logger(repository_name).info(f"Import for {repository_name} finished successfully.")
                delete_serverfile(file_name, repository_name)
                break
            elif status == "ERROR":
                get_tracking_logger(repository_name).error("Import for {repository_name} failed: {e}")
                raise ServerFileImportFailedException(repository_name, task.get('message'))

        except Exception as e:
            get_tracking_logger(repository_name).error("Error while polling import status: {e}")
            delete_serverfile(file_name, repository_name)
            raise ServerFileImportFailedException(repository_name, f"Error while polling import status: {e}")

        time.sleep(1)

def delete_serverfile(file_name: str, repository_name: str):
    get_tracking_logger(repository_name).info(f"Remove serverfile {file_name}")
    import_path = f"/graphdb-import/{file_name}"
    os.remove(import_path)

@lru_cache
def __load_repo_config_file() -> str:
    with open('app/utils/graphdb/repo-config.ttl', 'r') as f:
        return f.read()
    
def get_query_all_template(graph_name: str = None) -> str:
    with open('app/utils/graphdb/query_all_from_graph.sparql', 'r') as f:
        template = f.read()
        template = template.replace('{:graph}', (BASE_GRAPH_URI + graph_name) if graph_name else DEFAULT_GRAPH_NAME )
        return template

def get_construct_all_versioned_template(timestamp, graph_name: str = None) -> str:
    with open('app/utils/graphdb/construct_all_from_versioned_graph.sparql', 'r') as f:
        template = f.read()
        template = template.replace('{:timestamp}', _versioning_timestamp_format(timestamp))
        template = template.replace('{:graph}', (BASE_GRAPH_URI + graph_name) if graph_name else DEFAULT_GRAPH_NAME )
        return template       

def get_construct_all_template(graph_name: str = None) -> str:
    with open('app/utils/graphdb/construct_all_from_graph.sparql', 'r') as f:
        template = f.read()
        template = template.replace('{:graph}', (BASE_GRAPH_URI + graph_name) if graph_name else DEFAULT_GRAPH_NAME )
        return template
    
def get_count_triples_template(timestamp, graph_name: str = None) -> str:
    with open('app/utils/graphdb/count_triples.sparql', 'r') as f:
        template = f.read()
        template = template.replace('{:timestamp}', _versioning_timestamp_format(timestamp))
        template = template.replace('{:graph}', (BASE_GRAPH_URI + graph_name) if graph_name else DEFAULT_GRAPH_NAME )
        return template
    
@lru_cache
def get_drop_graph_template(graph_name: str) -> str:
    with open('app/utils/graphdb/drop_graph.sparql', 'r') as f:
        template = f.read()
        template = template.replace('{:graph}', BASE_GRAPH_URI + graph_name)
        return template
    
def get_delta_query_deletions_template(timestamp, graph_name: str) -> str:
    with open('app/utils/graphdb/delta_query_deletions.sparql', 'r') as f:
        template = f.read()
        template = template.replace('{:timestamp}', _versioning_timestamp_format(timestamp))
        template = template.replace('{:graph}', BASE_GRAPH_URI + graph_name)
        return template
    
def get_delta_query_insertions_template(timestamp, graph_name: str) -> str:
    with open('app/utils/graphdb/delta_query_insertions.sparql', 'r') as f:
        template = f.read()
        template = template.replace('{:timestamp}', _versioning_timestamp_format(timestamp))
        template = template.replace('{:graph}', BASE_GRAPH_URI + graph_name)
        return template
    
def _versioning_timestamp_format(timestamp: datetime) -> str:
    # TODO use same method as starvers library does
    if timestamp.strftime("%z") != '':
        return timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]  + timestamp.strftime("%z")[0:3] + ":" + timestamp.strftime("%z")[3:5]
    else:
        return timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]