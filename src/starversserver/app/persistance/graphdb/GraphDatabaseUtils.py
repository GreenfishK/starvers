# module for used graph database
# replace marked sections with own code if necessary
import datetime
import os
import time
import requests
from functools import lru_cache
from SPARQLWrapper import SPARQLWrapper, POST, DIGEST, CSV

from app.AppConfig import Settings
from app.LoggingConfig import get_logger
from app.exceptions.RepositoryCreationFailedException import GraphRepositoryCreationFailedException
from app.exceptions.ServerFileImportFailedException import ServerFileImportFailedException


DEFAULT_GRAPH_NAME = 'http://rdf4j.org/schema/rdf4j#nil'
BASE_GRAPH_URI = 'http://example.org/'
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_CONFIG_PATH = os.path.join(BASE_DIR, "repo-config.ttl")
QUERY_DIR =  os.path.join(BASE_DIR, 'queries')

def create_engine(repository_name: str, auth = DIGEST, method = POST, return_format = CSV, query_type = 'SELECT'):
    graph_db_get_endpoint = Settings().graph_db_url_get_endpoint.replace('{:repo_name}', repository_name)
    sparql_engine = SPARQLWrapper(graph_db_get_endpoint)
    sparql_engine.setHTTPAuth(auth)
    sparql_engine.setMethod(method)
    sparql_engine.setReturnFormat(return_format)
    # sparql.addCustomHttpHeader("Accept", "text/csv")
    sparql_engine.queryType = query_type

    return sparql_engine

# Implementation for Graph DB
def create_repository(repository_name: str): 
    repoConfig = __load_repo_config_file()
    repoConfig = repoConfig.replace('{:name}', repository_name)
    repoConfig = repoConfig.replace('{:description}', "Repository for versioned " + repository_name)

    get_logger(__name__).info(f"Repository name: {repository_name}: Create graphdb repository, if it does not exist.")
    response = requests.post(f"{Settings().graph_db_url}/rest/repositories", files=dict(config=repoConfig))
    
    if (response.status_code != 201):
        if (response.text.find('already exists.') > -1):
            get_logger(__name__).warning(f'[{response.status_code}] {response.text}')
        else:
            raise GraphRepositoryCreationFailedException(repository_name, response.text)
        

def recreate_repository(repository_name: str): 
    repoConfig = __load_repo_config_file()
    repoConfig = repoConfig.replace('{:name}', repository_name)
    repoConfig = repoConfig.replace('{:description}', "Repository for versioned " + repository_name)

    logger = get_logger(__name__)
    logger.info(f"Repository name: {repository_name}: Recreating GraphDB repository.")

    # Delete repository if it exists
    logger.info(f"Deleting GraphDB repository via REST API: {Settings().graph_db_url}/rest/repositories/{repository_name}")
    delete_response = requests.delete(f"{Settings().graph_db_url}/rest/repositories/{repository_name}")
    if delete_response.status_code in [200, 204]:
        logger.info(f"Repository {repository_name} deleted successfully.")
    elif delete_response.status_code == 404:
        logger.info(f"Repository {repository_name} did not exist.")
    else:
        logger.warning(f"Repository deletion for {repository_name} returned unexpected status: {delete_response.status_code} - {delete_response.text}")

    time.sleep(5)

    # Create repository
    response = requests.post(f"{Settings().graph_db_url}/rest/repositories", files=dict(config=repoConfig))
    
    if response.status_code == 201:
        logger.info(f"Repository {repository_name} created successfully.")
    else:
        logger.error(f"Repository creation for {repository_name} failed. Error code and message: {response.status_code} - {response.text}")

        
def import_serverfile(file_name: str, repository_name: str, graph_name: str = ""):
    get_logger(__name__,f"tracking_{repository_name}.log").info(f"Repository name: {repository_name}: Load serverfile {file_name} into graphdb repository.")
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
        get_logger(__name__,f"tracking_{repository_name}.log").error(f"Repository name: {repository_name}: Error loading serverfile {file_name} into graphdb repository with exception {response.text}")
        raise ServerFileImportFailedException(repository_name, response.text)
    

def poll_import_status(file_name: str, repository_name: str):
    get_logger(__name__,f"tracking_{repository_name}.log").info(f"Repository name: {repository_name}: Awaiting import of serverfile {file_name} into graphdb repository.")
    while True:
        try:
            response = requests.get(f"{Settings().graph_db_url}/rest/repositories/{repository_name}/import/server")
            response.raise_for_status()
            import_tasks = response.json()

            # Suche nach dem Task mit dem gewünschten Dateinamen
            task = next((t for t in import_tasks if t["name"] == file_name), None)

            if not task:
                get_logger(__name__,f"tracking_{repository_name}.log").error(f"Repository name: {repository_name}: Import for serverfile {file_name} not found!")
                raise ServerFileImportFailedException(repository_name, "Import not found!")

            status = task["status"]

            if status == "DONE":
                get_logger(__name__,f"tracking_{repository_name}.log").info(f"Repository name: {repository_name}: Import finished successfully.")
                delete_serverfile(file_name, repository_name)
                break
            elif status == "ERROR":
                get_logger(__name__,f"tracking_{repository_name}.log").error("Repository name: {repository_name}: Import failed: {e}")
                raise ServerFileImportFailedException(repository_name, task.get('message'))

        except Exception as e:
            get_logger(__name__,f"tracking_{repository_name}.log").error(f"Repository name: {repository_name}: Error while polling import status: {e}")
            delete_serverfile(file_name, repository_name)
            raise ServerFileImportFailedException(repository_name, f"Repository name: {repository_name}: Error while polling import status: {e}")

        time.sleep(1)


def delete_serverfile(file_name: str, repository_name: str):
    get_logger(__name__,f"tracking_{repository_name}.log").info(f"Repository name: {repository_name}: Remove serverfile {file_name}")
    import_path = f"/graphdb-import/{file_name}"
    os.remove(import_path)


@lru_cache
def __load_repo_config_file() -> str:
    with open(REPO_CONFIG_PATH, 'r') as f:
        return f.read()
    
def get_query_all_template(graph_name: str = "") -> str:
    with open(f'{QUERY_DIR}/query_all_from_graph.sparql', 'r') as f:
        template = f.read()
        template = template.replace('{:graph}', (BASE_GRAPH_URI + graph_name) if graph_name else DEFAULT_GRAPH_NAME )
        return template

def get_construct_all_versioned_template(timestamp: datetime.datetime, graph_name: str = "") -> str:
    with open(f'{QUERY_DIR}/construct_all_from_versioned_graph.sparql', 'r') as f:
        template = f.read()
        template = template.replace('{:timestamp}', _versioning_timestamp_format(timestamp))
        template = template.replace('{:graph}', (BASE_GRAPH_URI + graph_name) if graph_name else DEFAULT_GRAPH_NAME )
        return template       

def get_construct_all_template(graph_name: str = "") -> str:
    with open(f'{QUERY_DIR}/construct_all_from_graph.sparql', 'r') as f:
        template = f.read()
        template = template.replace('{:graph}', (BASE_GRAPH_URI + graph_name) if graph_name else DEFAULT_GRAPH_NAME )
        return template
    
def get_count_triples_template(timestamp: datetime.datetime, graph_name: str = "") -> str:
    with open(f'{QUERY_DIR}/count_triples.sparql', 'r') as f:
        template = f.read()
        template = template.replace('{:timestamp}', _versioning_timestamp_format(timestamp))
        template = template.replace('{:graph}', (BASE_GRAPH_URI + graph_name) if graph_name else DEFAULT_GRAPH_NAME )
        return template
    
@lru_cache
def get_drop_graph_template(graph_name: str) -> str:
    with open(f'{QUERY_DIR}/drop_graph.sparql', 'r') as f:
        template = f.read()
        template = template.replace('{:graph}', BASE_GRAPH_URI + graph_name)
        return template
    
def get_delta_query_deletions_template(timestamp: datetime.datetime, graph_name: str) -> str:
    with open(f'{QUERY_DIR}/delta_query_deletions.sparql', 'r') as f:
        template = f.read()
        template = template.replace('{:timestamp}', _versioning_timestamp_format(timestamp))
        template = template.replace('{:graph}', BASE_GRAPH_URI + graph_name)
        return template
    
def get_delta_query_insertions_template(timestamp: datetime.datetime, graph_name: str) -> str:
    with open(f'{QUERY_DIR}/delta_query_insertions.sparql', 'r') as f:
        template = f.read()
        template = template.replace('{:timestamp}', _versioning_timestamp_format(timestamp))
        template = template.replace('{:graph}', BASE_GRAPH_URI + graph_name)
        return template

def get_all_creation_timestamps() -> str:
    with open(f'{QUERY_DIR}/query_creation_timestamps.sparql', 'r') as f:
        template = f.read()
        return template

# Metric
def get_snapshot_classes_template(ts_current: datetime.datetime, ts_prev: datetime.datetime) -> str:
    with open(f'{QUERY_DIR}/query_snapshot_classes.sparql', 'r') as f:
        template = f.read()
        template = template.replace('{:ts_current}', _versioning_timestamp_format(ts_current))
        template = template.replace('{:ts_prev}', _versioning_timestamp_format(ts_prev))
        return template
        
# Metric
def get_snapshot_properties_template(ts_current: datetime.datetime, ts_prev: datetime.datetime, property_identifiers: str) -> str:
    with open(f'{QUERY_DIR}/query_snapshot_properties.sparql', 'r') as f:
        template = f.read()
        template = template.replace('{:ts_current}', _versioning_timestamp_format(ts_current))
        template = template.replace('{:ts_prev}', _versioning_timestamp_format(ts_prev))
        template = template.replace('{:property_identifiers}', property_identifiers)
        return template

# Metric
def get_dataset_static_core_template() -> str:
    with open(f'{QUERY_DIR}/query_static_core_triples.sparql', 'r') as f:
        template = f.read()
        return template

# Metric
def get_dataset_version_oblivious_template() -> str:
    with open(f'{QUERY_DIR}/query_version_oblivious_triples.sparql', 'r') as f:
        template = f.read()
        return template

def get_latest_update_ts_template() -> str:
    with open(f'{QUERY_DIR}/query_latest_update_ts.sparql', 'r') as f:
        template = f.read()
        return template

    
def _versioning_timestamp_format(timestamp: datetime.datetime) -> str:
    # TODO use same method as starvers library does
    if timestamp.strftime("%z") != '':
        return timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]  + timestamp.strftime("%z")[0:3] + ":" + timestamp.strftime("%z")[3:5]
    else:
        return timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]