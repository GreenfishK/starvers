# module for used graph database
# replace marked sections with own code if necessary
import datetime
import requests
from functools import lru_cache
from app.AppConfig import Settings
import logging
from app.utils.exceptions.RepositoryCreationFailedException import GraphRepositoryCreationFailedException
from app.utils.exceptions.ServerFileImportFailedException import ServerFileImportFailedException


LOG = logging.getLogger(__name__)

DEFAULT_GRAPH_NAME = 'http://rdf4j.org/schema/rdf4j#nil'

# Implementation for Graph DB
def create_repository(repository_name: str): # add URL and description?
    repoConfig = __load_repo_config_file()
    repoConfig = repoConfig.replace('{:name}', repository_name)
    repoConfig = repoConfig.replace('{:description}', "Repository for versioned " + repository_name)

    LOG.info(f"Create graphdb repository with name {repository_name}")
    response = requests.post(f"{Settings().graph_db_url}/rest/repositories", files=dict(config=repoConfig))
    if (response.status_code != 201):
        if (response.text.find('already exists.') > -1):
            LOG.warning(f'[{response.status_code}] {response.text}')
        else:
            raise GraphRepositoryCreationFailedException(repository_name, response.text)
        
def import_serverfile(file_name: str, repository_name: str, graph_name: str = None):
    LOG.info(f"Load serverfile {file_name} into graphdb repository {repository_name}")
    payload = {
        "fileNames": [file_name],
        "context": None,
        "importSettings": {
            "replaceGraphs": False,
            "context": None
        }
    }

    if graph_name: #import into named graph
        payload["context"] = "http://example.org/" + graph_name
        payload["importSettings"]["context"] = "http://example.org/" + graph_name

    response = requests.post(f"{Settings().graph_db_url}/rest/repositories/{repository_name}/import/server", json=payload)
    if (response.status_code != 200):
        raise ServerFileImportFailedException(repository_name, response.text)


@lru_cache
def __load_repo_config_file() -> str:
    with open('app/utils/graphdb/repo-config.ttl', 'r') as f:
        return f.read()
    
def get_query_all_template(graph_name: str = None) -> str:
    if graph_name is None:
        with open('app/utils/graphdb/query_all.sparql', 'r') as f:
            return f.read()
    else:
        with open('app/utils/graphdb/query_all_from_graph.sparql', 'r') as f:
            template = f.read()
            template = template.replace('{:graph_name}', graph_name)
            return template
    
    
@lru_cache
def get_drop_graph_template(graph_name: str) -> str:
    with open('app/utils/graphdb/drop_graph.sparql', 'r') as f:
        template = f.read()
        template = template.replace('{:graph_name}', graph_name)
        return template
    
def get_delta_query_deletions_template(timestamp, graph_name: str) -> str:
    with open('app/utils/graphdb/delta_query_deletions.sparql', 'r') as f:
        template = f.read()
        template = template.replace('{:timestamp}', _versioning_timestamp_format(timestamp))
        template = template.replace('{:graph_name}', graph_name)
        return template
    
def get_delta_query_insertions_template(timestamp, graph_name: str) -> str:
    with open('app/utils/graphdb/delta_query_insertions.sparql', 'r') as f:
        template = f.read()
        template = template.replace('{:timestamp}', _versioning_timestamp_format(timestamp))
        template = template.replace('{:graph_name}', graph_name)
        return template
    
def _versioning_timestamp_format(timestamp: datetime) -> str:
    # TODO use same method as starvers library does
    if timestamp.strftime("%z") != '':
        return timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]  + timestamp.strftime("%z")[0:3] + ":" + timestamp.strftime("%z")[3:5]
    else:
        return timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]