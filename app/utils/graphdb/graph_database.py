# module for used graph database
# replace marked sections with own code if necessary
import requests
from functools import lru_cache
from app.app_config import Settings
import logging
from app.utils.exceptions.graph_repository_creation_failed_exception import GraphRepositoryCreationFailedException


LOG = logging.getLogger(__name__)

# Implementation for Graph DB
def create_repository(name: str): # add URL and description?
    repoConfig = __loadRepoConfigFile()
    repoConfig = repoConfig.replace('{:name}', name)
    repoConfig = repoConfig.replace('{:description}', "Repository for versioned {name}")

    LOG.info(f"Create graphdb repository with name {name}")
    response = requests.post(f"{Settings().graph_db_url}/rest/repositories", files=dict(config=repoConfig))
    if (response.status_code != 201):
        if (response.text.find('already exists.') > -1):
            LOG.warning(f'[{response.status_code}] {response.text}')
        else:
            raise GraphRepositoryCreationFailedException(name, response.text);

@lru_cache
def __loadRepoConfigFile():
    with open('app/utils/graphdb/repo-config.ttl', 'r') as f:
        return f.read()
