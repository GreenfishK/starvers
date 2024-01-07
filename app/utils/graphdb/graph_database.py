# module for used graph database
# replace marked sections with own code if necessary
import requests
from functools import lru_cache
from app.app_config import Settings
import logging

LOG = logging.getLogger(__name__)

# Implementation for Graph DB
def create_repository(name: str): # add URL and description?
    repoConfig = __loadRepoConfigFile()
    repoConfig = repoConfig.replace('{:name}', name)
    repoConfig = repoConfig.replace('{:description}', "Repository for versioned {name}")

    LOG.info(f"Create graphdb repository with name {name}")
    response = requests.post(f"{Settings().graph_db_url}/rest/repositories", files=dict(config=repoConfig))
    if (response.status_code != 201):
        LOG.error(f'[{response.status_code}] {response.text}')

@lru_cache
def __loadRepoConfigFile():
    with open('app/utils/graphdb/repo-config.ttl', 'r') as f:
        return f.read()
