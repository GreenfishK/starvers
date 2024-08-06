
from abc import ABC, abstractmethod
from typing import List, Tuple
import logging

import requests
from app.utils.graphdb.GraphDatabaseUtils import create_repository, delete_repository, getLoadSilentTemplate

LOG = logging.getLogger(__name__)

class DeltaCalculationService(ABC):
    @abstractmethod
    def prepare():
        pass

    @abstractmethod
    def calculate_delta():
        pass
    
    @abstractmethod
    def clean_up():
        pass

    @abstractmethod
    def load_rdf_data():
        pass

class IterativeDeltaQueryService(DeltaCalculationService):
    def prepare(self, tmp_name):
        create_repository(tmp_name)

    def calculate_delta(self, starvers, starvers_tmp):
        starvers_tmp.run_initial_versioning(self.load_rdf_data(starvers_tmp), None)
        
        new = starvers_tmp.get_latest_version()
        latest = starvers.get_latest_version()

        inserts = self.__calculate_delta(new, latest)
        deletions = self.__calculate_delta(latest, new)

        return inserts, deletions

    
    def clean_up(self, tmp_name):
        delete_repository(tmp_name)

    def load_rdf_data(self, starvers) -> str:
        LOG.debug(f"Start fetching from {starvers.rdf_store_url}")
        response = requests.get(starvers.rdf_store_url, headers={"Accept": "application/n-triples"})
        LOG.debug(f"Finished fetching from {starvers.rdf_store_url}")

        return response.text
    
    def __calculate_delta(self, triples1: List[Tuple], triples2: List[Tuple]) -> List[Tuple]:
        delta = []

        for t1 in triples1:
            for t2 in triples2:
                if t1[0] == t2[0] and t1[1] == t2[1] and t1[2] == t2[2]:
                    break
            else:
                delta.append(t1)

        return delta

class SparqlDeltaQueryService(DeltaCalculationService):
    def prepare(self, tmp_name):
        create_repository(tmp_name)

    def calculate_delta(self, starvers, starvers_tmp):
        self.load_rdf_data(starvers_tmp)
        inserts = None # run starvers_tmp minus starvers
        deletions = None # run starvers minus starvers_tmp

        return inserts, deletions

    
    def clean_up(self, tmp_name):
        delete_repository(tmp_name)

    def load_rdf_data(self, starvers):
        loadSilent = getLoadSilentTemplate(starvers.rdf_store_url)
        starvers.query(loadSilent)
        pass