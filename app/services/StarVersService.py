from starvers.starvers import TripleStoreEngine
from SPARQLWrapper import SPARQLWrapper, POST, DIGEST, GET, JSON

from app.AppConfig import Settings
from app.utils.graphdb.GraphDatabaseUtils import loadInsertTemplate

class StarVersService():
    def __init__(self, repository_name: str) -> None:
        self.__repository_name = repository_name

        self.__graph_db_get_endpoint = Settings().graph_db_url_get_endpoint.replace('{:repo_name}', self.__repository_name)
        self.__graph_db_post_endpoint = Settings().graph_db_url_post_endpoint.replace('{:repo_name}', self.__repository_name)

        self.__starvers_engine = TripleStoreEngine(self.__graph_db_get_endpoint, self.__graph_db_post_endpoint)
        self.__sparql_wrapper = SPARQLWrapper(self.__graph_db_post_endpoint)
        self.__sparql_wrapper.setHTTPAuth(DIGEST)
        self.__sparql_wrapper.setMethod(POST)

    def push_initial_dataset(self, data: str):
        insert = loadInsertTemplate(data)
        self.__sparql_wrapper.setQuery(insert)
        self.__sparql_wrapper.query()

        self.__starvers_engine.version_all_triples()
        
        
