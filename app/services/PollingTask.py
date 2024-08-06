import time
from datetime import datetime
import logging
from uuid import UUID

import requests
from sqlalchemy import Boolean

from app.Database import Session, engine
from app.enums.DeltaQueryEnum import DeltaQuery
from app.services.VersioningService import StarVersService

LOG = logging.getLogger(__name__)

class PollingTask():
    def __init__(self, knowledge_graph_id: UUID, period: int, delta_query_type: DeltaQuery = DeltaQuery.ITERATIVE, *args, time_func=time.time, is_initial=True, **kwargs):
        super().__init__()
        self.knowledge_graph_id = knowledge_graph_id
        self.period = period
        self.args = args
        self.kwargs = kwargs
        self.__stopped = False
        self.__time_func = time_func
        self.__is_initial = is_initial
        self.next_run = self.__time_func()
        self.__starvers = None
        self.__delta_query_type = delta_query_type

    @property
    def is_initial_run(self) -> bool:
        return self.__is_initial
    
    @property
    def executor_ctx(self):
        return self.kwargs['executor_ctx']
    
    @property
    def time_func(self):
        return self.__time_func

    def __get_next_run(self) -> int:
        return self.__time_func() + self.period

    def set_next_run(self, time_taken: int = 0) -> None:
        self.__is_initial = False
        self.next_run = self.__get_next_run() - time_taken

    def __lt__(self, other) -> bool:
        return self.next_run < other.next_run

    def __repr__(self) -> str:
        return f"(Polling Task, Periodic: {self.period} seconds (s), Next run: {time.ctime(self.next_run)})"

    def run(self):
        start_time = time.time_ns()
        try:
            with Session(engine) as session:
                self.__stopped = self.__run(session)
        except Exception as e:
            LOG.error(e)
        finally:
            if self.__stopped:
                LOG.info(f'Stopped versioning for knowledge graph with uuid={self.knowledge_graph_id}!')
                return
            
            end_time = time.time_ns()
            time_taken = (end_time - start_time) / 1_000_000_000 # convert ns to s
            
            self.set_next_run(time_taken)
            next_delay = self.period - time_taken
            if next_delay < 0 or self.next_run <= self.__time_func():
                self.executor_ctx._put(self, 0)
            else:
                self.executor_ctx._put(self, next_delay)

    def __run(self, session: Session) -> Boolean:
        LOG.info(f"Start versioning task for knowledge graph with uuid={self.knowledge_graph_id}")

        from app.services.ManagementService import get_by_id
        knowledgeGraph = get_by_id(id=self.knowledge_graph_id, session=session)

        if not knowledgeGraph.active:
            return True

        version_timestamp = datetime.now()

        if self.__starvers is None:
            self.__starvers = StarVersService(knowledgeGraph.repository_name, knowledgeGraph.id, knowledgeGraph.rdf_store_url, self.__delta_query_type)

        if (self.__is_initial): # if initial no diff is necessary
            LOG.info(f"Initial version for knowledge graph with uuid={self.knowledge_graph_id}")

            # retrieve initial rdf data via http
            # #TODO move to versioning service????
            LOG.info(f"Start fetching from {knowledgeGraph.rdf_store_url}")
            response = requests.get(knowledgeGraph.rdf_store_url, headers={"Accept": "application/n-triples"})
            LOG.info(f"Finished fetching from {knowledgeGraph.rdf_store_url}")

            # push triples into repository
            self.__starvers.run_initial_versioning(response.text, version_timestamp)

        else: # get diff to previous version using StarVers
            changes = self.__starvers.run_versioning(version_timestamp)

            if (changes):
                LOG.info(f"Changes for knowledge graph with uuid={self.knowledge_graph_id} detected")
            else:
                LOG.info(f"No changes for knowledge graph with uuid={self.knowledge_graph_id} detected")
                return
        
        knowledgeGraph.last_modified = version_timestamp
        session.commit()
        session.refresh(knowledgeGraph)

        LOG.info(f"Finished versioning task for knowledge graph with uuid={self.knowledge_graph_id}")

        return False
