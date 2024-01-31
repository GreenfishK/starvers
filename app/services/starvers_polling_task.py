import time
from datetime import datetime
import logging
from uuid import UUID

LOG = logging.getLogger(__name__)

class StarVersPollingTask():
    def __init__(self, knowledge_graph_id: UUID, period: int, *args, **kwargs):
        super().__init__()
        self.knowledge_graph_id = knowledge_graph_id
        self.period = period
        self.args = args
        self.kwargs = kwargs
        self.__is_initial = True
        self.next_run = self.__get_time_ms()

    @property
    def is_initial_run(self) -> bool:
        return self.__is_initial
    
    @property
    def executor_ctx(self):
        return self.kwargs['executor_ctx']  # pragma: no cover

    def __get_next_run(self) -> int:
        return self.__get_time_ms() + self.period

    def set_next_run(self, time_taken: int = 0) -> None:
        self.__is_initial = False
        self.next_run = self.__get_next_run() - time_taken

    def __lt__(self, other) -> bool:
        return self.next_run < other.next_run

    def __repr__(self) -> str:
        return f"""(Polling Task, Periodic: {self.period} millisecond(ms), Next run: {time.ctime(self.next_run / 1000)})"""

    def __get_time_ms(self) -> int:
        return int(time.time_ns() / 1000000)

    def run(self):
        start_time = self.__get_time_ms()
        try:
            self.__run()
        except Exception as e:
            LOG.error(e)
        finally:
            end_time = self.__get_time_ms()
            time_taken = end_time - start_time
            
            self.set_next_run(time_taken)
            next_delay = self.period - time_taken
            if next_delay < 0 or self.next_run <= self.__get_time_ms():
                self.executor_ctx._put(self, 0)
            else:
                self.executor_ctx._put(self, next_delay)

    def __run(self):
        LOG.info(f"Start versioning task for knowledge graph with uuid={self.knowledge_graph_id}")

        from app.services.knowledge_graph_management import get_by_id
        knowledgeGraph = get_by_id(id=self.knowledge_graph_id)

        # retrieve dataset
        LOG.info(f"Start fetching from {knowledgeGraph.ressource_url}")
        #TODO fetch
        LOG.info(f"Finished fetching from {knowledgeGraph.ressource_url}")

        version_timestamp = datetime.now()

        LOG.info(f"Convert from rdf to rdf star")
        #TODO convert

        if (self.__is_initial): # if initial no diff is necessary
            LOG.info(f"Initial version for knowledge graph with uuid={self.knowledge_graph_id}")
            #TODO StarVers push

        else: # get diff to previous version using StarVers
            #TODO StarVers Diff calculation
            if (True):
                LOG.info(f"Changes for knowledge graph with uuid={self.knowledge_graph_id} detected")
            else:
                LOG.info(f"No changes for knowledge graph with uuid={self.knowledge_graph_id} detected")
                return
        
        knowledgeGraph.last_modified = version_timestamp

        from app.services.knowledge_graph_management import update
        update(knowledgeGraph)
        LOG.info(f"Finished versioning task for knowledge graph with uuid={self.knowledge_graph_id}")