import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Optional
from uuid import UUID
from datetime import datetime

from delayedqueue import DelayedQueue

from app.LoggingConfig import get_logger
from app.enums.DeltaTypeEnum import DeltaType
from app.services.PollingTask import PollingTask


LOG = get_logger(__name__)

class ScheduledThreadPoolExecutor(ThreadPoolExecutor):
    def __init__(self, max_workers=10, name=''):
        super().__init__(max_workers=max_workers, thread_name_prefix=name)
        self._max_workers = max_workers
        self.queue = DelayedQueue()
        self.shutdown = False


    def schedule_polling_at_fixed_rate(self, dataset_id: UUID, repository_name: str, latest_timestamp: datetime, period: int, initial_run: bool=True, *args, **kwargs) -> bool:
        if self.shutdown:
            raise RuntimeError(f"Repository name: {repository_name}: Cannot schedule new task after shutdown!")
        
        task = PollingTask(dataset_id, repository_name, latest_timestamp, period, *args, is_fixed_rate=True, time_func=self.queue.time_func, executor_ctx=self, is_initial=initial_run, **kwargs)
        return self._put(task, repository_name, 0)


    def _put(self, task: PollingTask, repository_name, delay: int) -> bool:
        if delay < 0:
            raise ValueError(f"Delay `{delay}` must be a non-negative number")
        is_scheduled = self.queue.put(task, delay)

        if is_scheduled:
            LOG.info(f"Repository name: {repository_name}: Task was put into queue.")
        else:
            LOG.warning(f"Repository name: {repository_name}: Task was not scheduled.")

        return is_scheduled
    

    def __run(self):
        while not self.shutdown:
            try:
                task: PollingTask = self.queue.get()
                future = super().submit(task.run)
                future.result()
            except Exception as e:
                print(e)


    def stop(self, wait_for_completion: Optional[bool] = True):
        self.shutdown = True
        super().shutdown(wait_for_completion)


    def start(self):
        t = threading.Thread(target=self.__run)
        t.daemon = True
        t.start()
