import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Optional
from uuid import UUID

from delayedqueue import DelayedQueue

from app.enums.DeltaTypeEnum import DeltaType
from app.services.PollingTask import PollingTask


LOG = logging.getLogger(__name__)

class ScheduledThreadPoolExecutor(ThreadPoolExecutor):
    def __init__(self, max_workers=10, name=''):
        super().__init__(max_workers=max_workers, thread_name_prefix=name)
        self._max_workers = max_workers
        self.queue = DelayedQueue()

        self.shutdown = False

    def schedule_polling_at_fixed_rate(self, dataset_id: UUID, period: int, delta_type: DeltaType, *args, initial_run=True, **kwargs) -> bool:
        if self.shutdown:
            raise RuntimeError("Cannot schedule new task after shutdown!")
        
        task = PollingTask(dataset_id, period, delta_type, *args, is_fixed_rate=True, time_func=self.queue.time_func, executor_ctx=self, is_initial=initial_run, **kwargs)
        return self._put(task, 0)

    def _put(self, task: PollingTask, delay: int) -> bool:
        if delay < 0:
            raise ValueError(f"Delay `{delay}` must be a non-negative number")
        LOG.info(f"Enqueuing {task} with delay of {delay}")
        return self.queue.put(task, delay)
    

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
