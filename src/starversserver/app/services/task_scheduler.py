"""
task_scheduler.py

A fixed-rate scheduler built on top of ThreadPoolExecutor.
Maintains a DelayedQueue of PollingTasks and dispatches them to worker threads.
"""

import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Optional
from uuid import UUID
from datetime import datetime

from delayedqueue import DelayedQueue

from app.LoggingConfig import get_logger
from app.services.polling_task import PollingTask

LOG = get_logger(__name__)


class TaskScheduler(ThreadPoolExecutor):
    """
    Wraps ThreadPoolExecutor with a DelayedQueue so that PollingTasks
    can be scheduled with an initial delay and then re-queued at a fixed rate.
    """

    def __init__(self, max_workers: int = 10):
        super().__init__(max_workers=max_workers)
        self.queue = DelayedQueue()
        self._shutdown = False

    # ---------------------------------------------------------------------------
    # Public API: schedule and stop
    # ---------------------------------------------------------------------------

    def schedule(
        self,
        dataset_id: UUID,
        repository_name: str,
        latest_timestamp: datetime,
        poll_interval_seconds: int,
        initial_run: bool = True,
    ) -> bool:
        """Create a PollingTask and place it in the queue with no initial delay."""
        if self._shutdown:
            raise RuntimeError(f"Cannot schedule task for '{repository_name}': scheduler is shut down.")

        task = PollingTask(
            dataset_id=dataset_id,
            repository_name=repository_name,
            latest_timestamp=latest_timestamp,
            period=poll_interval_seconds,
            is_initial=initial_run,
            time_func=self.queue.time_func,
            executor_ctx=self,
        )
        return self._enqueue(task, repository_name, delay=0)

    def start(self):
        """Start the background dispatcher thread."""
        dispatcher = threading.Thread(target=self._dispatch_loop, daemon=True)
        dispatcher.start()

    def stop(self, wait_for_completion: Optional[bool] = True):
        """Shut down the scheduler and optionally wait for running tasks to finish."""
        self._shutdown = True
        super().shutdown(wait=wait_for_completion)

    # ---------------------------------------------------------------------------
    # Internal: queue management and dispatch loop
    # ---------------------------------------------------------------------------

    def _enqueue(self, task: PollingTask, repository_name: str, delay: int) -> bool:
        """Place a task into the delayed queue. Returns True if successfully queued."""
        if delay < 0:
            raise ValueError(f"Delay must be non-negative, got: {delay}")

        queued = self.queue.put(task, delay)

        if queued:
            LOG.info(f"[{repository_name}] Task queued with delay={delay}s.")
        else:
            LOG.warning(f"[{repository_name}] Task could not be queued.")

        return queued

    def _dispatch_loop(self):
        """Continuously pull tasks from the queue and submit them to the thread pool."""
        while not self._shutdown:
            try:
                task: PollingTask = self.queue.get()
                future = super().submit(task.run)
                future.result()  # propagate exceptions from the task
            except Exception as e:
                LOG.error(f"Dispatcher encountered an error: {e}")


# ---------------------------------------------------------------------------
# Module-level singleton used across the application
# ---------------------------------------------------------------------------

scheduler = TaskScheduler(max_workers=10)
scheduler.start()
