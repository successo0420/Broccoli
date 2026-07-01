# broccoli/workers/threaded_worker.py
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from broccoli.core.task.task import Task
from broccoli.workers.base_worker import BaseWorker

logger = logging.getLogger(__name__)


class ThreadedWorker(BaseWorker):
    """
    Worker that processes multiple tasks concurrently using a thread pool.

    ``_handle_task_result`` is inherited from ``BaseWorker`` — retry logic,
    dependency release, and queue-set transitions all live in one place.
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        worker_id: str = None,
        max_workers: int = 4,
    ):
        super().__init__(redis_url, worker_id)
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.active_tasks: dict[str, Task] = {}
        self.task_lock = threading.Lock()

    def process_task(self, task: Task) -> None:
        """
        Process one task inside a thread-pool thread.

        Uses the inherited ``_handle_task_result`` for all state transitions,
        then calls ``post_process`` for result storage and user callbacks.
        """
        try:
            logger.info(
                f"Thread {threading.current_thread().name} "
                f"processing task {task.task_id}"
            )

            if not self.pre_process(task):
                logger.info(f"Task {task.task_id} skipped by pre_process")
                self.queue.fail(task)  # remove from processing set
                return

            success = self.process(task)

            # Central state machine (complete / requeue / fail) + _update_task
            self._handle_task_result(task, success)

            # Result storage and user-facing callbacks
            self.post_process(task, success)

        except Exception as e:
            logger.error(f"Task {task.task_id} failed: {e}", exc_info=True)
            task.error = str(e)
            self._handle_task_result(task, False)
            self.post_process(task, False)
        finally:
            with self.task_lock:
                self.active_tasks.pop(task.task_id, None)

    def start(self):
        """Main loop: pop tasks and submit them to the thread pool."""
        self.running = True
        logger.info(
            f"ThreadedWorker {self.worker_id} started (max_workers={self.max_workers})"
        )

        while self.running:
            try:
                with self.task_lock:
                    active_count = len(self.active_tasks)

                if active_count >= self.max_workers:
                    time.sleep(0.05)
                    continue

                task = self.queue.pop()
                if task is None:
                    time.sleep(0.05)
                    continue

                with self.task_lock:
                    self.active_tasks[task.task_id] = task

                self.executor.submit(self.process_task, task)
                logger.info(
                    f"Submitted {task.task_id} to thread pool "
                    f"(active: {active_count + 1}/{self.max_workers})"
                )

            except Exception as e:
                logger.error(
                    f"ThreadedWorker {self.worker_id} loop error: {e}", exc_info=True
                )
                time.sleep(1)

        self.executor.shutdown(wait=True)
        logger.info(f"ThreadedWorker {self.worker_id} stopped")
