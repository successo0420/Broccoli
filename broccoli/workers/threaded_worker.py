# broccoli/workers/threaded_worker.py
from __future__ import annotations

import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import TimeoutError as FutureTimeoutError

import redis

from broccoli.core.task.task import Task
from broccoli.workers.base_worker import BaseWorker

logger = logging.getLogger(__name__)


class ThreadedWorker(BaseWorker):
    """
    Worker that processes multiple tasks concurrently using a thread pool.

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
        # Separate executor to run process() under a timeout guard. Kept
        # distinct from self.executor (which process_task itself runs on) to
        # avoid the deadlock risk of a task waiting on a future submitted to
        # the same bounded pool it's already occupying a slot in.
        self._timeout_executor = ThreadPoolExecutor(
            max_workers=max_workers, thread_name_prefix="timeout-guard"
        )

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

            # Only self.process() is allowed to fail the task via exception.
            # _handle_task_result / post_process must be allowed to propagate
            # their own errors to the outer except below, rather than being
            # re-invoked a second time on an already-transitioned task.
            try:
                future = self._timeout_executor.submit(self.process, task)
                try:
                    success = future.result(timeout=self.task_timeout)
                except FutureTimeoutError:
                    future.cancel()  # best-effort; thread keeps running if already started
                    logger.error(
                        f"Task {task.task_id} timed out after {self.task_timeout}s"
                    )
                    task.error = f"Task timed out after {self.task_timeout}s"
                    success = False
            except Exception as e:
                logger.error(f"Task {task.task_id} handler raised: {e}", exc_info=True)
                task.error = str(e)
                success = False

            # Central state machine (complete / requeue / fail) + _update_task
            self._handle_task_result(task, success)

            # Result storage and user-facing callbacks
            self.post_process(task, success)

        except Exception as e:
            # Reached only if _handle_task_result/post_process themselves
            # raised (e.g. a Redis error) — the task's queue state may be
            # inconsistent, so we log rather than re-running the state
            # machine on it a second time.
            logger.error(
                f"Task {task.task_id} failed outside handler: {e}", exc_info=True
            )
        finally:
            with self.task_lock:
                self.active_tasks.pop(task.task_id, None)

    def start(self):
        """Main loop: pop tasks and submit them to the thread pool."""
        self._register_signal_handlers()
        self.running = True
        logger.info(
            f"ThreadedWorker {self.worker_id} started (max_workers={self.max_workers})"
        )

        backoff = 1
        while self.running:
            try:
                with self.task_lock:
                    active_count = len(self.active_tasks)

                if active_count >= self.max_workers:
                    time.sleep(0.05)
                    continue

                task = self.queue.pop()
                backoff = 1  # reset after any successful Redis round-trip
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

            except redis.exceptions.RedisError as e:
                logger.error(
                    f"ThreadedWorker {self.worker_id} Redis error: {e}, "
                    f"retrying in {backoff}s",
                    exc_info=True,
                )
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
            except Exception as e:
                logger.error(
                    f"ThreadedWorker {self.worker_id} loop error: {e}", exc_info=True
                )
                time.sleep(1)

        self.executor.shutdown(wait=True)
        self._timeout_executor.shutdown(wait=False)
        logger.info(f"ThreadedWorker {self.worker_id} stopped")
