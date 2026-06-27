# video_scheduler/core/threaded_worker.py
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from broccoli.core.task.task import Task
from broccoli.workers.base_worker import BaseWorker

logger = logging.getLogger(__name__)


class ThreadedWorker(BaseWorker):
    """Worker that processes multiple tasks concurrently using threads."""

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        worker_id: str = None,
        max_workers: int = 4,
    ):
        super().__init__(redis_url, worker_id)
        self.max_workers = max_workers
        self.executor = ThreadPoolExecutor(max_workers=max_workers)
        self.active_tasks = {}  # Track active tasks
        self.task_lock = threading.Lock()

    def process_task(self, task: Task) -> None:
        """Process a single task (runs in thread pool)."""
        try:
            logger.info(
                f"Thread {threading.current_thread().name} processing {task.task_id}"
            )

            # Pre-processing hook
            if not self.pre_process(task):
                logger.info(f"Task {task.task_id} skipped by pre_process")
                self._handle_task_result(task, False)
                return

            # Process the task
            success = self.process(task)

            # Update task status
            self._handle_task_result(task, success)

            # Post-processing hook
            self.post_process(task, success)

        except Exception as e:
            logger.error(f"Task {task.task_id} failed: {e}", exc_info=True)
            task.error = str(e)
            self._handle_task_result(task, False)
        finally:
            with self.task_lock:
                if task.task_id in self.active_tasks:
                    del self.active_tasks[task.task_id]

    def _handle_task_result(self, task: Task, success: bool) -> None:
        """Handle task result (same as BaseWorker)."""
        if success:
            task.status = "completed"
            task.progress = 100.0
        else:
            task.retries += 1
            if task.retries >= task.max_retries:
                task.status = "failed"
                if not task.error:
                    task.error = "Max retries exceeded"
            else:
                task.status = "pending"
                self.queue.requeue(task.task_id)
                logger.info(
                    f"Task {task.task_id} requeued (attempt {task.retries}/{task.max_retries})"
                )

        self._update_task(task)

    def start(self):
        """Start the threaded worker."""
        self.running = True
        logger.info(
            f"ThreadedWorker {self.worker_id} started with {self.max_workers} threads"
        )

        while self.running:
            try:
                # Check if we have capacity
                with self.task_lock:
                    active_count = len(self.active_tasks)

                if active_count >= self.max_workers:
                    time.sleep(0.1)
                    continue

                # Get task from queue
                task = self.queue.pop()
                if task is None:
                    time.sleep(0.1)
                    continue

                # Track active task
                with self.task_lock:
                    self.active_tasks[task.task_id] = task

                # Submit to thread pool
                self.executor.submit(self.process_task, task)
                logger.info(
                    f"Submitted task {task.task_id} to thread pool (active: {active_count + 1}/{self.max_workers})"
                )

            except Exception as e:
                logger.error(f"Worker error: {e}")
                time.sleep(1)

        # Shutdown thread pool
        self.executor.shutdown(wait=True)
        logger.info(f"ThreadedWorker {self.worker_id} stopped")
