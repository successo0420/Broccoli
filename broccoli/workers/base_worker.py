# video_scheduler/core/worker.py
import logging
import time
from abc import ABC
from datetime import datetime

from broccoli.core.redis_controller import RedisController
from broccoli.core.result import ResultBackend
from broccoli.core.task.task import Task
from broccoli.core.task.task_queue import TaskQueue
from broccoli.core.task.task_registry import TaskRegistry

logger = logging.getLogger(__name__)


class BaseWorker(ABC):
    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        worker_id: str = None,
        queue_name: str = "tasks:queue",
        task_prefix: str = "task",
    ):
        self.redis_url = redis_url
        self._redis = RedisController(redis_url).get_client()
        self.queue = TaskQueue(
            queue_name=queue_name, redis_url=redis_url, task_prefix=task_prefix
        )
        self.registry = TaskRegistry()
        self.running = False
        self.worker_id = worker_id or f"worker-{id(self)}"
        self.task_timeout = 3600
        self.result = ResultBackend(redis_url)

    def pre_process(self, task: Task) -> bool:
        """Hook to run before processing. Override this in your custom worker."""
        # Default implementation - just return True
        logger.info(f"Pre-processing task {task.task_id} ({task.task_type})")
        return True

    def post_process(self, task: Task, success: bool) -> None:
        """Hook to run after processing. Override this in your custom worker."""
        self.result.store_task(task)
        self._redis.delete(f"task:{task.task_id}")  # Clean up task data from Redis
        logger.info(f"Task {task.task_id} {task.status} with result: {task.result}")

    def process(self, task: Task) -> bool:
        """Process a single task using the registered handler."""
        try:
            handler = self.registry.get_handler(task.task_type)
            if not handler:
                task.error = f"No handler registered for task type: {task.task_type}"
                logger.error(task.error)
                return False

            result = handler(task.payload)
            task.result = result
            return True

        except Exception as e:
            task.error = str(e)
            logger.error(f"Task {task.task_id} failed: {e}", exc_info=True)
            return False

    def _update_task(self, task: Task) -> None:
        task.updated_at = datetime.now().isoformat()
        self._redis.hset(f"task:{task.task_id}", mapping=task.to_dict())

    def start(self):
        self.running = True

        logger.info(f"Worker {self.worker_id} started")

        while self.running:
            try:
                task = self.queue.pop()
                if task is None:
                    continue

                logger.info(
                    f"Worker {self.worker_id} processing task {task.task_id} ({task.task_type})"
                )

                # Pre-processing hook
                if not self.pre_process(task):
                    logger.info(f"Task {task.task_id} skipped by pre_process")
                    continue

                # Process the task
                success = self.process(task)

                # Post-processing hook
                self.post_process(task, success)

                # Update task status
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

            except Exception as e:
                logger.error(
                    f"Worker {self.worker_id} encountered error: {e}", exc_info=True
                )
                time.sleep(1)

        logger.info(f"Worker {self.worker_id} stopped")

    def _stop_handler(self, signum, frame):
        logger.info(f"Worker {self.worker_id} received stop signal")
        self.running = False

    def stop(self):
        self.running = False
