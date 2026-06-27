# video_scheduler/core/worker.py
import logging
import time
from abc import ABC
from datetime import datetime
from typing import Any, Callable, List, Optional

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
        self.task_prefix = task_prefix
        self.queue = TaskQueue(
            queue_name=queue_name, redis_url=redis_url, task_prefix=task_prefix
        )
        self.registry = TaskRegistry()
        self.running = False
        self.worker_id = worker_id or f"worker-{id(self)}"
        self.task_timeout = 3600
        self.result = ResultBackend(redis_url)

        # Handler lists
        self._completion_handlers: List[Callable[[Task, Any], None]] = []
        self._failure_handlers: List[Callable[[Task, Exception], None]] = []
        self._pre_process_handlers: List[Callable[[Task], bool]] = []
        self._post_process_handlers: List[Callable[[Task, bool], None]] = []

    # ============ Handler Registration Methods ============

    def add_completion_handler(self, handler: Callable[[Task, Any], None]):
        """Add a handler to run when tasks complete successfully."""
        self._completion_handlers.append(handler)
        return self  # For chaining

    def add_failure_handler(self, handler: Callable[[Task, Exception], None]):
        """Add a handler to run when tasks fail."""
        self._failure_handlers.append(handler)
        return self

    def add_pre_process_handler(self, handler: Callable[[Task], bool]):
        """Add a handler to run before task processing. Return False to skip."""
        self._pre_process_handlers.append(handler)
        return self

    def add_post_process_handler(self, handler: Callable[[Task, bool], None]):
        """Add a handler to run after task processing (regardless of success)."""
        self._post_process_handlers.append(handler)
        return self

    # ============ Decorator Methods ============

    def on_complete(self, func):
        """Decorator to register completion handler."""
        self._completion_handlers.append(func)
        return func

    def on_failure(self, func):
        """Decorator to register failure handler."""
        self._failure_handlers.append(func)
        return func

    def on_pre_process(self, func):
        """Decorator to register pre-process handler."""
        self._pre_process_handlers.append(func)
        return func

    def on_post_process(self, func):
        """Decorator to register post-process handler."""
        self._post_process_handlers.append(func)
        return func

    # ============ Handler Execution Methods ============

    def _run_completion_handlers(self, task: Task, result: Any):
        """Run all completion handlers."""
        for handler in self._completion_handlers:
            try:
                handler(task, result)
            except Exception as e:
                logger.error(f"Completion handler failed: {e}", exc_info=True)

    def _run_failure_handlers(self, task: Task, error: Exception):
        """Run all failure handlers."""
        for handler in self._failure_handlers:
            try:
                handler(task, error)
            except Exception as e:
                logger.error(f"Failure handler failed: {e}", exc_info=True)

    def _run_pre_process_handlers(self, task: Task) -> bool:
        """Run all pre-process handlers. Return False if any returns False."""
        for handler in self._pre_process_handlers:
            try:
                if not handler(task):
                    return False
            except Exception as e:
                logger.error(f"Pre-process handler failed: {e}", exc_info=True)
                return False
        return True

    def _run_post_process_handlers(self, task: Task, success: bool):
        """Run all post-process handlers."""
        for handler in self._post_process_handlers:
            try:
                handler(task, success)
            except Exception as e:
                logger.error(f"Post-process handler failed: {e}", exc_info=True)

    # ============ Override Hooks (for backward compatibility) ============

    def pre_process(self, task: Task) -> bool:
        """Hook to run before processing. Override this in your custom worker."""
        # Run registered handlers
        return self._run_pre_process_handlers(task)

    def post_process(self, task: Task, success: bool) -> None:
        """Hook to run after processing. Override this in your custom worker."""
        # Run registered handlers
        self._run_post_process_handlers(task, success)

        # Chain tasks are cleaned up in bulk by ChainWorkerMixin
        if task.payload.get("__chain_id"):
            logger.info(
                f"Task {task.task_id} {task.status} (chain task, skipping result store)"
            )
            return

        # Store result
        print(task.status, task.result)
        self.result.store_task(task)
        if self._redis.delete(f"{self.task_prefix}:{task.task_id}"):
            print(f"Task {task.task_id} removed from Redis after processing")
        logger.info(f"Task {task.task_id} {task.status} with result: {task.result}")

        # Run completion or failure handlers
        if success:
            self._run_completion_handlers(task, task.result)
        else:
            self._run_failure_handlers(
                task,
                Exception(task.error) if task.error else Exception("Unknown error"),
            )

    # ============ Main Process Method ============

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
        """Update task in Redis."""
        if task.payload.get("__chain_id"):
            return
        task.updated_at = datetime.now().isoformat()
        self._redis.hset(f"{self.task_prefix}:{task.task_id}", mapping=task.to_dict())

    def start(self):
        """Start the worker."""
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

                # Post-processing hook
                self.post_process(task, success)

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
