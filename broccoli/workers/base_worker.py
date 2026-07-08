# broccoli/workers/base_worker.py
import logging
import signal
import time
from abc import ABC
from datetime import datetime
from typing import Any, Callable, List

import redis

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
        self._chain_completion_handlers: List[Callable[[Task, Any], None]] = []

    # ============ Handler Registration Methods ============

    def add_completion_handler(self, handler: Callable[[Task, Any], None]):
        """Add a handler to run when tasks complete successfully."""
        self._completion_handlers.append(handler)
        logger.info(f"Added completion handler: {handler.__name__}")
        return self

    def add_chain_completion_handler(self, handler: Callable[[Task, Any], None]):
        """Add a handler to run when the last task in a chain completes successfully."""
        self._chain_completion_handlers.append(handler)
        logger.info(f"Added chain completion handler: {handler.__name__}")
        return self

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

    def on_chain_complete(self, func):
        """Decorator to register chain completion handler."""
        self._chain_completion_handlers.append(func)
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
        for handler in self._completion_handlers:
            try:
                logger.info(
                    f"Running completion handler: {handler.__name__} for task {task.task_id}"
                )
                handler(task, result)
            except Exception as e:
                logger.error(f"Completion handler failed: {e}", exc_info=True)

    def _run_chain_completion_handlers(self, task: Task, result: Any):
        for handler in self._chain_completion_handlers:
            try:
                logger.info(
                    f"Running chain completion handler: {handler.__name__} for task {task.task_id}"
                )
                handler(task, result)
            except Exception as e:
                logger.error(f"Chain completion handler failed: {e}", exc_info=True)

    def _run_failure_handlers(self, task: Task, error: Exception):
        for handler in self._failure_handlers:
            try:
                handler(task, error)
            except Exception as e:
                logger.error(f"Failure handler failed: {e}", exc_info=True)

    def _run_pre_process_handlers(self, task: Task) -> bool:
        """Return False if any handler returns False or raises."""
        for handler in self._pre_process_handlers:
            try:
                if not handler(task):
                    return False
            except Exception as e:
                logger.error(f"Pre-process handler failed: {e}", exc_info=True)
                return False
        return True

    def _run_post_process_handlers(self, task: Task, success: bool):
        for handler in self._post_process_handlers:
            try:
                handler(task, success)
            except Exception as e:
                logger.error(f"Post-process handler failed: {e}", exc_info=True)

    # ============ Override Hooks ============

    def pre_process(self, task: Task) -> bool:
        """
        Hook called before processing. Override in subclasses for custom logic.
        Registered handlers run first; return False from any handler to skip the task.
        """
        return self._run_pre_process_handlers(task)

    def post_process(self, task: Task, success: bool) -> None:
        """
        Hook called after processing (and after queue-state transitions).

        Only runs result storage and fires completion/failure handlers for
        terminal states (completed or failed).  Requeued tasks (status='pending')
        are skipped entirely — their hash must remain in Redis for the next
        worker that picks them up.

        Queue management (complete / fail / requeue) has already happened in
        ``_handle_task_result`` before this is called.
        """
        self._run_post_process_handlers(task, success)

        # Do not store or delete a requeued task — it will be retried and still
        # needs its hash in Redis.  The worker that eventually completes or
        # permanently fails it will handle cleanup.
        if task.status == "pending":
            return

        # On permanent failure, record the task in a dead-letter set *before*
        # touching result storage or deleting its hash, so it stays
        # inspectable/re-queueable even if result.store_task() below raises
        # (e.g. a Redis error) and the hash still ends up deleted.
        if task.status == "failed":
            try:
                self._redis.zadd(
                    f"{self.task_prefix}:dead_letter", {task.task_id: time.time()}
                )
            except Exception as e:
                logger.error(
                    f"Failed to record {task.task_id} in dead-letter set: {e}",
                    exc_info=True,
                )
        if task.payload.get("__chain_id"):
            logger.info(
                f"Task {task.task_id} is part of chain {task.chain_id}; skipping result storage"
            )
            if task.payload.get("__is_last_task"):
                logger.info(
                    f"Task {task.task_id} is the last task in chain {task.chain_id}; running chain completion handlers"
                )
                # If this is the last task in the chain, store its result in the
                # result backend for the chain.

                self._run_chain_completion_handlers(task, task.payload)
        else:
            self.result.store_task(task)
            logger.info(f"Task {task.task_id} {task.status} — result stored")
        self._redis.delete(f"{self.task_prefix}:{task.task_id}")
        self._run_completion_handlers(task, task.result)

    # ============ Core Task Lifecycle ============

    def process(self, task: Task) -> bool:
        """
        Execute the task using its registered handler.
        Returns True on success, False on failure.
        """
        try:
            handler = self.registry.get_handler(task.task_type)
            if not handler:
                task.error = f"No handler registered for task type: {task.task_type}"
                logger.error(task.error)
                return False

            task.result = handler(task.payload)
            return True

        except Exception as e:
            task.error = str(e)
            logger.error(f"Task {task.task_id} failed: {e}", exc_info=True)
            return False

    def _handle_task_result(self, task: Task, success: bool) -> None:
        """
        Central state machine for task outcomes.

        Transitions:
          success           → completed  — queue.complete() releases dependents
          failure, retries  → pending    — queue.requeue() moves back to runnable queue
          failure, no retry → failed     — queue.fail() removes from processing set

        _update_task() is called BEFORE queue.complete() so that any concurrent
        push() checking the parent's status will see 'completed' rather than
        'in_progress' when deciding whether to wait or enqueue immediately.

        Subclasses (ThreadedWorker, AsyncWorker, HybridWorker) inherit this so
        the retry / dependency-release logic lives in exactly one place.
        """
        if success:
            task.status = "completed"
            task.progress = 100.0
            # Persist 'completed' status BEFORE releasing dependents so that
            # any concurrent push() checking this task's status sees the
            # terminal state and enqueues immediately rather than waiting.
            self._update_task(task)
            self.queue.complete(task)  # releases any waiting dependents
        else:
            task.retries += 1
            if task.retries >= task.max_retries:
                task.status = "failed"
                if not task.error:
                    task.error = "Max retries exceeded"
                self._update_task(task)
                self.queue.fail(task)
            else:
                task.status = "pending"
                self._update_task(task)
                self.queue.requeue(task.task_id)
                logger.info(
                    f"Task {task.task_id} requeued "
                    f"(attempt {task.retries}/{task.max_retries})"
                )

    def _update_task(self, task: Task) -> None:
        """Persist current task state to Redis (skipped for chain tasks)."""
        if task.payload.get("__chain_id"):
            return
        task.updated_at = datetime.now().isoformat()
        self._redis.hset(f"{self.task_prefix}:{task.task_id}", mapping=task.to_dict())

    # ============ Worker Loop ============

    def start(self):
        """
        Start the blocking worker loop.

        NOTE — ``task_timeout`` is NOT enforced here. This loop runs
        ``self.process(task)`` synchronously on the only thread the worker
        has; there is no clean, non-hacky way to abort a running Python
        function from another thread (killing threads is unsafe/unsupported
        in CPython). ThreadedWorker, AsyncWorker, and HybridWorker all
        enforce ``task_timeout`` because they have a spare thread/event loop
        to wait on. If you need timeout enforcement, use one of those
        instead of the plain BaseWorker loop.
        """
        self._register_signal_handlers()
        self.running = True
        logger.info(f"Worker {self.worker_id} started")

        backoff = 1
        while self.running:
            try:
                task = self.queue.pop()
                backoff = 1  # reset after any successful Redis round-trip
                if task is None:
                    continue

                logger.info(
                    f"Worker {self.worker_id} processing task "
                    f"{task.task_id} ({task.task_type})"
                )

                if not self.pre_process(task):
                    logger.info(f"Task {task.task_id} skipped by pre_process")
                    # Remove from processing set without touching result storage.
                    self.queue.fail(task)
                    continue

                success = self.process(task)
                self._handle_task_result(task, success)
                self.post_process(task, success)

            except redis.exceptions.RedisError as e:
                logger.error(
                    f"Worker {self.worker_id} Redis error: {e}, retrying in {backoff}s",
                    exc_info=True,
                )
                time.sleep(backoff)
                backoff = min(backoff * 2, 60)
            except Exception as e:
                logger.error(
                    f"Worker {self.worker_id} encountered error: {e}", exc_info=True
                )
                time.sleep(1)

        logger.info(f"Worker {self.worker_id} stopped")

    def _stop_handler(self, signum, frame):
        logger.info(f"Worker {self.worker_id} received stop signal")
        self.running = False

    def _register_signal_handlers(self):
        """
        Register SIGINT/SIGTERM to call ``_stop_handler``.

        ``signal.signal`` only works from the main thread — when a worker is
        run inside ``WorkerPool`` it lives on a background daemon thread, so
        we skip registration there and rely on ``WorkerPool`` catching the
        signal itself and calling ``stop()`` on each worker instead.
        """
        import threading

        if threading.current_thread() is not threading.main_thread():
            return
        try:
            signal.signal(signal.SIGINT, self._stop_handler)
            signal.signal(signal.SIGTERM, self._stop_handler)
        except (ValueError, OSError) as e:
            logger.debug(f"Could not register signal handlers: {e}")

    def stop(self):
        self.running = False
