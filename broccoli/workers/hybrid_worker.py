# broccoli/workers/hybrid_worker.py
# See async_worker.py: this defers evaluation of the `X | None` /
# lowercase-generic hints below so they don't raise on Python 3.9.
from __future__ import annotations

import asyncio
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import redis

from broccoli.core.task.task import Task
from broccoli.workers.base_worker import BaseWorker

logger = logging.getLogger(__name__)


class HybridWorker(BaseWorker):
    """
    Worker that combines asyncio (for concurrency) with a thread pool
    (for CPU-bound / blocking task handlers).

    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        worker_id: str = None,
        queue_name: str = "tasks:queue",
        task_prefix: str = "task",
        thread_workers: int = 4,
        async_tasks: int = 10,
        result_ttl: int = 86400,  # 24 hours
        recover_on_startup: bool = True,
        recover_stalled_timeout: int = 3600,
    ):
        super().__init__(
            redis_url=redis_url,
            worker_id=worker_id,
            queue_name=queue_name,
            task_prefix=task_prefix,
            recover_on_startup=recover_on_startup,
            recover_stalled_timeout=recover_stalled_timeout,
        )
        self.thread_pool = ThreadPoolExecutor(max_workers=thread_workers)
        self.thread_workers = thread_workers
        self.async_tasks = async_tasks
        self.active_tasks: set[str] = set()
        self.loop: asyncio.AbstractEventLoop | None = None
        self.result_ttl = result_ttl
        # Semaphore created lazily inside the running loop.
        self._semaphore: asyncio.Semaphore | None = None

    # ------------------------------------------------------------------
    # post_process override — prevent double-firing
    # ------------------------------------------------------------------

    def post_process(self, task: Task, success: bool) -> None:
        """
        HybridWorker handles result storage, hash cleanup, and completion /
        failure callbacks inside ``_handle_hybrid_result``.  This override
        runs only the registered post-process handlers so that none of those
        actions fire a second time.
        """
        self._run_post_process_handlers(task, success)

    # ------------------------------------------------------------------
    # Async / hybrid task processing
    # ------------------------------------------------------------------

    async def process_task_hybrid(self, task: Task) -> None:
        """
        Acquire a concurrency slot, run the task handler in the thread pool,
        then handle the result (queue transitions, result storage, callbacks).
        """
        async with self._semaphore:
            try:
                # pre_process is checked before entering the thread pool so a
                # skip (returns False) can be routed to queue.fail() directly,
                # rather than being indistinguishable from a real processing
                # failure and consuming retry budget in _handle_hybrid_result.
                if not self.pre_process(task):
                    logger.info(f"Task {task.task_id} skipped by pre_process")
                    self.queue.fail(task)
                    return

                # Use get_running_loop() — get_event_loop() is deprecated in 3.10+.
                loop = asyncio.get_running_loop()
                try:
                    success = await asyncio.wait_for(
                        loop.run_in_executor(self.thread_pool, self.process, task),
                        timeout=self.task_timeout,
                    )
                except asyncio.TimeoutError:
                    logger.error(
                        f"Task {task.task_id} timed out after {self.task_timeout}s"
                    )
                    task.error = f"Task timed out after {self.task_timeout}s"
                    success = False
                self._handle_hybrid_result(task, success)
            except Exception as e:
                logger.error(f"Hybrid task {task.task_id} failed: {e}", exc_info=True)
                task.error = str(e)
                self._handle_hybrid_result(task, False)
            finally:
                self.active_tasks.discard(task.task_id)

    def _handle_hybrid_result(self, task: Task, success: bool) -> None:
        """
        Manage queue transitions and persist results.

        Queue state is handled by TaskQueue methods (complete / fail / requeue),
        not by direct Redis calls.  Result saving and hash cleanup are kept
        separate so each concern has one place.

        Completion / failure handlers are fired here (not in post_process) to
        keep the full lifecycle in one readable sequence.
        """
        if success:
            task.status = "completed"
            task.progress = 100.0
            # Update Redis before releasing dependents (same ordering as BaseWorker).
            self._update_task(task)
            self.queue.complete(task)  # releases dependents, removes from processing
            logger.info(f"Task {task.task_id} completed")
        else:
            task.retries += 1
            if task.retries >= task.max_retries:
                task.status = "failed"
                if not task.error:
                    task.error = "Max retries exceeded"
                self._update_task(task)
                self.queue.fail(task)  # removes from processing set
            else:
                task.status = "pending"
                self._update_task(task)
                self.queue.requeue(task.task_id)  # back to runnable queue
                logger.info(
                    f"Task {task.task_id} requeued "
                    f"(attempt {task.retries}/{task.max_retries})"
                )
                # Requeued — don't store a partial result or fire handlers.
                # post_process still runs for registered post-process handlers.
                self.post_process(task, success)
                return

        # On permanent failure, record in the dead-letter set before result
        # storage/cleanup so the task stays inspectable even if _save_result
        # raises.
        if task.status == "failed":
            try:
                failed_at = time.time()
                self._redis.zadd(
                    f"{self.task_prefix}:dead_letter", {task.task_id: failed_at}
                )
                dead_copy = task.to_dict()
                dead_copy["failed_at"] = str(failed_at)
                self._redis.hset(
                    f"dl:{task.task_id}",
                    mapping=dead_copy,
                )
            except Exception as e:
                logger.error(
                    f"Failed to record {task.task_id} in dead-letter set: {e}",
                    exc_info=True,
                )

        # Persist extended result metadata with TTL.
        self._save_result(task)

        # Remove task metadata hash (queue sets already updated above).
        self._cleanup_task(task)

        # Fire user-facing callbacks.
        if success:
            self._run_completion_handlers(task, task.result)
        else:
            error = Exception(task.error) if task.error else Exception("Task failed")
            self._run_failure_handlers(task, error)

        # post_process runs only registered post-process handlers (overridden above).
        self.post_process(task, success)

    # ------------------------------------------------------------------
    # Result storage & cleanup
    # ------------------------------------------------------------------

    def _save_result(self, task: Task) -> None:
        """Persist full result metadata under ``result:<task_id>`` with a TTL."""
        result_data = {
            "id": task.task_id,
            "task_type": task.task_type,
            "status": task.status,
            "result": task.result,
            "error": task.error,
            "chain": bool(task.payload.get("__chain_id")),
            "chain_id": task.payload.get("__chain_id"),
            "worker_id": self.worker_id,
            "completed_at": datetime.now().isoformat(),
            "retries": task.retries,
        }
        self._redis.setex(
            f"result:{task.task_id}", self.result_ttl, json.dumps(result_data)
        )
        logger.debug(f"Result saved for task {task.task_id} (TTL={self.result_ttl}s)")

    def _cleanup_task(self, task: Task) -> None:
        """
        Remove the task metadata hash from Redis.

        Queue sorted-set membership is already managed by
        ``queue.complete()`` / ``queue.fail()`` — no ``zrem`` calls here.
        """
        self._redis.delete(f"{self.task_prefix}:{task.task_id}")

        if task.payload.get("__chain_id"):
            self._redis.delete(f"chain:{task.task_id}")

        logger.info(f"Task {task.task_id} metadata cleaned up")

    # ------------------------------------------------------------------
    # Worker loop
    # ------------------------------------------------------------------

    def start(self):
        """Create a fresh event loop and run until stopped."""
        self._register_signal_handlers()
        self.running = True
        self._recover_stalled_on_startup()
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)
        logger.info(
            f"HybridWorker {self.worker_id} started "
            f"(threads={self.thread_workers}, async_slots={self.async_tasks})"
        )
        try:
            self.loop.run_until_complete(self._run_hybrid())
        finally:
            self.loop.close()

    async def _run_hybrid(self):
        self._semaphore = asyncio.Semaphore(self.async_tasks)

        backoff = 1
        while self.running:
            if len(self.active_tasks) >= self.async_tasks:
                await asyncio.sleep(0.05)
                continue

            try:
                loop = asyncio.get_running_loop()
                task = await loop.run_in_executor(None, self.queue.pop_with_timeout, 1)
                backoff = 1  # reset after any successful Redis round-trip
            except redis.exceptions.RedisError as e:
                logger.error(
                    f"HybridWorker {self.worker_id} Redis error: {e}, "
                    f"retrying in {backoff}s",
                    exc_info=True,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
                continue

            if task is None:
                await asyncio.sleep(0.05)
                continue

            self.active_tasks.add(task.task_id)
            asyncio.create_task(self.process_task_hybrid(task))

        # Drain in-flight tasks before shutting down.
        while self.active_tasks:
            await asyncio.sleep(0.05)

        self.thread_pool.shutdown(wait=True)
        logger.info(f"HybridWorker {self.worker_id} stopped")

    def stop(self):
        self.running = False
        logger.info(f"HybridWorker {self.worker_id} stopping...")
