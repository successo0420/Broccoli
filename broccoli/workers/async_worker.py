# broccoli/workers/async_worker.py
# PEP 563 defers annotation evaluation, so the PEP 604 `X | None` and
# lowercase-generic (`set[str]`) hints below don't raise on Python 3.9,
# which doesn't support that syntax natively (3.10+ only).
from __future__ import annotations

import asyncio
import logging

import redis

from broccoli.core.task.task import Task
from broccoli.workers.base_worker import BaseWorker

logger = logging.getLogger(__name__)


class AsyncWorker(BaseWorker):
    """
    Worker that processes tasks concurrently using asyncio.

    CPU-bound / blocking task handlers are offloaded to the default thread-pool
    executor so they don't stall the event loop.

    State transitions (complete / fail / requeue) are fully delegated to
    ``BaseWorker._handle_task_result``
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        worker_id: str = None,
        queue_name: str = "tasks:queue",
        task_prefix: str = "task",
        max_concurrent: int = 10,
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
        self.max_concurrent = max_concurrent
        # Semaphore is created lazily inside the running event loop to avoid
        # "no running event loop" errors at construction time.
        self._semaphore: asyncio.Semaphore | None = None
        self.active_tasks: set[str] = set()

    # ------------------------------------------------------------------
    # Async task lifecycle
    # ------------------------------------------------------------------

    async def process_task_async(self, task: Task) -> None:
        """Acquire a concurrency slot and process one task."""
        async with self._semaphore:
            try:
                logger.info(f"AsyncWorker {self.worker_id} processing {task.task_id}")

                if not self.pre_process(task):
                    logger.info(f"Task {task.task_id} skipped by pre_process")
                    self.queue.fail(task)  # remove from processing set
                    return

                # Use get_running_loop() — get_event_loop() is deprecated in 3.10+
                # and raises a DeprecationWarning inside a running coroutine.
                loop = asyncio.get_running_loop()
                # Only self.process() is allowed to fail the task via exception.
                # _handle_task_result / post_process must be allowed to propagate
                # their own errors to the outer except below, rather than being
                # re-invoked a second time on an already-transitioned task.
                try:
                    success = await asyncio.wait_for(
                        loop.run_in_executor(None, self.process, task),
                        timeout=self.task_timeout,
                    )
                except asyncio.TimeoutError:
                    logger.error(
                        f"Task {task.task_id} timed out after {self.task_timeout}s"
                    )
                    task.error = f"Task timed out after {self.task_timeout}s"
                    success = False
                except Exception as e:
                    logger.error(
                        f"Task {task.task_id} handler raised: {e}", exc_info=True
                    )
                    task.error = str(e)
                    success = False

                # Central state machine: complete / requeue / fail
                self._handle_task_result(task, success)

                # Result storage and user-facing callbacks
                self.post_process(task, success)

            except Exception as e:
                # Reached only if _handle_task_result/post_process themselves
                # raised — the task's queue state may already be inconsistent,
                # so we log rather than re-running the state machine on it again.
                logger.error(
                    f"Task {task.task_id} failed outside handler: {e}", exc_info=True
                )
            finally:
                self.active_tasks.discard(task.task_id)

    # ------------------------------------------------------------------
    # Event-loop entry points
    # ------------------------------------------------------------------

    async def start_async(self):
        """Main async loop: pop tasks and dispatch them as asyncio tasks."""
        self._register_signal_handlers()
        self._recover_stalled_on_startup()
        # Semaphore must be created inside the running loop.
        self._semaphore = asyncio.Semaphore(self.max_concurrent)
        self.running = True
        logger.info(
            f"AsyncWorker {self.worker_id} started "
            f"(max_concurrent={self.max_concurrent})"
        )

        backoff = 1
        while self.running:
            try:
                if len(self.active_tasks) >= self.max_concurrent:
                    await asyncio.sleep(0.05)
                    continue

                loop = asyncio.get_running_loop()
                task = await loop.run_in_executor(None, self.queue.pop)
                backoff = 1  # reset after any successful Redis round-trip

                if task is None:
                    if len(self._completion_handlers) > 0:
                        self._run_completion_handlers()
                        return
                    await asyncio.sleep(0.05)
                    continue

                self.active_tasks.add(task.task_id)
                asyncio.create_task(self.process_task_async(task))
                logger.info(
                    f"Dispatched {task.task_id} "
                    f"(active: {len(self.active_tasks)}/{self.max_concurrent})"
                )

            except redis.exceptions.RedisError as e:
                logger.error(
                    f"AsyncWorker {self.worker_id} Redis error: {e}, "
                    f"retrying in {backoff}s",
                    exc_info=True,
                )
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)
            except Exception as e:
                logger.error(
                    f"AsyncWorker {self.worker_id} loop error: {e}", exc_info=True
                )
                await asyncio.sleep(1)

        # Drain any still-running tasks before exiting.
        while self.active_tasks:
            await asyncio.sleep(0.05)

        logger.info(f"AsyncWorker {self.worker_id} stopped")

    def start(self):
        """Synchronous entry point — runs the async loop until completion."""
        asyncio.run(self.start_async())
