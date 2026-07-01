# broccoli/workers/async_worker.py
import asyncio
import logging

from broccoli.core.task.task import Task
from broccoli.workers.base_worker import BaseWorker

logger = logging.getLogger(__name__)


class AsyncWorker(BaseWorker):
    """
    Worker that processes tasks concurrently using asyncio.

    CPU-bound / blocking task handlers are offloaded to the default thread-pool
    executor so they don't stall the event loop.

    State transitions (complete / fail / requeue) are fully delegated to
    ``BaseWorker._handle_task_result`` — no duplicate logic here.
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        worker_id: str = None,
        max_concurrent: int = 10,
    ):
        super().__init__(redis_url, worker_id)
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
                success = await loop.run_in_executor(None, self.process, task)

                # Central state machine: complete / requeue / fail
                self._handle_task_result(task, success)

                # Result storage and user-facing callbacks
                self.post_process(task, success)

            except Exception as e:
                logger.error(f"Task {task.task_id} async error: {e}", exc_info=True)
                task.error = str(e)
                self._handle_task_result(task, False)
                self.post_process(task, False)
            finally:
                self.active_tasks.discard(task.task_id)

    # ------------------------------------------------------------------
    # Event-loop entry points
    # ------------------------------------------------------------------

    async def start_async(self):
        """Main async loop: pop tasks and dispatch them as asyncio tasks."""
        # Semaphore must be created inside the running loop.
        self._semaphore = asyncio.Semaphore(self.max_concurrent)
        self.running = True
        logger.info(
            f"AsyncWorker {self.worker_id} started "
            f"(max_concurrent={self.max_concurrent})"
        )

        while self.running:
            try:
                if len(self.active_tasks) >= self.max_concurrent:
                    await asyncio.sleep(0.05)
                    continue

                loop = asyncio.get_running_loop()
                task = await loop.run_in_executor(None, self.queue.pop)

                if task is None:
                    await asyncio.sleep(0.05)
                    continue

                self.active_tasks.add(task.task_id)
                asyncio.create_task(self.process_task_async(task))
                logger.info(
                    f"Dispatched {task.task_id} "
                    f"(active: {len(self.active_tasks)}/{self.max_concurrent})"
                )

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
