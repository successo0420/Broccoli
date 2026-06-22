# video_scheduler/core/async_worker.py
import asyncio
import logging

from broccoli.core.task import Task
from broccoli.workers.base_worker import BaseWorker

logger = logging.getLogger(__name__)


class AsyncWorker(BaseWorker):
    """Worker that processes tasks asynchronously using asyncio."""

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        worker_id: str = None,
        max_concurrent: int = 10,
    ):
        super().__init__(redis_url, worker_id)
        self.max_concurrent = max_concurrent
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.active_tasks = set()

    async def process_task_async(self, task: Task) -> None:
        """Process a task asynchronously."""
        async with self.semaphore:
            try:
                logger.info(f"Async processing {task.task_id}")

                # Pre-processing hook (can be async if needed)
                if not self.pre_process(task):
                    logger.info(f"Task {task.task_id} skipped by pre_process")
                    self._handle_task_result(task, False)
                    return

                # Process the task (run in thread pool if it's sync)
                loop = asyncio.get_event_loop()
                success = await loop.run_in_executor(None, self.process, task)

                # Post-processing hook
                self.post_process(task, success)

                # Update task status
                self._handle_task_result(task, success)

            except Exception as e:
                logger.error(f"Task {task.task_id} async failed: {e}")
                task.error = str(e)
                self._handle_task_result(task, False)
            finally:
                self.active_tasks.discard(task.task_id)

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

    async def start_async(self):
        """Start the async worker."""
        self.running = True
        logger.info(
            f"AsyncWorker {self.worker_id} started with {self.max_concurrent} concurrent tasks"
        )

        while self.running:
            try:
                # Check capacity
                if len(self.active_tasks) >= self.max_concurrent:
                    await asyncio.sleep(0.1)
                    continue

                # Get task (Redis operations are sync, run in thread pool)
                loop = asyncio.get_event_loop()
                task = await loop.run_in_executor(None, self.queue.pop)

                if task is None:
                    await asyncio.sleep(0.1)
                    continue

                # Track and process
                self.active_tasks.add(task.task_id)
                asyncio.create_task(self.process_task_async(task))
                logger.info(
                    f"Started async task {task.task_id} (active: {len(self.active_tasks)}/{self.max_concurrent})"
                )

            except Exception as e:
                logger.error(f"Async worker error: {e}")
                await asyncio.sleep(1)

        # Wait for active tasks to complete
        while self.active_tasks:
            await asyncio.sleep(0.1)

        logger.info(f"AsyncWorker {self.worker_id} stopped")

    def start(self):
        """Sync wrapper for start_async."""
        asyncio.run(self.start_async())
