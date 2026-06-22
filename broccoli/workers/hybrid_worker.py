# video_scheduler/core/hybrid_worker.py
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor

from broccoli.core.task import Task
from broccoli.workers.base_worker import BaseWorker

logger = logging.getLogger(__name__)


class HybridWorker(BaseWorker):
    """Worker that combines threads and async for maximum throughput."""

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        worker_id: str = None,
        thread_workers: int = 4,
        async_tasks: int = 10,
    ):
        super().__init__(redis_url, worker_id)
        self.thread_pool = ThreadPoolExecutor(max_workers=thread_workers)
        self.async_semaphore = asyncio.Semaphore(async_tasks)
        self.thread_workers = thread_workers
        self.async_tasks = async_tasks
        self.active_tasks = set()
        self.loop = None

    async def process_task_hybrid(self, task):
        """Process task using hybrid approach."""
        async with self.async_semaphore:
            try:
                # Use thread pool for CPU-bound work
                loop = asyncio.get_event_loop()
                success = await loop.run_in_executor(
                    self.thread_pool, self._process_sync, task
                )
                self._handle_task_result(task, success)
            except Exception as e:
                logger.error(f"Hybrid task {task.task_id} failed: {e}")
                task.error = str(e)
                self._handle_task_result(task, False)
            finally:
                self.active_tasks.discard(task.task_id)

    def _process_sync(self, task):
        """Sync processing (runs in thread pool)."""
        if not self.pre_process(task):
            return False
        success = self.process(task)
        self.post_process(task, success)
        return success

    def start(self):
        """Start hybrid worker."""
        self.running = True
        self.loop = asyncio.new_event_loop()
        asyncio.set_event_loop(self.loop)

        logger.info(
            f"HybridWorker started (threads={self.thread_workers}, async={self.async_tasks})"
        )
        self.loop.run_until_complete(self._run_hybrid())
        self.loop.close()

    async def _run_hybrid(self):
        while self.running:
            if len(self.active_tasks) >= self.async_tasks:
                await asyncio.sleep(0.1)
                continue

            # Use timeout to prevent blocking
            task = await asyncio.get_event_loop().run_in_executor(
                None,
                self.queue.pop_with_timeout,
                1,  # Add timeout method
            )
            if task is None:
                await asyncio.sleep(0.1)
                continue

            self.active_tasks.add(task.task_id)
            asyncio.create_task(self.process_task_hybrid(task))

        while self.active_tasks:
            await asyncio.sleep(0.1)
        self.thread_pool.shutdown(wait=True)

    def _handle_task_result(self, task: Task, success: bool) -> None:
        """Handle task result (same as BaseWorker)."""
        if success:
            task.status = "completed"
            task.progress = 100.0
            logger.info(f"Task {task.task_id} completed successfully")
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
