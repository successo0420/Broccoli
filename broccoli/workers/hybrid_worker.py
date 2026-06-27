# video_scheduler/core/hybrid_worker.py
import asyncio
import json
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from broccoli.core.task.task import Task
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
        result_ttl: int = 86400,  # 24 hours
    ):
        super().__init__(redis_url, worker_id)
        self.thread_pool = ThreadPoolExecutor(max_workers=thread_workers)
        self.async_semaphore = asyncio.Semaphore(async_tasks)
        self.thread_workers = thread_workers
        self.async_tasks = async_tasks
        self.active_tasks = set()
        self.loop = None
        self.result_ttl = result_ttl

    async def process_task_hybrid(self, task):
        """Process task using hybrid approach."""
        async with self.async_semaphore:
            try:
                # Use thread pool for CPU-bound work
                loop = asyncio.get_event_loop()
                success = await loop.run_in_executor(
                    self.thread_pool, self._process_sync, task
                )
                # Handle result after processing
                self._handle_task_result(task, success)
            except Exception as e:
                logger.error(f"Hybrid task {task.task_id} failed: {e}")
                task.error = str(e)
                self._handle_task_result(task, False)
            finally:
                self.active_tasks.discard(task.task_id)

    def _process_sync(self, task):
        """Sync processing (runs in thread pool)."""
        # Pre-processing hook
        if not self.pre_process(task):
            return False

        # Process the task
        success = self.process(task)
        return success

    def _handle_task_result(self, task: Task, success: bool) -> None:
        """Handle task result - saves result and cleans up."""
        # Update task status
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
                return  # Don't clean up if requeued

        # Run registered handlers
        if success:
            self._run_completion_handlers(task, task.result)
        else:
            error = Exception(task.error) if task.error else Exception("Task failed")
            self._run_failure_handlers(task, error)

        # Save result (separate from task data)
        self._save_result(task)

        # Clean up task from Redis
        self._cleanup_task(task)

        # Call post_process hook
        self.post_process(task, success)

    def _save_result(self, task: Task) -> None:
        """Save task result with full metadata."""
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

        # Store result with TTL
        self._redis.setex(
            f"result:{task.task_id}", self.result_ttl, json.dumps(result_data)
        )
        logger.debug(f"Result saved for task {task.task_id}")

    def _cleanup_task(self, task: Task) -> None:
        """Completely remove task from Redis."""
        task_id = task.task_id

        # Delete task data hash
        self._redis.delete(f"{self.task_prefix}:{task_id}")

        # Remove from queue
        self._redis.zrem(self.queue.queue_key, task_id)

        # Delete chain reference if exists
        if task.payload.get("__chain_id"):
            self._redis.delete(f"chain:{task_id}")

        logger.info(f"Task {task_id} cleaned up from Redis")

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

            # Pop task with timeout
            task = await asyncio.get_event_loop().run_in_executor(
                None,
                self.queue.pop_with_timeout,
                1,
            )

            if task is None:
                await asyncio.sleep(0.1)
                continue

            self.active_tasks.add(task.task_id)
            asyncio.create_task(self.process_task_hybrid(task))

        # Wait for all tasks to complete
        while self.active_tasks:
            await asyncio.sleep(0.1)

        self.thread_pool.shutdown(wait=True)
        logger.info("HybridWorker stopped")

    def stop(self):
        """Stop the worker."""
        self.running = False
        logger.info("HybridWorker stopping...")
