# video_scheduler/core/worker_pool.py
import logging
import threading
from typing import List

from broccoli.workers.base_worker import BaseWorker

logger = logging.getLogger(__name__)


class WorkerPool:
    def __init__(
        self,
        worker_type: BaseWorker = BaseWorker,
        num_workers: int = 4,
        redis_url: str = "redis://localhost:6379",
    ):
        self.worker_type = worker_type
        self.num_workers = num_workers
        self.redis_url = redis_url
        self.workers: List[BaseWorker] = self._create_workers()
        self.threads: List[threading.Thread] = []
        self.running = False
        self.shutdown_flag = threading.Event()  # Added for clean shutdown

    def _create_workers(self):
        """Create worker instances."""
        for i in range(self.num_workers):
            worker = self.worker_type(
                redis_url=self.redis_url, worker_id=f"worker-{i + 1}"
            )
            self.workers.append(worker)
        return self.workers

    def start(self):
        """Start all workers."""
        self.running = True

        self._create_workers()

        for i in range(self.num_workers):
            worker = self.workers[i]
            thread = threading.Thread(
                target=worker.start,
                name=f"Worker-{i + 1}",
                daemon=True,  # Keep daemon so they exit when main exits
            )

            self.threads.append(thread)
            thread.start()
            logger.info(f"Started worker {i + 1}/{self.num_workers}")

        # Wait for shutdown signal
        self.shutdown_flag.wait()  # Blocks until set

        # Stop all workers
        self.stop()

    def _signal_handler(self, signum, frame):
        """Handle Ctrl+C and SIGTERM."""
        logger.info("Received stop signal, shutting down workers...")
        self.shutdown_flag.set()  # Unblock the main thread

    def stop(self):
        """Stop all workers gracefully."""
        logger.info("Stopping all workers...")

        # Stop each worker
        for worker in self.workers:
            try:
                worker.stop()
            except Exception as e:
                logger.error(f"Error stopping worker: {e}")

        # Wait for threads to finish (with timeout)
        for thread in self.threads:
            thread.join(timeout=3)

        self.running = False
        logger.info("All workers stopped")
