# broccoli/workers/worker_pool.py
import logging
import threading
from typing import List

from broccoli.workers.base_worker import BaseWorker

logger = logging.getLogger(__name__)


class WorkerPool:
    """
    Manages a fixed-size pool of workers, each running in its own daemon thread.

    Bug fixed from original:
    - ``_create_workers`` referenced ``self.workers`` before it was defined (AttributeError),
      then was called a *second* time inside ``start()``, which would double the pool.
      Workers are now created once in ``__init__`` and started (not re-created) in ``start()``.
    """

    def __init__(
        self,
        worker_type: type = BaseWorker,
        num_workers: int = 4,
        redis_url: str = "redis://localhost:6379",
    ):
        self.worker_type = worker_type
        self.num_workers = num_workers
        self.redis_url = redis_url
        self.workers: List[BaseWorker] = []  # initialised before _create_workers runs
        self.threads: List[threading.Thread] = []
        self.running = False
        self.shutdown_flag = threading.Event()
        self._create_workers()  # populate self.workers once

    def _create_workers(self) -> None:
        """Instantiate ``num_workers`` workers and append them to ``self.workers``."""
        for i in range(self.num_workers):
            worker = self.worker_type(
                redis_url=self.redis_url, worker_id=f"worker-{i + 1}"
            )
            self.workers.append(worker)

    def start(self):
        """
        Spawn one daemon thread per worker, then block until a shutdown signal
        arrives (``stop()`` or a signal handler calling ``_signal_handler``).
        """
        self.running = True

        for i, worker in enumerate(self.workers):
            thread = threading.Thread(
                target=worker.start,
                name=f"Worker-{i + 1}",
                daemon=True,
            )
            self.threads.append(thread)
            thread.start()
            logger.info(f"Started worker {i + 1}/{self.num_workers}")

        # Block the calling thread until stop() or a signal fires.
        self.shutdown_flag.wait()
        self.stop()

    def _signal_handler(self, signum, frame):
        """Handle SIGINT / SIGTERM by unblocking the main thread."""
        logger.info("Received stop signal, shutting down worker pool...")
        self.shutdown_flag.set()

    def stop(self):
        """Gracefully stop all workers and join their threads."""
        logger.info("Stopping all workers...")

        for worker in self.workers:
            try:
                worker.stop()
            except Exception as e:
                logger.error(f"Error stopping worker: {e}")

        for thread in self.threads:
            thread.join(timeout=3)

        self.running = False
        logger.info("All workers stopped")
