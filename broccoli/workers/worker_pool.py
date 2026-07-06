# broccoli/workers/worker_pool.py
import logging
import signal
import threading
from typing import List

from broccoli.workers.base_worker import BaseWorker

logger = logging.getLogger(__name__)


class WorkerPool:
    """
    Manages a fixed-size pool of workers, each running in its own daemon thread.
    """

    def __init__(
        self,
        worker_type: type = BaseWorker,
        num_workers: int = 4,
        redis_url: str = "redis://localhost:6379",
        **worker_kwargs,
    ):
        self.worker_type = worker_type
        self.num_workers = num_workers
        self.redis_url = redis_url
        self.worker_kwargs = worker_kwargs  # e.g. max_workers=, max_concurrent=
        self.workers: List[BaseWorker] = []  # initialised before _create_workers runs
        self.threads: List[threading.Thread] = []
        self.running = False
        self.shutdown_flag = threading.Event()
        self._create_workers()  # populate self.workers once

    def _create_workers(self) -> None:
        """Instantiate ``num_workers`` workers and append them to ``self.workers``."""
        for i in range(self.num_workers):
            worker = self.worker_type(
                redis_url=self.redis_url,
                worker_id=f"worker-{i + 1}",
                **self.worker_kwargs,
            )
            self.workers.append(worker)

    def start(self):
        """
        Spawn one daemon thread per worker, then block until a shutdown signal
        arrives (``stop()`` or a signal handler calling ``_signal_handler``).
        """
        # Reset per-cycle state so repeated start()/stop() cycles (e.g. in
        # tests) don't accumulate dead threads from a previous run or get
        # stuck on an already-set flag from a prior shutdown.
        self.threads = []
        self.shutdown_flag.clear()
        self.running = True

        try:
            signal.signal(signal.SIGINT, self._signal_handler)
            signal.signal(signal.SIGTERM, self._signal_handler)
        except (ValueError, OSError) as e:
            logger.debug(f"Could not register signal handlers: {e}")

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

        # Set first so a direct stop() call (e.g. from a test or a completion
        # handler) also unblocks start(), which may be parked on this wait().
        self.shutdown_flag.set()

        for worker in self.workers:
            try:
                worker.stop()
            except Exception as e:
                logger.error(f"Error stopping worker: {e}")

        for thread in self.threads:
            thread.join(timeout=3)

        self.running = False
        logger.info("All workers stopped")
