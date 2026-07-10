# broccoli/workers/auto_scaling_pool.py
import logging
import threading
import time
from typing import Optional

from broccoli.core.task.task_queue import TaskQueue
from broccoli.workers.hybrid_worker import HybridWorker
from broccoli.workers.worker_pool import WorkerPool

logger = logging.getLogger(__name__)


class AutoScalingWorkerPool(WorkerPool):
    """
    WorkerPool that automatically scales the number of workers based on queue length.

    Args:
        min_workers: Minimum number of workers to keep alive.
        max_workers: Maximum number of workers to spawn.
        scale_up_threshold: Number of runnable tasks that triggers scaling up.
        scale_down_threshold: Number of runnable tasks that triggers scaling down.
        check_interval: Seconds between scaling checks.
        cooldown_seconds: Minimum time between scaling actions (to avoid flapping).
        queue_name: Queue name to monitor (default: "tasks:queue").
        task_prefix: Task prefix for the queue (default: "task").
    """

    def __init__(
        self,
        worker_type=HybridWorker,
        min_workers: int = 1,
        max_workers: int = 10,
        scale_up_threshold: int = 50,
        scale_down_threshold: int = 10,
        check_interval: int = 10,
        cooldown_seconds: int = 30,
        redis_url: str = "redis://localhost:6379",
        queue_name: str = "tasks:queue",
        task_prefix: str = "task",
        **worker_kwargs,
    ):
        super().__init__(
            worker_type=worker_type,
            num_workers=min_workers,
            redis_url=redis_url,
            **worker_kwargs,
        )
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.scale_up_threshold = scale_up_threshold
        self.scale_down_threshold = scale_down_threshold
        self.check_interval = check_interval
        self.cooldown_seconds = cooldown_seconds

        self.queue = TaskQueue(
            redis_url=redis_url,
            queue_name=queue_name,
            task_prefix=task_prefix,
        )
        self._last_scaling_action = 0.0
        self._scaler_thread: Optional[threading.Thread] = None
        self._stop_scaler = threading.Event()

    def start(self):
        """Start the pool and the background scaler thread."""
        self._stop_scaler.clear()
        print("Starting AutoScalingWorkerPool...")
        self._scaler_thread = threading.Thread(
            target=self._scaler_loop,
            name="ScalerThread",
            daemon=True,
        )
        self._scaler_thread.start()
        super().start()

    def stop(self):
        """Stop the scaler and then the pool."""
        self._stop_scaler.set()
        if self._scaler_thread and self._scaler_thread.is_alive():
            self._scaler_thread.join(timeout=5)
        super().stop()

    def _scaler_loop(self):
        """Background loop that monitors queue length and adjusts worker count."""
        print(f"self._scaler_loop started: {self._stop_scaler.is_set()}")
        print(f"self.running: {self.running}")
        while not self._stop_scaler.is_set():
            time.sleep(self.check_interval)
            if not self.running:
                break
            self._scale()

    def _scale(self):
        now = time.time()
        if now - self._last_scaling_action < self.cooldown_seconds:
            return

        stats = self.queue.stats()
        runnable = stats.get("runnable", 0)
        current = len(self.workers)

        target = current
        if runnable > self.scale_up_threshold and current < self.max_workers:
            target = min(current + 1, self.max_workers)
        elif runnable < self.scale_down_threshold and current > self.min_workers:
            target = max(current - 1, self.min_workers)

        if target != current:
            self.set_worker_count(target)
            self._last_scaling_action = time.time()
            logger.info(
                f"Autoscaled: {current} → {target} workers (runnable: {runnable})"
            )
