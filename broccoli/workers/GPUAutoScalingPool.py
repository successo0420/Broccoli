import logging
import subprocess

from broccoli.workers.auto_scale_worker import AutoScalingWorkerPool
from broccoli.workers.gpu_worker import GPUWorker

logger = logging.getLogger(__name__)


class GPUAutoScalingPool(AutoScalingWorkerPool):
    """
    Autoscaling pool for GPU workers that monitors GPU memory/utilisation
    before scaling up.
    """

    def __init__(
        self,
        gpu_ids: list = None,
        max_gpu_memory_util: float = 0.9,
        min_free_memory_mb: int = 1024,
        **kwargs,
    ):
        # Force worker type to GPUWorker
        kwargs.setdefault("worker_type", GPUWorker)
        super().__init__(**kwargs)
        self.gpu_ids = gpu_ids or [0]  # default to GPU 0
        self.max_gpu_memory_util = max_gpu_memory_util
        self.min_free_memory_mb = min_free_memory_mb

    def _scale(self):
        """Override to check GPU availability before scaling up."""
        stats = self.queue.stats()
        runnable = stats.get("runnable", 0)
        current = len(self.workers)

        # Only scale up if we have enough GPU memory free
        if runnable > self.scale_up_threshold and current < self.max_workers:
            if not self._has_available_gpu():
                logger.info("GPU resources insufficient, not scaling up")
                return

        super()._scale()

    def _has_available_gpu(self) -> bool:
        """Check if any GPU has free memory above threshold."""
        try:
            # Use nvidia-smi to get memory info
            result = subprocess.run(
                [
                    "nvidia-smi",
                    "--query-gpu=memory.free",
                    "--format=csv,noheader,nounits",
                ],
                capture_output=True,
                text=True,
                check=True,
            )
            free_memories = [
                int(x.strip()) for x in result.stdout.strip().split("\n") if x.strip()
            ]
            # Only consider GPUs in self.gpu_ids
            for i, free_mb in enumerate(free_memories):
                if i in self.gpu_ids and free_mb >= self.min_free_memory_mb:
                    return True
            return False
        except Exception as e:
            logger.warning(f"Could not query GPU memory: {e}")
            return True  # fallback: assume available
