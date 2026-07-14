# broccoli/workers/gpu_worker.py
import logging
import os
from typing import Optional

import torch

from broccoli.workers.hybrid_worker import HybridWorker

logger = logging.getLogger(__name__)


class GPUWorker(HybridWorker):
    """
    GPU‑aware worker that restricts itself to a specific GPU device.

    Args:
        gpu_id: GPU index to use (0, 1, ...). Sets CUDA_VISIBLE_DEVICES.
        queue_name: Queue to poll. Defaults to "gpu_tasks:queue".
        preload_cuda: If True, initializes CUDA context on startup to reduce
                      first‑task latency.
    """

    def __init__(
        self,
        gpu_id: int = 0,
        queue_name: str = "gpu_tasks:queue",
        preload_cuda: bool = True,
        **kwargs,
    ):
        # Force the queue name to the GPU queue
        kwargs.setdefault("queue_name", queue_name)
        # Use a distinct task prefix to avoid collision with CPU tasks
        kwargs.setdefault("task_prefix", "gpu")
        super().__init__(**kwargs)

        self.gpu_id = gpu_id
        self.preload_cuda = preload_cuda

        # Restrict to this GPU
        os.environ["CUDA_VISIBLE_DEVICES"] = str(gpu_id)

        if preload_cuda and torch.cuda.is_available():
            try:
                torch.cuda.set_device(0)  # device 0 after restriction
                _ = torch.tensor([1.0]).cuda()  # force context creation
                logger.info(
                    f"GPUWorker {self.worker_id} preloaded CUDA on GPU {gpu_id}"
                )
            except Exception as e:
                logger.warning(f"Failed to preload CUDA: {e}")

    def post_process(self, task, success):
        """Clear GPU cache after each task to prevent memory fragmentation."""
        try:
            if torch.cuda.is_available():
                torch.cuda.empty_cache()
        except Exception:
            pass
        super().post_process(task, success)
