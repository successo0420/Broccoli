from broccoli.core.task.task_queue import TaskQueue


class GPUQueue(TaskQueue):
    """A queue for managing GPU tasks. Inherits from TaskQueue and adds GPU-specific functionality."""

    def __init__(self, redis_url: str = "redis://localhost:6379"):
        super().__init__(
            redis_url=redis_url,
            queue_name="gpu_tasks:queue",
            task_prefix="gpu",
        )
