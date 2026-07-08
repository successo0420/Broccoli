from broccoli.core.task.task_queue import TaskQueue


class ChainQueue(TaskQueue):
    """A queue for managing chain tasks. Inherits from TaskQueue and adds chain-specific functionality."""

    def __init__(self, redis_url: str = "redis://localhost:6379"):
        super().__init__(
            redis_url=redis_url,
            queue_name="chain_tasks:queue",
            task_prefix="chain",
        )
