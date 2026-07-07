from broccoli.core.task.task_queue import TaskQueue


class ChainQueue(TaskQueue):
    def __init__(self, redis_url: str = "redis://localhost:6379"):
        super().__init__(
            redis_url=redis_url,
            queue_name="chain_tasks:queue",
            task_prefix="chain",
        )
