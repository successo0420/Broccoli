from broccoli.core.task.task_queue import TaskQueue


class ChainQueue(TaskQueue):
    """A queue for managing chain tasks. Inherits from TaskQueue and adds chain-specific functionality."""

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        queue_name: str = "chain_tasks:queue",
        task_prefix: str = "chain",
        decode_responses: bool = True,
        redis_config: dict = None,
    ):
        super().__init__(
            redis_url=redis_url,
            queue_name=queue_name,
            task_prefix=task_prefix,
            decode_responses=decode_responses,
            redis_config=redis_config,
        )
