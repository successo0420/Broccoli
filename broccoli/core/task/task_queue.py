# video_scheduler/core/task_queue.py
import logging
from typing import Optional

from broccoli.core.redis_controller import RedisController
from broccoli.core.task.task import Task

logger = logging.getLogger(__name__)


class TaskQueue:
    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        queue_name: str = "tasks:queue",
        task_prefix: str = "task",
    ):
        self.redis_url = redis_url
        self._redis = RedisController(redis_url).get_client()
        self.queue_key = queue_name
        self.task_prefix = task_prefix

    def push(self, task: Task, priority: int = 0) -> str:
        """Push task with priority (0=highest, higher numbers = lower priority)."""
        self._redis.hset(f"{self.task_prefix}:{task.task_id}", mapping=task.to_dict())
        # Use sorted set for priority
        self._redis.zadd(self.queue_key, mapping={task.task_id: priority})
        return task.task_id

    def requeue(self, task_id: str, priority: int = 0) -> None:
        """Put a task back in the queue. Must use the same data structure as
        push() (a sorted set), since pop() reads from it with BZPOPMIN."""
        self._redis.zadd(self.queue_key, mapping={task_id: priority})

    def pop(self) -> Task | None:
        """Pop the highest-priority task, but skip tasks whose dependencies aren't complete."""
        while True:  # Keep trying until we find an available task
            result = self._redis.bzpopmin(self.queue_key, timeout=1)
            if result is None:
                return None

            _, task_id, priority = result

            task_data = self._redis.hgetall(f"{self.task_prefix}:{task_id}")
            if not task_data:
                continue

            task = Task.from_dict(task_data)

            # Check if this task has a dependency
            if task.depends_on:
                dep_task = self.get_task(task.depends_on)

                # If dependency doesn't exist or isn't completed, requeue and skip
                if not dep_task or dep_task.status != "completed":
                    self.requeue(
                        task_id, priority=priority
                    )  # Put back with same priority

                    continue  # Try the next task

            # If we get here, task is ready to process
            self._redis.hset(f"{self.task_prefix}:{task_id}", "status", "in_progress")
            task.status = "in_progress"
            return task

    def get_task(self, task_id: str) -> Optional[Task]:
        """Get a task by ID."""
        task_data = self._redis.hgetall(f"{self.task_prefix}:{task_id}")
        if not task_data:
            return None
        return Task.from_dict(task_data)

    def pop_with_timeout(self, timeout=1):
        """Pop with timeout."""
        result = self._redis.bzpopmin(self.queue_key, timeout=timeout)
        if result is None:
            return None

        _, task_id, priority = result
        task_data = self._redis.hgetall(f"{self.task_prefix}:{task_id}")

        if not task_data:
            return None

        task = Task.from_dict(task_data)

        self._redis.hset(f"{self.task_prefix}:{task_id}", "status", "in_progress")
        task.status = "in_progress"
        return task
