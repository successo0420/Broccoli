# video_scheduler/core/result.py
import json
from typing import Any

from broccoli.core.redis_controller import RedisController
from broccoli.core.task.task import Task


class ResultBackend:
    """
    A backend for storing and retrieving task results in Redis.
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        decode_responses: bool = True,
        redis_config: dict | None = None,
    ):
        redis_config = redis_config or {}
        self._redis = RedisController(
            redis_url,
            decode_responses=decode_responses,
            **redis_config,
        ).get_client()
        self.ttl = 3600  # Default TTL for results in seconds

    def store_task(self, task: Task) -> None:
        """Store task result with TTL."""
        key = f"result:{task.task_id}"
        json_string = json.dumps(task.to_dict())
        self._redis.set(name=key, ex=self.ttl, value=json_string)

    def get_task_result(self, id: str) -> Any | None:
        """Retrieve task result."""
        key = f"result:{id}"
        data = self._redis.get(key)
        if isinstance(data, bytes):
            return data.decode()
        if isinstance(data, str):
            return json.loads(data)
        if data:
            return data
        return None

    def get_dead_letter_task_result(self, id: str) -> Any | None:
        """Retrieve dead letter task result."""
        key = f"dl:{id}"
        data = self._redis.get(key)
        if isinstance(data, bytes):
            return data.decode()
        if data:
            return data
        return None
