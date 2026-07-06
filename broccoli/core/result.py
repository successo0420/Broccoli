# video_scheduler/core/result.py
import json
from dataclasses import dataclass

import redis

from broccoli.core.chain.chain import Chain
from broccoli.core.task.task import Task


class ResultBackend:
    def __init__(self, redis_url: str = "redis://localhost:6379/0"):
        self._redis = redis.from_url(redis_url)
        self.ttl = 3600  # Default TTL for results in seconds

    def store_task(self, task: Task) -> None:
        """Store task result with TTL."""
        key = f"result:{task.task_id}"
        result_mapping = ResultMapping(
            id=task.task_id,
            result=task.result,
            status=task.status,
            chain=False,
            error=task.error or "",
        )

        json_string = json.dumps(result_mapping.to_dict())
        self._redis.set(name=key, ex=self.ttl, value=json_string)

    def store_chain(self, chain: Chain) -> None:
        """Store chain result with TTL."""
        key = f"result:{chain.chain_id}"
        result_mapping = ResultMapping(
            id=chain.chain_id,
            result=chain.result,
            status=chain.status,
            chain=True,
            error="",
        )

        json_string = json.dumps(result_mapping.to_dict())
        self._redis.set(name=key, ex=self.ttl, value=json_string)

    def get(self, id: str) -> any:
        """Retrieve task result."""
        key = f"result:{id}"
        data = self._redis.get(key)
        if data:
            return ResultMapping.from_dict(json.loads(data))
        return None


@dataclass
class ResultMapping:
    id: str
    result: any
    status: str
    chain: bool
    error: str = ""  # Optional error field for tasks

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "result": json.dumps(self.result),
            "status": self.status,
            "chain": self.chain,
            "error": self.error,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "ResultMapping":
        return cls(
            id=data.get("id"),
            result=json.loads(data.get("result", "{}")),
            status=data.get("status", "unknown"),
            chain=data.get("chain", False),
            error=data.get("error", ""),
        )
