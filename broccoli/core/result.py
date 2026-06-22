# video_scheduler/core/result.py
import json
from dataclasses import dataclass

import redis


class ResultBackend:
    def __init__(self, redis_url: str):
        self.redis = redis.from_url(redis_url)

    def store(self, task):
        """Store task result with TTL."""
        key = f"result:{task.task_id}"
        result_mapping = ResultMapping(
            task_id=task.task_id,
            result=task.result,
            status=task.status,
            error=task.error or "",
        )
        self.redis.setex(key, self.ttl, mapping=result_mapping.to_dict())

    def get(self, task_id: str) -> any:
        """Retrieve task result."""
        key = f"result:{task_id}"
        data = self.redis.get(key)
        if data:
            return ResultMapping.from_dict(json.loads(data))
        return None


@dataclass
class ResultMapping:
    task_id: str
    result: any
    status: str
    error: str

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "result": json.dumps(self.result),
            "status": self.status,
            "error": self.error,
        }

    def from_dict(cls, data: dict) -> "ResultMapping":
        return cls(
            task_id=data.get("task_id"),
            result=json.loads(data.get("result", "{}")),
            status=data.get("status", "unknown"),
            error=data.get("error", ""),
        )
