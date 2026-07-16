import json
import uuid
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class Chain:
    chain_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    total_tasks: Optional[int] = None
    completion_task: Optional[str] = None
    status: str = "pending"
    completed_tasks: int = 0
    failed: bool = False
    current_task: int = 0
    result: any = None

    @staticmethod
    def _normalize_redis_mapping(data: dict) -> dict:
        normalized = {}
        for key, value in data.items():
            if isinstance(key, bytes):
                key = key.decode()
            if isinstance(value, bytes):
                value = value.decode()
            normalized[key] = value
        return normalized

    def to_dict(self) -> dict:
        """Convert the Chain object to a dictionary."""
        return {
            "chain_id": self.chain_id,
            "completion_task": self.completion_task or "",
            "status": self.status,
            "completed_tasks": self.completed_tasks,
            "failed": str(self.failed),
            "total_tasks": str(self.total_tasks)
            if self.total_tasks is not None
            else None,
            "current_task": self.current_task,
            "result": json.dumps(self.result) if self.result is not None else None,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Chain":
        """Create a Chain object from a dictionary."""
        data = cls._normalize_redis_mapping(data)
        return cls(
            chain_id=data.get("chain_id"),
            completion_task=data.get("completion_task") or None,
            status=data.get("status", "pending"),
            completed_tasks=int(data.get("completed_tasks", 0)),
            current_task=int(data.get("current_task", 0)),
            result=json.loads(data.get("result")) if data.get("result") else None,
            failed=data.get("failed", "False") == "True",
            total_tasks=int(data.get("total_tasks", 0))
            if data.get("total_tasks")
            else None,
        )
