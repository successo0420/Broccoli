import json
from dataclasses import dataclass
from typing import Optional


@dataclass
class Chain:
    chain_id: str
    total_tasks: Optional[int] = None
    completion_task: Optional[str] = None
    status: str = "pending"
    completed_tasks: int = 0
    failed: bool = False
    current_task: int = 0
    result: any = None

    def to_dict(self) -> dict:
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
        return cls(
            chain_id=data.get("chain_id"),
            completion_task=data.get("completion_task") or None,
            status=data.get("status", "pending"),
            completed_tasks=int(data.get("completed_tasks", 0)),
            current_task=int(data.get("current_task", 0)),
            result=data.get("result") if data.get("result") else None,
            failed=data.get("failed", "False") == "True",
            total_tasks=int(data.get("total_tasks", 0))
            if data.get("total_tasks")
            else None,
        )
