# video_scheduler/core/task.py
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, List, Optional


@dataclass
class Task:
    task_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    task_type: str = ""
    status: str = "pending"
    progress: float = 0.0
    retries: int = 0
    chain_id: Optional[str] = None
    max_retries: int = 3
    error: Optional[str] = None
    secondary_error: Optional[str] = None
    payload: dict = field(default_factory=dict)
    result: Any = None
    depends_on: Optional[List[str]] = None  # now a list of task IDs
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    updated_at: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "task_id": self.task_id,
            "task_type": self.task_type,
            "status": self.status,
            "progress": str(self.progress),
            "retries": str(self.retries),
            "chain_id": self.chain_id or "",
            "max_retries": str(self.max_retries),
            "error": self.error or "",
            "secondary_error": self.secondary_error or "",
            "payload": json.dumps(self.payload),
            "result": json.dumps(self.result) if self.result is not None else "",
            "depends_on": json.dumps(self.depends_on)
            if self.depends_on
            else "[]",  # JSON string
            "created_at": self.created_at or "",
            "updated_at": self.updated_at or "",
        }

    @classmethod
    def from_dict(cls, data: dict) -> "Task":
        return cls(
            task_id=data.get("task_id"),
            task_type=data.get("task_type", ""),
            status=data.get("status", "pending"),
            chain_id=data.get("chain_id") or None,
            progress=float(data.get("progress", 0.0)),
            retries=int(data.get("retries", 0)),
            max_retries=int(data.get("max_retries", 3)),
            error=data.get("error") or None,
            secondary_error=data.get("secondary_error") or None,
            payload=json.loads(data.get("payload", "{}")),
            result=json.loads(data.get("result")) if data.get("result") else None,
            depends_on=json.loads(data.get("depends_on", "[]")),  # parse JSON list
            created_at=data.get("created_at") or datetime.now().isoformat(),
            updated_at=data.get("updated_at") or None,
        )
