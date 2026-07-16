# video_scheduler/core/task_chain.py
import json
import logging
import uuid
from typing import Any, Dict, List, Optional, Union

from broccoli.core.chain.chain import Chain
from broccoli.core.redis_controller import RedisController
from broccoli.core.task.task import Task
from broccoli.core.task.task_queue import TaskQueue
from broccoli.core.task.task_registry import TaskRegistry

logger = logging.getLogger(__name__)


class TaskChain:
    """Chain multiple tasks together where each task passes its result to the next."""

    def __init__(self, redis_url: str = "redis://localhost:6379", chain_id: str = None):
        self.queue = TaskQueue(
            redis_url=redis_url, queue_name="chain_tasks:queue", task_prefix="chain"
        )
        self._redis = RedisController(redis_url).get_client()
        self.registry = TaskRegistry()
        self.chain_id = chain_id or str(uuid.uuid4())

    def chain(
        self,
        tasks: List[Union[Dict[str, Any], Task]],
        shared_payload: Dict[str, Any] = None,
        completion_task: str = None,
        completion_payload: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Chain tasks together.

        Accepts either a list of task dictionaries (with 'task_type', 'payload', etc.)
        or a list of pre‑instantiated Task objects.

        Args:
            tasks: List of task configurations (dict) or Task objects.
            shared_payload: Data passed to all tasks in the chain.
            completion_task: Optional task type to run after the chain completes.
            completion_payload: Data passed to the completion task.

        Returns:
            chain_id: ID for tracking the entire chain.
        """
        if not tasks:
            raise ValueError("Cannot chain empty task list")

        # --- 1. Store chain metadata ---
        chain = Chain(
            chain_id=self.chain_id,
            total_tasks=len(tasks),
            completed_tasks=0,
            failed=False,
            status="pending",
        )
        if completion_task:
            chain.completion_task = completion_task

        self._redis.hset(
            f"chain:{self.chain_id}",
            mapping={k: str(v) for k, v in chain.to_dict().items()},
        )

        # --- 2. Normalise and assign task IDs ---
        normalised = []  # list of dicts with at least 'task_type', 'payload', 'task_id', 'max_retries'

        for item in tasks:
            if isinstance(item, Task):
                # Unpack the Task object
                task_dict = {
                    "task_type": item.task_type,
                    "payload": item.payload.copy(),  # avoid mutation
                    "task_id": item.task_id,  # preserve original ID
                    "max_retries": item.max_retries,
                }
            else:
                # Assume dict
                task_dict = item.copy()
                if "task_id" not in task_dict:
                    task_dict["task_id"] = str(uuid.uuid4())
                if "max_retries" not in task_dict:
                    task_dict["max_retries"] = 3
            normalised.append(task_dict)

        # Store all task configs for reference (e.g., chain inspection)
        self._redis.setex(
            f"chain:{self.chain_id}:tasks",
            86400,  # 24h TTL
            json.dumps(normalised),
        )

        # --- 3. Push each step with depends_on linking to the previous one ---
        prev_task_id = None
        total = len(normalised)

        for i, task_dict in enumerate(normalised):
            is_last = i == total - 1
            task = Task(
                task_type=task_dict["task_type"],
                task_id=task_dict["task_id"],
                payload={
                    **task_dict.get("payload", {}),
                    **(shared_payload or {}),
                    "__chain_id": self.chain_id,
                    "__chain_position": i,
                    "__total_tasks": total,
                    "__previous_task_id": prev_task_id,
                    "__is_last_task": is_last,
                },
                max_retries=task_dict.get("max_retries", 3),
                depends_on=[prev_task_id] if prev_task_id else None,
            )
            self.queue.push(task)
            prev_task_id = task.task_id
            logger.info(
                f"Pushed task {task.task_id} (position {i}) for chain {self.chain_id}"
            )

        # --- 4. Completion task (if any) depends on the last step ---
        if completion_task:
            last_task_id = normalised[-1]["task_id"]
            comp_payload = {
                "chain_id": self.chain_id,
                "last_task_id": last_task_id,
                **(completion_payload or {}),
            }
            comp_task = Task(
                task_type=completion_task,
                payload=comp_payload,
                chain_id=self.chain_id,
                depends_on=[last_task_id],
                max_retries=3,
            )
            self.queue.push(comp_task)
            logger.info(
                f"Pushed completion task {comp_task.task_id} for chain {self.chain_id}"
            )

        logger.info(f"Chain {self.chain_id} started with {len(normalised)} steps")
        return self.chain_id

    def get_chain_status(self, chain_id: str) -> Dict[str, Any]:
        """Get the status of a chain."""
        data = self._redis.hgetall(f"chain:{chain_id}")
        if not data:
            data = self._redis.get(f"result:{chain_id}")
            if not data:
                return {"status": "Not Found"}

        return data
