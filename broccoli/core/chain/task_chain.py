# video_scheduler/core/task_chain.py
import json
import logging
import uuid
from typing import Any, Dict, List, Optional

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
        tasks: List[Dict[str, Any]],
        shared_payload: Dict[str, Any] = None,
        completion_task: str = None,
        completion_payload: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Chain tasks together.

        Args:
            tasks: List of task configurations, each with 'task_type' and 'payload'
            shared_payload: Data passed to all tasks in the chain
            completion_task: Optional task to run after the chain completes
            completion_payload: Data passed to the completion task

        Returns:
            chain_id: ID for tracking the entire chain

        Example:
            chain = TaskChain()
            chain.chain([
                {"task_type": "download_file", "payload": {"url": "https://..."}},
                {"task_type": "process_video", "payload": {"quality": "high"}},
                {"task_type": "upload_file", "payload": {"bucket": "my-bucket"}}
            ], completion_task="on_chain_finished")
        """
        if not tasks:
            raise ValueError("Cannot chain empty task list")

        # Store chain metadata
        chain = Chain(
            chain_id=self.chain_id,
            total_tasks=len(tasks),
            completed_tasks=0,
            failed=False,
            status="pending",
        )

        if completion_task:
            chain.completion_task = completion_task

        # Save chain metadata to Redis
        self._redis.hset(
            f"chain:{self.chain_id}",
            mapping={k: str(v) for k, v in chain.to_dict().items()},
        )

        # Assign task_ids to ALL tasks first, before creating any Task objects.
        # This ensures the IDs stored in Redis match the Task objects pushed to the queue.
        for t in tasks:
            if "task_id" not in t:
                t["task_id"] = str(uuid.uuid4())

        # Store all task configs for reference
        self._redis.set(
            ex=86400,  # 24 hours TTL
            name=f"chain:{self.chain_id}:tasks",
            value=json.dumps(tasks),
        )

        # Create and push first task using the now-stable task_id
        prev_task_id = None
        for i, task_config in enumerate(tasks):
            last_task = i == len(tasks) - 1
            task = Task(
                task_type=task_config["task_type"],
                task_id=task_config["task_id"],
                chain_id=self.chain_id,
                payload={
                    **task_config.get("payload", {}),
                    **(shared_payload or {}),
                    "__chain_id": self.chain_id,
                    "__chain_position": i,
                    "__total_tasks": len(tasks),
                    "__previous_task_id": prev_task_id,
                    "__is_last_task": last_task,
                },
                max_retries=task_config.get("max_retries", 3),
                depends_on=[prev_task_id],
            )
            self.queue.push(task)
            prev_task_id = task.task_id
            logger.info(
                f"Pushed task {task.task_id} (position {i}) for chain {self.chain_id}"
            )
        if completion_task:
            last_task_id = tasks[-1]["task_id"]
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

        logger.info(f"Chain {self.chain_id} started with {len(tasks)} steps")
        return self.chain_id

    def get_chain_status(self, chain_id: str) -> Dict[str, Any]:
        """Get the status of a chain."""
        data = self._redis.hgetall(f"chain:{chain_id}")
        if not data:
            data = self._redis.get(f"result:{chain_id}")
            if not data:
                return {"status": "Not Found"}

        return data
