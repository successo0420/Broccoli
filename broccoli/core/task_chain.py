# video_scheduler/core/task_chain.py
import json
import logging
import uuid
from typing import Any, Dict, List

from broccoli.core.task import Task
from broccoli.core.task_queue import TaskQueue
from broccoli.core.task_registry import TaskRegistry

logger = logging.getLogger(__name__)


class TaskChain:
    """Chain multiple tasks together where each task passes its result to the next."""

    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.queue = TaskQueue(redis_url)
        self.registry = TaskRegistry()
        self.chain_id = str(uuid.uuid4())

    def chain(
        self, tasks: List[Dict[str, Any]], shared_payload: Dict[str, Any] = None
    ) -> str:
        """
        Chain tasks together.

        Args:
            tasks: List of task configurations, each with 'task_type' and 'payload'
            shared_payload: Data passed to all tasks in the chain

        Returns:
            chain_id: ID for tracking the entire chain

        Example:
            chain = TaskChain()
            chain.chain([
                {"task_type": "download_file", "payload": {"url": "https://..."}},
                {"task_type": "process_video", "payload": {"quality": "high"}},
                {"task_type": "upload_file", "payload": {"bucket": "my-bucket"}}
            ])
        """
        if not tasks:
            raise ValueError("Cannot chain empty task list")

        # Store chain metadata
        chain_metadata = {
            "chain_id": self.chain_id,
            "total_tasks": len(tasks),
            "completed_tasks": 0,
            "failed": False,
            "current_task": 0,
            "status": "pending",
        }

        # Save chain metadata to Redis
        self.queue.redis.hset(
            f"chain:{self.chain_id}",
            mapping={k: str(v) for k, v in chain_metadata.items()},
        )

        # Create first task with chain info
        first_task = tasks[0]
        task = Task(
            task_type=first_task["task_type"],
            payload={
                **first_task.get("payload", {}),
                **(shared_payload or {}),
                "__chain_id": self.chain_id,
                "__chain_position": 0,
                "__total_tasks": len(tasks),
                "__is_first": True,
            },
            max_retries=first_task.get("max_retries", 3),
        )

        # Store all task configs for reference
        self.queue.redis.set(
            ex=86400,  # 24 hours TTL
            name=f"chain:{self.chain_id}:tasks",
            value=json.dumps(tasks),
        )

        # Push first task
        self.queue.push(task)
        logger.info(f"Chain {self.chain_id} started with task {task.task_id}")

        return self.chain_id

    def continue_chain(self, previous_task: Task, previous_result: Any) -> None:
        """
        Continue the chain after a task completes.
        Called by the worker's post_process hook.
        """
        chain_id = previous_task.payload.get("__chain_id")
        if not chain_id:
            return

        position = previous_task.payload.get("__chain_position", 0)
        total_tasks = previous_task.payload.get("__total_tasks", 0)

        # Update chain progress
        self.queue.redis.hincrby(f"chain:{chain_id}", "completed_tasks", 1)
        self.queue.redis.hset(f"chain:{chain_id}", "current_task", position + 1)

        # Check if chain is complete
        if position + 1 >= total_tasks:
            self.queue.redis.hset(f"chain:{chain_id}", "status", "completed")
            logger.info(f"Chain {chain_id} completed successfully")
            return

        # Get next task configuration
        tasks_json = self.queue.redis.get(f"chain:{chain_id}:tasks")
        if not tasks_json:
            logger.error(f"Chain {chain_id} tasks not found")
            return

        tasks = json.loads(tasks_json)
        next_task_config = tasks[position + 1]

        # Create next task
        next_task = Task(
            task_type=next_task_config["task_type"],
            payload={
                **next_task_config.get("payload", {}),
                "__chain_id": chain_id,
                "__chain_position": position + 1,
                "__total_tasks": total_tasks,
                "__previous_result": previous_result,
                "__is_first": False,
            },
            max_retries=next_task_config.get("max_retries", 3),
        )

        # Push next task
        self.queue.push(next_task)
        logger.info(
            f"Chain {chain_id} continuing with task {next_task.task_id} (position {position + 1})"
        )

    def get_chain_status(self, chain_id: str) -> Dict[str, Any]:
        """Get the status of a chain."""
        data = self.queue.redis.hgetall(f"chain:{chain_id}")
        if not data:
            return {"status": "not_found"}

        return {
            "chain_id": chain_id,
            "total_tasks": int(data.get("total_tasks", 0)),
            "completed_tasks": int(data.get("completed_tasks", 0)),
            "current_task": int(data.get("current_task", 0)),
            "status": data.get("status", "unknown"),
            "failed": data.get("failed", "False") == "True",
        }


class ChainWorkerMixin:
    """Mixin to add chain support to a worker."""

    def post_process(self, task: Task, success: bool) -> None:
        """Override this in your worker and call super().post_process()."""
        # Check if this task is part of a chain
        chain_id = task.payload.get("__chain_id")
        if chain_id and success:
            from broccoli.core.task_chain import TaskChain

            chain = TaskChain()
            chain.continue_chain(task, task.result)

        # Your existing post_process logic here
