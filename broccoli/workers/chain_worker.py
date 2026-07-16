# broccoli/workers/chain_worker.py
import json
import logging
from typing import Any

from broccoli.core.chain.chain import Chain
from broccoli.core.redis_controller import RedisController
from broccoli.core.result import ResultBackend
from broccoli.core.task.task import Task
from broccoli.workers.base_worker import BaseWorker

logger = logging.getLogger(__name__)


class ChainWorker(BaseWorker):
    """
    Worker specialised for chain tasks.

    It registers a handler for the completion task (default: "on_chain_finished")
    and updates chain progress when each step finishes.
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        worker_id: str = None,
        queue_name: str = "chain_tasks:queue",
        task_prefix: str = "chain",
        recover_on_startup: bool = True,
        recover_stalled_timeout: int = 3600,
        decode_responses: bool = True,
        redis_config: dict = None,
    ):
        super().__init__(
            redis_url=redis_url,
            worker_id=worker_id,
            queue_name=queue_name,
            task_prefix=task_prefix,
            recover_on_startup=recover_on_startup,
            recover_stalled_timeout=recover_stalled_timeout,
            decode_responses=decode_responses,
            redis_config=redis_config,
        )

        self.result_backend = ResultBackend(
            redis_url,
            decode_responses=decode_responses,
            redis_config=redis_config,
        )
        self._redis = RedisController(
            redis_url,
            decode_responses=decode_responses,
            **(redis_config or {}),
        ).get_client()

        # Hook to update chain progress after every step
        # This runs for every processed task and no-ops for non-chain payloads.
        self.add_post_process_handler(self._update_chain_progress)
        # This runs only when the final chain marker task is processed.
        self.add_chain_completion_handler(self._on_chain_finished)

    # ------------------------------------------------------------------
    # Chain progress tracking
    # ------------------------------------------------------------------

    def _update_chain_progress(self, task: Task, result: Any) -> None:
        """Increment completed_tasks and update current_task for chain steps."""
        # Chain metadata is carried in payload fields injected by the chain
        # producer. If absent, this was a normal task and we should exit early.
        chain_id = task.payload.get("__chain_id")
        if not chain_id:
            return  # not a chain task

        # Skip for completion task (it has no __chain_position)
        position = task.payload.get("__chain_position")
        if position is None:
            return

        pipe = self._redis.pipeline()
        # Keep progress updates atomic from the perspective of readers.
        pipe.hincrby(f"chain:{chain_id}", "completed_tasks", 1)
        pipe.hset(f"chain:{chain_id}", "current_task", position + 1)
        pipe.execute()
        logger.debug(f"Chain {chain_id} progress: step {position + 1} done")

    # ------------------------------------------------------------------
    # Chain completion handler
    # ------------------------------------------------------------------

    def _on_chain_finished(self, task, payload: dict) -> None:
        """
        Default handler called when the last chain step finishes.

        It retrieves the final result from the result backend, stores the
        chain result, and cleans up Redis keys.
        """
        chain_id = payload.get("__chain_id")
        last_task_id = payload.get("__is_last_task")

        if not chain_id or not last_task_id:
            logger.error("Missing chain_id or last_task_id in completion payload")
            return

        # Load and update chain record
        chain_data = self._redis.hgetall(f"chain:{chain_id}")
        if not chain_data:
            logger.error(f"Chain {chain_id} metadata not found")
            return

        chain = Chain.from_dict(chain_data)
        # task.result here is the result produced by the final executable step.
        chain.result = task.result  # Store the final result from the last task
        chain.status = "completed"

        # Store the chain result
        self.result_backend.store_chain(chain)

        # Clean up runtime keys only after result persistence succeeds.
        self._cleanup_chain(chain_id)

        logger.info(f"Chain {chain_id} completed and cleaned up")

    def _cleanup_chain(self, chain_id: str) -> None:
        """
        Delete all per‑step task hashes and chain metadata from Redis.
        """
        # Keep deletion explicit instead of wildcard key scans to avoid
        # accidentally removing unrelated keys with similar prefixes.
        self._redis.delete(f"chain:{chain_id}")
        self._redis.delete(f"chain:{chain_id}:tasks")
        logger.info(f"Chain {chain_id} cleaned up")
