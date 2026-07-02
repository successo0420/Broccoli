# broccoli/workers/chain_worker.py
import json
import logging

from broccoli.core.chain.chain import Chain
from broccoli.core.chain.chain_mixin import ChainWorkerMixin
from broccoli.core.redis_controller import RedisController
from broccoli.core.result import ResultBackend
from broccoli.workers.base_worker import BaseWorker

logger = logging.getLogger(__name__)


class ChainWorker(ChainWorkerMixin, BaseWorker):
    """
    Worker that processes chained tasks.

    Each step in a chain carries ``__chain_id`` in its payload so that
    ``post_process`` in BaseWorker skips individual result storage —
    the chain result is written atomically by ``on_chain_finished``.

    With the new TaskQueue design, dependency release between chain steps
    happens automatically inside ``queue.complete()`` when a step finishes,
    provided each step's ``depends_on`` is set to the previous step's task_id
    at push time.  ``ChainWorkerMixin`` is responsible for setting that up.
    """

    def __init__(self, redis_url: str = "redis://localhost:6379"):
        super().__init__(
            redis_url=redis_url,
            queue_name="chain_tasks:queue",
            task_prefix="chain",
        )

        # The final step of every chain pushes an "on_chain_finished" task
        # whose handler ties everything together.
        self.registry.register_manually(
            "on_chain_finished",
            self.on_chain_finished,
        )

        self.result_backend = ResultBackend(redis_url)
        self._redis = RedisController(redis_url).get_client()

    # ------------------------------------------------------------------
    # Chain completion
    # ------------------------------------------------------------------

    def on_chain_finished(self, payload: dict):
        """
        Registered task handler called when the last step in a chain completes.

        Reads the chain record, attaches the final result, persists it via the
        result backend, and runs full cleanup.
        """
        chain_id = payload.get("chain_id")
        final_result = payload.get("result")

        chain = Chain.from_dict(self._redis.hgetall(f"chain:{chain_id}"))
        chain.result = final_result
        self.result_backend.store_chain(chain)
        self.cleanup(chain_id)

    def cleanup(self, chain_id: str) -> None:
        """
        Remove all per-step task hashes and chain metadata from Redis.

        Queue sorted-set entries have already been removed by the TaskQueue
        methods (``complete`` / ``fail``) as each step finished — this method
        only deletes leftover hash keys.
        """
        tasks_raw = self._redis.get(f"chain:{chain_id}:tasks")
        if not tasks_raw:
            logger.warning(f"No task list found for chain {chain_id} during cleanup")
            return

        tasks = json.loads(tasks_raw)
        for task in tasks:
            task_id = task.get("task_id")
            if task_id:
                self._redis.delete(f"{self.task_prefix}:{task_id}")
                logger.debug(f"Deleted task hash for {task_id}")

        self._redis.delete(f"chain:{chain_id}")
        self._redis.delete(f"chain:{chain_id}:tasks")
        logger.info(f"Chain {chain_id} cleaned up")

    def on_finish(self, chain_id: str, final_result) -> None:
        """
        Hook called when the entire chain is finished.
        Override in subclasses for custom post-chain logic.
        """
        self.on_chain_finished({"chain_id": chain_id, "result": final_result})
