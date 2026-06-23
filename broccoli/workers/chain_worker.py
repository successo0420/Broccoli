import json
import logging

from broccoli.core.chain.chain import Chain
from broccoli.core.chain.chain_mixin import ChainWorkerMixin
from broccoli.core.redis_controller import RedisController
from broccoli.core.result import ResultBackend
from broccoli.workers.base_worker import BaseWorker

logger = logging.getLogger(__name__)


class ChainWorker(ChainWorkerMixin, BaseWorker):
    def __init__(self, redis_url="redis://localhost:6379"):
        super().__init__(redis_url, queue_name="chain_tasks:queue", task_prefix="chain")

        self.registry.register_manually(
            "on_chain_finished",
            self.on_chain_finished,
        )

        self.result_backend = ResultBackend(redis_url)
        self._redis = RedisController(redis_url).get_client()

    def on_chain_finished(self, payload):
        chain_id = payload.get("chain_id")
        final_result = payload.get("result")
        chain = Chain.from_dict(self._redis.hgetall(f"chain:{chain_id}"))
        chain.result = final_result
        self.result_backend.store_chain(chain)
        self.cleanup(chain_id)
        logger.info(f"Chain {chain_id} finished with final result: {final_result}")

    def cleanup(self, chain_id: str):
        """Clean up all chain and task keys from Redis, leaving only the stored result."""
        tasks_raw = self._redis.get(f"chain:{chain_id}:tasks")
        if not tasks_raw:
            logger.warning(f"No tasks list found for chain {chain_id} during cleanup")
            return

        tasks = json.loads(tasks_raw)
        for task in tasks:
            task_id = task.get("task_id")
            if task_id:
                # Delete both possible key prefixes — chain_mixin deletes chain: during
                # normal flow, but we also cover task: in case _update_task wrote it,
                # and catch any stragglers if cleanup runs before mixin does.
                self._redis.delete(f"chain:{task_id}")
                self._redis.delete(f"task:{task_id}")
                logger.info(f"Cleaned up task {task_id}")

        self._redis.delete(f"chain:{chain_id}")
        self._redis.delete(f"chain:{chain_id}:tasks")
        logger.info(f"Cleaned up chain {chain_id}")

    def on_finish(self, chain_id: str, final_result: any) -> None:
        """Hook called when the entire chain is finished. Override in your worker."""
        self.on_chain_finished({"chain_id": chain_id, "result": final_result})
