import logging

from broccoli.core.chain import Chain
from broccoli.core.chain_mixin import ChainWorkerMixin
from broccoli.core.redis_controller import RedisController
from broccoli.core.result import ResultBackend
from broccoli.workers.base_worker import BaseWorker

logger = logging.getLogger(__name__)


class ChainWorker(ChainWorkerMixin, BaseWorker):
    def __init__(self, redis_url="redis://localhost:6379"):
        super().__init__(redis_url, chain=True)  # Initialize BaseWorker with chain=True

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
        self._redis.delete(f"chain:{chain_id}")  # Clean up chain data from Redis
        self._redis.delete(
            f"chain:{chain_id}:tasks"
        )  # Clean up chain tasks data from Redis

        logger.info(f"Chain {chain_id} finished with final result: {final_result}")

    def on_finish(self, chain_id: str, final_result: any) -> None:
        """Hook called when the entire chain is finished. Override in your worker."""
        self.on_chain_finished({"chain_id": chain_id, "result": final_result})
