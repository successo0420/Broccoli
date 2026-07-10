# examples/error_task.py
import logging
import random
import time

from broccoli.core.redis_controller import RedisController
from broccoli.core.task.task import Task
from broccoli.core.task.task_queue import TaskQueue
from broccoli.core.task.task_registry import TaskRegistry
from broccoli.logging_config import setup_logging
from broccoli.workers.async_worker import AsyncWorker
from broccoli.workers.auto_scale_worker import AutoScalingWorkerPool
from broccoli.workers.base_worker import BaseWorker
from broccoli.workers.hybrid_worker import HybridWorker
from broccoli.workers.threaded_worker import ThreadedWorker

# setup_logging()
# logger = logging.getLogger(__name__)
registry = TaskRegistry()
i = 1
RedisController().get_client().flushdb()  # Clear Redis before tests


@registry.register("error_task")
def error_task(payload: dict) -> dict:
    """
    A test task that randomly fails ~50% of the time.
    """
    number = random.random()
    # print(f"Random number generated: {number}")
    if number < 0.3:
        # logger.warning("Task failed randomly!")
        raise RuntimeError("Simulated random failure (50% chance)")

    # logger.info("Task succeeded!")
    return {"status": "ok", "data": payload}


queue = TaskQueue(queue_name="tasks:queue")  # or chain_tasks:queue
for i in range(500):
    task = Task(
        task_type="error_task",
        payload={"test": "data"},
        max_retries=3,  # allow up to 3 retries
    )
    queue.push(task)
    # print(f"Pushed task {task.task_id}")

start = time.time()
worker = AutoScalingWorkerPool(check_interval=1)


def on_complete():
    worker.stop()


worker.add_worker_completion_handler(on_complete)


worker.start()


end = time.time()
print(f"Total time taken: {end - start} seconds")
