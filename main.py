# tests/test_workers.py
import logging
import random
import time

from broccoli.core.redis_controller import RedisController
from broccoli.core.result import ResultBackend
from broccoli.core.task import Task
from broccoli.core.task_queue import TaskQueue
from broccoli.core.task_registry import TaskRegistry
from broccoli.workers.async_worker import AsyncWorker
from broccoli.workers.base_worker import BaseWorker
from broccoli.workers.hybrid_worker import HybridWorker
from broccoli.workers.threaded_worker import ThreadedWorker
from broccoli.workers.worker_pool import WorkerPool

logging.basicConfig(level=logging.INFO)

# Register tasks
registry = TaskRegistry()


@registry.register("sleep")
def sleep_task(payload):
    time.sleep(payload.get("seconds", 1))
    return {"slept": payload.get("seconds", 1)}


@registry.register("add")
def add_task(payload):
    return {"result": payload.get("a", 0) + payload.get("b", 0)}


@registry.register("fail")
def fail_task(payload):
    raise Exception("Intentional failure")


@registry.register("print")
def print_task(payload):
    print(payload.get("message", "No message provided"))
    return {"status": "printed"}


def create_tasks(count=5):
    tasks = TaskQueue()
    for i in range(count):
        task = Task(task_type="print", payload={"message": f"Hello, World! {i + 1}"})
        tasks.push(task)
        print(f"Created task {task.task_id} with payload: {task.payload}")
    return tasks


def test_base_worker():
    worker = BaseWorker()
    create_tasks(5)
    worker.start()  # Processes sequentially


def test_threaded_worker():
    worker = ThreadedWorker(max_workers=3)
    create_tasks(6)
    worker.start()  # Processes 3 at a time


def test_async_worker():
    worker = AsyncWorker(max_concurrent=3)
    create_tasks(6)
    worker.start()  # Processes 3 concurrently


def test_hybrid_worker():
    worker = HybridWorker(thread_workers=2, async_tasks=3)
    create_tasks(6)
    worker.start()  # Processes with mixed threading/async


def test_worker_pool():

    pool = WorkerPool(worker_type=ThreadedWorker, num_workers=3)
    create_tasks(10)
    pool.start()


def test_all_workers():
    """Run all worker types sequentially."""
    print("\n=== Testing BaseWorker ===")
    test_base_worker()
    # print("\n=== Testing ThreadedWorker ===")
    # test_threaded_worker()
    # print("\n=== Testing AsyncWorker ===")
    # test_async_worker()
    # print("\n=== Testing WorkerPool ===")
    # test_worker_pool()
    # print("\n=== Testing HybridWorker ===")
    # test_hybrid_worker()


if __name__ == "__main__":
    # queue = TaskQueue(redis_url="redis://localhost:6379")
    # RedisController(
    #     "redis://localhost:6379"
    # ).delete_all_keys()  # Clear Redis before testing
    test_all_workers()
