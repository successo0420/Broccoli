# test_chain.py
import logging

from broccoli.core.chain.task_chain import TaskChain
from broccoli.core.redis_controller import RedisController
from broccoli.core.task.task_registry import TaskRegistry
from broccoli.workers.chain_worker import ChainWorker

logging.basicConfig(level=logging.INFO)

# Register tasks
registry = TaskRegistry()


@registry.register("add")
def add_task(payload):
    a = payload.get("a", 0)
    b = payload.get("b", 0)
    result = a + b
    print(f"Add: {a} + {b} = {result}")
    return result


@registry.register("multiply")
def multiply_task(payload):
    # Uses previous result from chain
    value = payload.get("__previous_result", 1)
    multiplier = payload.get("multiplier", 2)
    result = value * multiplier
    print(f"Multiply: {value} * {multiplier} = {result}")
    return result


@registry.register("square")
def square_task(payload):
    value = payload.get("__previous_result", 1)
    result = value**2
    print(f"Square: {value}^2 = {result}")
    return result


@registry.register("chain_finished")
def chain_finished_task(payload):
    print("we done done")


# RedisController().delete_all_keys()  # Clear Redis for a clean test run
# Chain tasks together
chain = TaskChain()
chain_id = chain.chain(
    [
        {"task_type": "add", "payload": {"a": 5, "b": 3}},
        {"task_type": "multiply", "payload": {"multiplier": 4}},
        {"task_type": "square", "payload": {}},
    ],
    completion_task="chain_finished",
)


# Start worker to process chain
worker = ChainWorker()
worker.start()  # Process: 5+3=8, 8*4=32, 32^2=1024
