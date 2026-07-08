"""
feature_test_main.py

A runnable integration-style test harness for Broccoli core features.

What it covers:
1) Priority scheduling (lower priority number runs first)
2) Dependency scheduling (child waits for parent)
3) Retry + dead-letter + dead-letter requeue
4) Chain queue execution and chain completion status

Usage:
    python /home/runner/work/Broccoli/Broccoli/feature_test_main.py

Optional env vars:
    BROCCOLI_REDIS_URL=redis://localhost:6379
"""

import logging
import os
import threading
import time
from typing import Callable

from broccoli.core.chain.task_chain import TaskChain
from broccoli.core.redis_controller import RedisController
from broccoli.core.task.task import Task
from broccoli.core.task.task_queue import TaskQueue
from broccoli.core.task.task_registry import TaskRegistry
from broccoli.logging_config import setup_logging
from broccoli.workers.chain_worker import ChainWorker
from broccoli.workers.threaded_worker import ThreadedWorker

REDIS_URL = os.getenv("BROCCOLI_REDIS_URL", "redis://localhost:6379")
registry = TaskRegistry()


# -------------------------
# Task handlers for scenarios
# -------------------------
execution_log = []
requeue_attempts = set()
setup_logging()
logger = logging.getLogger(__name__)


@registry.register("priority_marker")
def priority_marker(payload):
    execution_log.append(payload["label"])
    return {"ok": True, "label": payload["label"]}


@registry.register("dependency_parent")
def dependency_parent(payload):
    execution_log.append("parent")
    return {"ok": True}


@registry.register("dependency_child")
def dependency_child(payload):
    execution_log.append("child")
    return {"ok": True}


@registry.register("fail_once_then_pass")
def fail_once_then_pass(payload):
    task_id = payload["task_id"]
    if task_id not in requeue_attempts:
        requeue_attempts.add(task_id)
        raise RuntimeError("Intentional first-attempt failure")
    return {"ok": True, "task_id": task_id}


@registry.register("chain_step_one")
def chain_step_one(payload):
    execution_log.append("chain_step_one")
    return {"step": 1}


@registry.register("chain_step_two")
def chain_step_two(payload):
    execution_log.append("chain_step_two")
    return {"step": 2}


# -------------------------
# Helpers
# -------------------------
def assert_true(condition: bool, message: str):
    if not condition:
        raise AssertionError(message)


def wait_for(
    condition: Callable[[], bool], timeout: float = 12.0, interval: float = 0.1
):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if condition():
            return True
        time.sleep(interval)
    return False


def run_worker_until_drained(worker, queue, timeout: float = 12.0):
    t = threading.Thread(target=worker.start, daemon=True)
    t.start()

    drained = wait_for(queue.is_fully_drained, timeout=timeout)
    worker.stop()
    t.join(timeout=2.0)

    assert_true(
        drained, f"Queue {queue.get_queue_name()} did not drain within {timeout}s"
    )


# -------------------------
# Scenarios
# -------------------------
def scenario_priority_scheduling():
    print("\n[1/4] Testing priority scheduling...")
    execution_log.clear()
    queue = TaskQueue(redis_url=REDIS_URL)

    # Insert in mixed order; expect priority 0 first, then 2, then 5.
    task = Task(task_type="priority_marker", payload={"label": "p5"})
    queue.push(task, priority=5)
    print(f"Pushed task: {task.task_id} with priority 5")
    task = Task(task_type="priority_marker", payload={"label": "p0"})
    queue.push(task, priority=0)
    print(f"Pushed task: {task.task_id} with priority 0")
    task = Task(task_type="priority_marker", payload={"label": "p2"})
    queue.push(task, priority=2)
    print(f"Pushed task: {task.task_id} with priority 2")

    worker = ThreadedWorker(redis_url=REDIS_URL, max_workers=1)
    run_worker_until_drained(worker, queue)
    print(f"Execution log: {execution_log}")


def scenario_dependency_scheduling():
    print("\n[2/4] Testing dependency scheduling...")
    execution_log.clear()
    queue = TaskQueue(redis_url=REDIS_URL)

    parent = Task(task_type="dependency_parent", payload={})
    child = Task(task_type="dependency_child", payload={}, depends_on=parent.task_id)

    queue.push(parent, priority=1)
    queue.push(child, priority=1)

    waiting = queue.get_waiting_for(parent.task_id)
    assert_true(child.task_id in waiting, "Child task should be waiting on parent")

    worker = ThreadedWorker(redis_url=REDIS_URL, max_workers=1)
    run_worker_until_drained(worker, queue)

    assert_true(
        execution_log == ["parent", "child"],
        f"Dependency order incorrect: {execution_log}",
    )
    print("PASS: Dependency scheduling works")


def scenario_dead_letter_requeue():
    print("\n[3/4] Testing retry/dead-letter/requeue...")
    queue = TaskQueue(redis_url=REDIS_URL)

    task = Task(
        task_type="fail_once_then_pass",
        payload={},
        max_retries=1,
    )
    task.payload["task_id"] = task.task_id

    queue.push(task, priority=1)

    # First run: task fails permanently -> dead letter
    worker = ThreadedWorker(redis_url=REDIS_URL, max_workers=1)
    run_worker_until_drained(worker, queue)

    redis_client = RedisController(REDIS_URL).get_client()
    dead_score = redis_client.zscore(f"{queue.task_prefix}:dead_letter", task.task_id)
    assert_true(dead_score is not None, "Task should be in dead-letter after first run")

    # Requeue from dead-letter, then run again; handler succeeds second attempt.
    requeued = queue.requeue_dead(task.task_id)
    assert_true(requeued, "requeue_dead should return True")

    worker2 = ThreadedWorker(redis_url=REDIS_URL, max_workers=1)
    run_worker_until_drained(worker2, queue)

    dead_score_after = redis_client.zscore(
        f"{queue.task_prefix}:dead_letter", task.task_id
    )
    assert_true(
        dead_score_after is None,
        "Task should no longer be in dead-letter after successful replay",
    )
    print("PASS: Dead-letter and requeue flow works")


def scenario_chain_execution():
    print("\n[4/4] Testing chain execution...")
    execution_log.clear()

    chain = TaskChain(redis_url=REDIS_URL)
    chain_id = chain.chain(
        [
            {"task_type": "chain_step_one", "payload": {}},
            {"task_type": "chain_step_two", "payload": {}},
        ]
    )

    queue = TaskQueue(
        redis_url=REDIS_URL,
        queue_name="chain_tasks:queue",
        task_prefix="chain",
    )
    worker = ChainWorker(redis_url=REDIS_URL)
    run_worker_until_drained(worker, queue)

    status = chain.get_chain_status(chain_id)
    if isinstance(status, dict):
        raw_status = status.get("status")
        if isinstance(raw_status, bytes):
            raw_status = raw_status.decode()
    else:
        raw_status = None

    assert_true(
        raw_status == "completed", f"Expected completed chain status, got: {status}"
    )
    assert_true(
        execution_log == ["chain_step_one", "chain_step_two"],
        f"Unexpected chain execution order: {execution_log}",
    )
    print("PASS: Chain scheduling and execution works")


def main():
    print("BROCCOLI FEATURE TEST HARNESS")
    print(f"Redis URL: {REDIS_URL}")

    # Clean Redis for deterministic results.
    RedisController(REDIS_URL).delete_all_keys()

    scenario_priority_scheduling()
    scenario_dependency_scheduling()
    scenario_dead_letter_requeue()
    scenario_chain_execution()

    print("\nAll scenarios passed.")


if __name__ == "__main__":
    main()
