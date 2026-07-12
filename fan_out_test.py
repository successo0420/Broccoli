#!/usr/bin/env python3
"""
Quick test for multi‑dependency support in TaskQueue.

Creates three parent tasks (A, B, C) and one dependent task (D)
that depends on all three.  Workers simulate completion of parents
and verify that D is only enqueued after the last parent finishes.

Assumptions:
- Redis is running on localhost:6379
- Task and TaskQueue are importable from the correct paths.
  Adjust the import statements below if your project structure differs.
"""

import logging
import sys
import time

# Adjust these imports to match your project layout
from broccoli.core.redis_controller import RedisController
from broccoli.core.task.task import Task
from broccoli.core.task.task_queue import TaskQueue
from broccoli.core.task.task_registry import TaskRegistry
from broccoli.workers.base_worker import BaseWorker

# Set up logging to see what's happening
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

registry = TaskRegistry()


@registry.register("parent")
def parent(payload: dict):
    """Simulate a parent task that takes some time to complete."""
    logger.info(f"Parent task {payload['name']} started.")
    time.sleep(1)  # Simulate work
    logger.info(f"Parent task {payload['name']} completed.")


RedisController().get_client().flushdb()  # Clear Redis for a clean test
# Use a unique queue name to avoid interfering with other tests
queue_name = "test:multi_dep:queue"
task_prefix = "test_task"

# Initialize the queue
queue = TaskQueue(
    queue_name=queue_name,
    task_prefix=task_prefix,
)

# ------------------------------------------------------------------
# 1. Create three parent tasks (A, B, C)
# ------------------------------------------------------------------
task_a = Task(task_type="parent", payload={"name": "A"})
task_b = Task(task_type="parent", payload={"name": "B"})
task_c = Task(task_type="parent", payload={"name": "C"})

logger.info("Pushing parents A, B, C...")
queue.push(task_a, priority=5)
queue.push(task_b, priority=1)
queue.push(task_c, priority=3)

# ------------------------------------------------------------------
# 2. Create dependent task D that waits for A, B, and C
# ------------------------------------------------------------------
task_d = Task(
    task_type="parent",
    payload={"name": "D"},
    depends_on=[task_a.task_id, task_b.task_id, task_c.task_id],
)
logger.info(f"Pushing dependent D with depends_on: {task_d.depends_on}")
queue.push(task_d, priority=2)  # higher priority doesn't matter because it waits
BaseWorker(queue_name=queue_name, task_prefix=task_prefix).start()
