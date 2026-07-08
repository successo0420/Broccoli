#!/usr/bin/env python3
"""
Broccoli Task Queue CLI

Usage examples:
  # Start a threaded worker pool (4 workers)
  broccoli worker start --type threaded --pool --num-workers 4

  # Start a single async worker with 10 concurrent tasks
  broccoli worker start --type async --concurrency 10

  # Start a chain worker (single)
  broccoli worker start --type chain

  # Inspect queue stats
  broccoli queue stats

  # List pending tasks
  broccoli queue list --status pending --limit 5

  # Get a task by ID
  broccoli queue get <task_id>

  # See which tasks are waiting for a given parent
  broccoli queue waiting <parent_id>

  # List dead-letter tasks
  broccoli dead list

  # Requeue a dead task (retry it)
  broccoli dead requeue <task_id>

  # Get chain status
  broccoli chain status <chain_id>

  # List tasks in a chain
  broccoli chain tasks <chain_id>

  # Health check (returns exit code 0 if Redis is reachable)
  broccoli health
"""

import argparse
import json
import logging
import os
import sys
from typing import Any, Dict, List, Optional

from broccoli.core.chain.chain_queue import ChainQueue
from broccoli.core.chain.task_chain import TaskChain

# Import Broccoli components
from broccoli.core.redis_controller import RedisController
from broccoli.core.task.task import Task
from broccoli.core.task.task_queue import TaskQueue
from broccoli.logging_config import setup_logging as configure_logging
from broccoli.workers.async_worker import AsyncWorker
from broccoli.workers.base_worker import BaseWorker
from broccoli.workers.chain_worker import ChainWorker
from broccoli.workers.hybrid_worker import HybridWorker
from broccoli.workers.threaded_worker import ThreadedWorker
from broccoli.workers.worker_pool import WorkerPool

# Defaults from environment variables
DEFAULT_REDIS_URL = os.getenv("BROCCOLI_REDIS_URL", "redis://localhost:6379")
DEFAULT_QUEUE_NAME = os.getenv("BROCCOLI_QUEUE_NAME", "tasks:queue")
DEFAULT_CHAIN_QUEUE_NAME = os.getenv("BROCCOLI_CHAIN_QUEUE_NAME", "chain_tasks:queue")
DEFAULT_TASK_PREFIX = os.getenv("BROCCOLI_TASK_PREFIX", "task")


def setup_logging(verbose: int):
    """Configure logging based on verbosity count."""
    if verbose >= 2:
        level = logging.DEBUG
    elif verbose == 1:
        level = logging.INFO
    else:
        level = logging.WARNING
    configure_logging(level)


def print_table(headers: List[str], rows: List[List[Any]]):
    """Pretty-print a table with aligned columns."""
    if not rows:
        print("No data.")
        return
    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(str(cell)))
    fmt = "  ".join(f"{{:<{w}}}" for w in col_widths)
    print(fmt.format(*headers))
    print("-" * (sum(col_widths) + 2 * (len(headers) - 1)))
    for row in rows:
        print(fmt.format(*(str(c) for c in row)))


def output_json(data: Any):
    """Print JSON with indentation."""
    print(json.dumps(data, indent=2, default=str))


def output_result(data: Any, fmt: str):
    """Print data in either JSON or table format."""
    if fmt == "json":
        output_json(data)
    else:
        if isinstance(data, list) and data and isinstance(data[0], dict):
            # Try to render as table using dict keys
            headers = list(data[0].keys())
            rows = [[row.get(k, "") for k in headers] for row in data]
            print_table(headers, rows)
        elif isinstance(data, dict):
            # Print key-value pairs
            for k, v in data.items():
                print(f"{k}: {v}")
        else:
            print(data)


def get_queue(args) -> TaskQueue:
    """Instantiate a TaskQueue with CLI args."""
    return TaskQueue(
        redis_url=args.redis_url,
        queue_name=args.queue_name,
        task_prefix=args.task_prefix,
    )


def get_chain_queue(args) -> ChainQueue:
    """Instantiate a ChainQueue with CLI args."""
    return ChainQueue(
        redis_url=args.redis_url,
        queue_name=args.chain_queue_name,
        task_prefix=args.task_prefix,
    )


def get_task_chain(args) -> TaskChain:
    """Instantiate a TaskChain with CLI args."""
    return TaskChain(redis_url=args.redis_url)


# ============ Worker start ============


def cmd_worker_start(args):
    """Start a worker or worker pool."""
    setup_logging(args.verbose)

    # Map worker types to classes
    worker_classes = {
        "base": BaseWorker,
        "threaded": ThreadedWorker,
        "async": AsyncWorker,
        "hybrid": HybridWorker,
        "chain": ChainWorker,
    }
    worker_class = worker_classes[args.type]

    # Determine which queue name and task prefix to use
    if args.type == "chain":
        queue_name = args.chain_queue_name
    else:
        queue_name = args.queue_name

    # Common worker kwargs
    worker_kwargs = {
        "redis_url": args.redis_url,
        "worker_id": args.worker_id,
        "queue_name": queue_name,
        "task_prefix": args.task_prefix,
    }

    # Add type-specific kwargs
    if args.type == "threaded":
        worker_kwargs["max_workers"] = args.concurrency
    elif args.type == "async":
        worker_kwargs["max_concurrent"] = args.concurrency
    elif args.type == "hybrid":
        worker_kwargs["thread_workers"] = args.thread_workers
        worker_kwargs["async_tasks"] = args.async_tasks
    # ChainWorker inherits BaseWorker and uses default args; no extra needed

    # Optionally recover stalled tasks before starting
    if args.recover_stalled > 0:
        # We need a queue instance to call recover_stalled
        temp_queue = TaskQueue(
            redis_url=args.redis_url,
            queue_name=queue_name,
            task_prefix=args.task_prefix,
        )
        recovered = temp_queue.recover_stalled(args.recover_stalled)
        logging.info(
            f"Recovered {recovered} stalled tasks (timeout={args.recover_stalled}s)"
        )

    if args.pool:
        pool = WorkerPool(
            worker_type=worker_class,
            num_workers=args.num_workers,
            redis_url=args.redis_url,
            **worker_kwargs,
        )
        pool.start()
    else:
        worker = worker_class(**worker_kwargs)
        worker.start()


# ============ Queue commands ============


def cmd_queue_stats(args):
    """Show queue statistics."""
    q = get_queue(args)
    stats = q.stats()
    # Also get waiting tasks count? Not directly available; we can count all dependency keys?
    # We'll just show what we have.
    output_result(stats, args.format)


def cmd_queue_list(args):
    """List task IDs matching a status."""
    q = get_queue(args)
    # We need to scan all task hashes or use a status index? Not available.
    # We can use Redis SCAN with pattern to find tasks by status? That's expensive.
    # Instead, we can use the queue sorted set for pending tasks, processing set for in_progress,
    # and we can scan for waiting status? There's no index for waiting.
    # We'll provide a limited set: pending (from queue), in_progress (from processing), and completed/failed/waiting are not directly listable without scanning all task hashes.
    # For simplicity, we'll only support pending and in_progress, and maybe we can scan all task keys for a demo.
    # Better: we'll implement a scan over `task:*` and filter by status. This is not efficient but works for small/medium workloads.
    if args.status not in (
        "pending",
        "in_progress",
        "completed",
        "failed",
        "waiting",
        "all",
    ):
        print(
            f"Invalid status: {args.status}. Must be one of: pending, in_progress, completed, failed, waiting, all",
            file=sys.stderr,
        )
        sys.exit(1)

    redis_client = RedisController(args.redis_url).get_client()
    task_keys = redis_client.keys(f"{args.task_prefix}:*")
    tasks = []
    for key in task_keys:
        # key is bytes or str
        task_data = redis_client.hgetall(key)
        if not task_data:
            continue
        # decode bytes if needed
        if isinstance(task_data, dict):
            # Convert bytes keys/values to str
            decoded = {}
            for k, v in task_data.items():
                if isinstance(k, bytes):
                    k = k.decode()
                if isinstance(v, bytes):
                    v = v.decode()
                decoded[k] = v
            task_data = decoded
        status = task_data.get("status")
        if args.status != "all" and status != args.status:
            continue
        tasks.append(
            {
                "task_id": task_data.get("task_id"),
                "task_type": task_data.get("task_type"),
                "status": status,
                "created_at": task_data.get("created_at"),
            }
        )
    # Sort by created_at
    tasks.sort(key=lambda x: x.get("created_at", ""))
    if args.limit and len(tasks) > args.limit:
        tasks = tasks[: args.limit]
    output_result(tasks, args.format)


def cmd_queue_get(args):
    """Fetch and display a task by ID."""
    q = get_queue(args)
    task = q.get_task(args.task_id)
    if not task:
        print(f"Task {args.task_id} not found", file=sys.stderr)
        sys.exit(1)
    # Convert to dict for output
    data = task.to_dict()
    output_result(data, args.format)


def cmd_queue_waiting(args):
    """Show tasks waiting for a specific parent."""
    q = get_queue(args)
    waiting_ids = q.get_waiting_for(args.parent_id)
    if not waiting_ids:
        print(f"No tasks waiting for {args.parent_id}")
        return
    # Optionally fetch each task's details
    tasks = []
    for wid in waiting_ids:
        task = q.get_task(wid)
        if task:
            tasks.append(
                {
                    "task_id": task.task_id,
                    "task_type": task.task_type,
                    "status": task.status,
                    "created_at": task.created_at,
                }
            )
    output_result(tasks, args.format)


# ============ Dead-letter commands ============


def cmd_dead_list(args):
    """List dead-letter tasks."""
    redis_client = RedisController(args.redis_url).get_client()
    dead_key = f"{args.task_prefix}:dead_letter"
    # zrange to get all with scores (timestamps)
    members = redis_client.zrange(dead_key, 0, -1, withscores=True)
    if not members:
        print("No dead-letter tasks.")
        return
    tasks = []
    for member, score in members:
        task_id = member.decode() if isinstance(member, bytes) else member
        # Try to fetch the original task data (may have been cleaned up, but we can store copy in dead letter)
        # We'll just show the ID and timestamp.
        tasks.append(
            {
                "task_id": task_id,
                "failed_at": score,  # timestamp
            }
        )
    output_result(tasks, args.format)


def cmd_dead_requeue(args):
    """Requeue a dead-letter task (retry it)."""
    redis_client = RedisController(args.redis_url).get_client()
    dead_key = f"{args.task_prefix}:dead_letter"
    # Check if task is in dead set
    is_dead = redis_client.zscore(dead_key, args.task_id) is not None
    if not is_dead:
        print(f"Task {args.task_id} not found in dead-letter set", file=sys.stderr)
        sys.exit(1)
    # We need to re-push it to the queue. But we need the task data. It may have been deleted.
    # We can retrieve from result backend? Or we can store a copy.
    # For simplicity, we'll assume the task hash still exists (if not, we can't).
    q = get_queue(args)
    task = q.get_task(args.task_id)
    if not task:
        print(
            f"Task {args.task_id} metadata not found; cannot requeue. (Maybe it was already cleaned up.)",
            file=sys.stderr,
        )
        sys.exit(1)
    # Reset retries to 0 and push again
    task.retries = 0
    task.status = "pending"
    task.error = None
    # Remove from dead-letter set
    redis_client.zrem(dead_key, args.task_id)
    # Push back to queue
    q.push(task)
    print(f"Requeued task {args.task_id}")


# ============ Chain commands ============


def cmd_chain_status(args):
    """Get chain status."""
    tc = get_task_chain(args)
    status = tc.get_chain_status(args.chain_id)
    output_result(status, args.format)


def cmd_chain_tasks(args):
    """List tasks in a chain."""
    redis_client = RedisController(args.redis_url).get_client()
    tasks_json = redis_client.get(f"{args.task_prefix}:{args.chain_id}:tasks")
    if not tasks_json:
        print(f"No tasks found for chain {args.chain_id}", file=sys.stderr)
        sys.exit(1)
    tasks = json.loads(tasks_json)
    output_result(tasks, args.format)


# ============ Health check ============


def cmd_health(args):
    """Check if Redis is reachable and basic queue operations work."""
    try:
        redis_client = RedisController(args.redis_url).get_client()
        redis_client.ping()
        # Also try to get queue stats
        q = get_queue(args)
        q.stats()
        print("OK")
        sys.exit(0)
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)


# ============ Main parser ============


def create_parser():
    parser = argparse.ArgumentParser(
        description="Broccoli Task Queue CLI",
        epilog="See subcommand help for details: broccoli <command> --help",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="count",
        default=0,
        help="Increase logging verbosity (-v for INFO, -vv for DEBUG)",
    )

    subparsers = parser.add_subparsers(
        dest="command", required=True, help="Subcommand to run"
    )

    # ---------- worker start ----------
    worker_parser = subparsers.add_parser("worker", help="Manage workers")
    worker_subparsers = worker_parser.add_subparsers(
        dest="worker_action", required=True
    )
    start_parser = worker_subparsers.add_parser("start", help="Start a worker or pool")
    start_parser.add_argument(
        "--type",
        choices=["base", "threaded", "async", "hybrid", "chain"],
        default="threaded",
        help="Worker type (default: threaded)",
    )
    start_parser.add_argument(
        "--pool", action="store_true", help="Run multiple workers in a pool"
    )
    start_parser.add_argument(
        "--num-workers",
        type=int,
        default=4,
        help="Number of workers in pool (default: 4)",
    )
    start_parser.add_argument(
        "--redis-url",
        default=DEFAULT_REDIS_URL,
        help=f"Redis URL (env: BROCCOLI_REDIS_URL, default: {DEFAULT_REDIS_URL})",
    )
    start_parser.add_argument(
        "--queue-name",
        default=DEFAULT_QUEUE_NAME,
        help=f"Queue name for regular tasks (env: BROCCOLI_QUEUE_NAME, default: {DEFAULT_QUEUE_NAME})",
    )
    start_parser.add_argument(
        "--chain-queue-name",
        default=DEFAULT_CHAIN_QUEUE_NAME,
        help=f"Queue name for chain tasks (env: BROCCOLI_CHAIN_QUEUE_NAME, default: {DEFAULT_CHAIN_QUEUE_NAME})",
    )
    start_parser.add_argument(
        "--task-prefix",
        default=DEFAULT_TASK_PREFIX,
        help=f"Task hash prefix (env: BROCCOLI_TASK_PREFIX, default: {DEFAULT_TASK_PREFIX})",
    )
    start_parser.add_argument(
        "--worker-id", help="Unique worker ID (auto-generated if not set)"
    )
    start_parser.add_argument(
        "--concurrency",
        type=int,
        default=4,
        help="Concurrency level (threads for threaded/hybrid, async tasks for async)",
    )
    start_parser.add_argument(
        "--thread-workers",
        type=int,
        default=4,
        help="Thread pool size for hybrid worker (default: 4)",
    )
    start_parser.add_argument(
        "--async-tasks",
        type=int,
        default=10,
        help="Async task concurrency for hybrid worker (default: 10)",
    )
    start_parser.add_argument(
        "--recover-stalled",
        type=int,
        default=0,
        help="Recover stalled tasks older than N seconds before starting (0 = off)",
    )
    start_parser.set_defaults(func=cmd_worker_start)

    # ---------- queue ----------
    queue_parser = subparsers.add_parser("queue", help="Inspect the task queue")
    queue_subparsers = queue_parser.add_subparsers(dest="queue_action", required=True)

    # queue stats
    stats_parser = queue_subparsers.add_parser("stats", help="Show queue statistics")
    stats_parser.add_argument("--redis-url", default=DEFAULT_REDIS_URL)
    stats_parser.add_argument("--queue-name", default=DEFAULT_QUEUE_NAME)
    stats_parser.add_argument("--task-prefix", default=DEFAULT_TASK_PREFIX)
    stats_parser.add_argument(
        "--format", choices=["table", "json"], default="table", help="Output format"
    )
    stats_parser.set_defaults(func=cmd_queue_stats)

    # queue list
    list_parser = queue_subparsers.add_parser("list", help="List tasks by status")
    list_parser.add_argument(
        "--status",
        default="pending",
        choices=["pending", "in_progress", "completed", "failed", "waiting", "all"],
        help="Filter by status",
    )
    list_parser.add_argument(
        "--limit", type=int, help="Maximum number of tasks to show"
    )
    list_parser.add_argument("--redis-url", default=DEFAULT_REDIS_URL)
    list_parser.add_argument("--queue-name", default=DEFAULT_QUEUE_NAME)
    list_parser.add_argument("--task-prefix", default=DEFAULT_TASK_PREFIX)
    list_parser.add_argument("--format", choices=["table", "json"], default="table")
    list_parser.set_defaults(func=cmd_queue_list)

    # queue get
    get_parser = queue_subparsers.add_parser("get", help="Get task details by ID")
    get_parser.add_argument("task_id", help="Task ID")
    get_parser.add_argument("--redis-url", default=DEFAULT_REDIS_URL)
    get_parser.add_argument("--queue-name", default=DEFAULT_QUEUE_NAME)
    get_parser.add_argument("--task-prefix", default=DEFAULT_TASK_PREFIX)
    get_parser.add_argument("--format", choices=["table", "json"], default="table")
    get_parser.set_defaults(func=cmd_queue_get)

    # queue waiting
    waiting_parser = queue_subparsers.add_parser(
        "waiting", help="Show tasks waiting for a parent task"
    )
    waiting_parser.add_argument("parent_id", help="Parent task ID")
    waiting_parser.add_argument("--redis-url", default=DEFAULT_REDIS_URL)
    waiting_parser.add_argument("--queue-name", default=DEFAULT_QUEUE_NAME)
    waiting_parser.add_argument("--task-prefix", default=DEFAULT_TASK_PREFIX)
    waiting_parser.add_argument("--format", choices=["table", "json"], default="table")
    waiting_parser.set_defaults(func=cmd_queue_waiting)

    # ---------- dead ----------
    dead_parser = subparsers.add_parser("dead", help="Manage dead-letter tasks")
    dead_subparsers = dead_parser.add_subparsers(dest="dead_action", required=True)

    dead_list_parser = dead_subparsers.add_parser("list", help="List dead-letter tasks")
    dead_list_parser.add_argument("--redis-url", default=DEFAULT_REDIS_URL)
    dead_list_parser.add_argument("--task-prefix", default=DEFAULT_TASK_PREFIX)
    dead_list_parser.add_argument(
        "--format", choices=["table", "json"], default="table"
    )
    dead_list_parser.set_defaults(func=cmd_dead_list)

    dead_requeue_parser = dead_subparsers.add_parser(
        "requeue", help="Requeue a dead-letter task (retry)"
    )
    dead_requeue_parser.add_argument("task_id", help="Task ID to requeue")
    dead_requeue_parser.add_argument("--redis-url", default=DEFAULT_REDIS_URL)
    dead_requeue_parser.add_argument("--queue-name", default=DEFAULT_QUEUE_NAME)
    dead_requeue_parser.add_argument("--task-prefix", default=DEFAULT_TASK_PREFIX)
    dead_requeue_parser.set_defaults(func=cmd_dead_requeue)

    # ---------- chain ----------
    chain_parser = subparsers.add_parser("chain", help="Inspect task chains")
    chain_subparsers = chain_parser.add_subparsers(dest="chain_action", required=True)

    chain_status_parser = chain_subparsers.add_parser("status", help="Get chain status")
    chain_status_parser.add_argument("chain_id", help="Chain ID")
    chain_status_parser.add_argument("--redis-url", default=DEFAULT_REDIS_URL)
    chain_status_parser.add_argument("--task-prefix", default=DEFAULT_TASK_PREFIX)
    chain_status_parser.add_argument(
        "--format", choices=["table", "json"], default="table"
    )
    chain_status_parser.set_defaults(func=cmd_chain_status)

    chain_tasks_parser = chain_subparsers.add_parser(
        "tasks", help="List tasks in a chain"
    )
    chain_tasks_parser.add_argument("chain_id", help="Chain ID")
    chain_tasks_parser.add_argument("--redis-url", default=DEFAULT_REDIS_URL)
    chain_tasks_parser.add_argument("--task-prefix", default=DEFAULT_TASK_PREFIX)
    chain_tasks_parser.add_argument(
        "--format", choices=["table", "json"], default="table"
    )
    chain_tasks_parser.set_defaults(func=cmd_chain_tasks)

    # ---------- health ----------
    health_parser = subparsers.add_parser(
        "health", help="Check system health (Redis connectivity)"
    )
    health_parser.add_argument("--redis-url", default=DEFAULT_REDIS_URL)
    health_parser.add_argument("--queue-name", default=DEFAULT_QUEUE_NAME)
    health_parser.add_argument("--task-prefix", default=DEFAULT_TASK_PREFIX)
    health_parser.set_defaults(func=cmd_health)

    return parser


def main():
    parser = create_parser()
    args = parser.parse_args()

    # Set logging based on global verbosity
    setup_logging(args.verbose)

    # Call the subcommand function
    if hasattr(args, "func"):
        try:
            args.func(args)
        except KeyboardInterrupt:
            print("\nInterrupted", file=sys.stderr)
            sys.exit(1)
        except Exception as e:
            logging.error(f"Error: {e}", exc_info=True)
            sys.exit(1)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
