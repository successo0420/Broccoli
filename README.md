# Broccoli

Broccoli is a Redis-backed Python task queue for running background work with:

- **Priority scheduling** (strict priority tiers + FIFO ordering inside each tier)
- **Dependency-aware tasks** (`depends_on`)
- **Retries + dead-letter handling**
- **Crash/stall recovery**
- **Multiple worker runtimes** (base, threaded, async, hybrid, chain)
- **CLI tooling** for operational inspection and control

It is designed for teams that want Celery-like queue behavior with a smaller, explicit codebase.

---

## Table of contents

- [Features](#features)
- [Architecture and Redis data model](#architecture-and-redis-data-model)
- [Requirements](#requirements)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Core concepts](#core-concepts)
- [Task lifecycle](#task-lifecycle)
- [Worker types](#worker-types)
- [Task dependencies](#task-dependencies)
- [Dead-letter and recovery](#dead-letter-and-recovery)
- [CLI reference](#cli-reference)
- [Environment variables](#environment-variables)
- [Programmatic API reference](#programmatic-api-reference)
- [Operational guidance](#operational-guidance)
- [Troubleshooting](#troubleshooting)
- [Development](#development)
- [License](#license)

---

## Features

### Queueing and scheduling

- Push tasks with explicit priorities (`0` is highest priority)
- FIFO ordering is preserved within each priority band
- Queue operations are persisted in Redis for durability

### Dependency-aware execution

- A task can declare `depends_on=<task_id>`
- Dependent tasks are marked `waiting` until their parent completes
- Dependency release is handled automatically on parent completion

### Worker execution models

- Single-threaded worker (`BaseWorker`)
- Thread pool worker (`ThreadedWorker`)
- Asyncio worker (`AsyncWorker`)
- Hybrid worker (`HybridWorker`: async dispatch + threaded execution)
- Chain-specific worker (`ChainWorker`)

### Reliability and failure handling

- Built-in retry handling (`max_retries`, default `3`)
- Dead-letter capture for permanently failed tasks
- Requeue dead-letter tasks for replay
- Stalled task recovery (manual and startup auto-recovery)

### Operations and observability

- Queue depth and processing stats
- Dead-letter inspection CLI commands
- Health checks for Redis reachability

---

## Architecture and Redis data model

Broccoli primarily uses Redis sorted sets and hashes:

- `<base>:queue` — runnable tasks, scored by `(priority tier + FIFO sequence)`
- `<base>:processing` — in-flight tasks, scored by processing timestamp
- `<base>:sequence` — monotonic sequence for FIFO ordering
- `<task_prefix>:<task_id>` — task metadata hash
- `dependency:<task_id>` — set of blocked dependents waiting for parent
- `<task_prefix>:dead_letter` — dead-letter task IDs with failure timestamps
- `dl:<task_id>` — dead-letter task snapshot
- `result:<task_id>` / `result:<chain_id>` — result records with TTL

For chains, additional keys like `chain:<chain_id>` and `chain:<chain_id>:tasks` are used.

---

## Requirements

- Python **3.11+**
- Redis **6+**

---

## Installation

```bash
pip install broccoli-workers
```

Local development install:

```bash
pip install -e .
```

---

## Quick start

### 1) Create a producer

```python
from broccoli.core.task.task import Task
from broccoli.core.task.task_queue import TaskQueue

queue = TaskQueue(redis_url="redis://localhost:6379")

task = Task(task_type="send_email", payload={"to": "user@example.com"})
queue.push(task, priority=1)
print(task.task_id)
```

### 2) Create a worker and register handlers

```python
from broccoli.workers.threaded_worker import ThreadedWorker

worker = ThreadedWorker(redis_url="redis://localhost:6379", max_workers=4)

@worker.registry.register("send_email")
def send_email(payload):
    print(f"Sending email to {payload['to']}")
    return {"status": "sent", "to": payload["to"]}

worker.start()
```

---

## Core concepts

### Task model

`Task` fields include:

- `task_id`: UUID (auto-generated unless provided)
- `task_type`: handler key in registry
- `payload`: arbitrary JSON-serializable dictionary
- `status`: `pending | waiting | in_progress | completed | failed`
- `retries`: current retry count
- `max_retries`: retry limit
- `depends_on`: optional parent task ID
- `result`: handler output
- `error`: error message for failed attempts

### Registry model

`TaskRegistry` is singleton-backed, so handlers are globally visible in-process.

You can register with a decorator:

```python
@worker.registry.register("my_task")
def my_task(payload):
    return {"ok": True}
```

Or manually:

```python
worker.registry.register_manually("my_task", my_task_handler)
```

---

## Task lifecycle

1. **Push**: task metadata hash is saved.
2. **Queue placement**:
   - no dependency -> `pending` in runnable queue
   - with unresolved dependency -> `waiting`
3. **Pop**: worker moves task to `in_progress` and processing set.
4. **Execution**:
   - success -> `completed`
   - failure + retries left -> `pending` (requeued)
   - failure + retries exhausted -> `failed` (dead-letter eligible)
5. **Post-processing**:
   - results persisted
   - task hash cleanup for terminal states
   - handlers invoked

---

## Worker types

### BaseWorker

- Sequential task execution
- Simple and predictable behavior
- Useful for debugging and low-throughput workloads

### ThreadedWorker

- Executes tasks concurrently using `ThreadPoolExecutor`
- Good for mixed and blocking workloads
- Config: `max_workers`

### AsyncWorker

- Uses `asyncio` loop with bounded concurrency
- Runs task handlers via executor with timeout enforcement
- Config: `max_concurrent`

### HybridWorker

- Async dispatch + thread pool execution
- Suitable for high-throughput mixed workloads
- Config: `thread_workers`, `async_tasks`

### ChainWorker

- Specialized for chain orchestration
- Updates chain progress and completion state
- Works with `TaskChain`

---

## Task dependencies

When pushing dependent tasks:

- If parent already completed, dependent is enqueued immediately.
- If parent not complete, dependent enters `waiting` and is linked under `dependency:<parent_id>`.

On parent completion, waiting tasks are released and enqueued using their original priority.

Helpful APIs:

- `queue.get_waiting_for(parent_id)`
- `queue.get_waiting_tasks()`

---

## Dead-letter and recovery

### Dead-letter behavior

On permanent failure, task IDs are stored in `<task_prefix>:dead_letter` and a snapshot is persisted at `dl:<task_id>`.

### Requeue dead-letter task

```python
queue.requeue_dead(task_id)
```

CLI:

```bash
broccoli dead list
broccoli dead requeue <task_id>
```

### Recover stalled processing tasks

Manual recovery before startup:

```bash
broccoli worker start --type threaded --recover-stalled 600
```

Automatic startup recovery (default enabled):

```bash
broccoli worker start --type threaded --recover-on-startup
broccoli worker start --type threaded --no-recover-on-startup
```

---

## CLI reference

### Global

```bash
broccoli -v ...
broccoli -vv ...
```

### Worker startup

```bash
broccoli worker start --type threaded
broccoli worker start --type async --concurrency 20
broccoli worker start --type hybrid --thread-workers 8 --async-tasks 50
broccoli worker start --type chain --chain-queue-name chain_tasks:queue
broccoli worker start --type threaded --pool --num-workers 4
```

Common worker flags:

- `--redis-url`
- `--queue-name`
- `--chain-queue-name`
- `--task-prefix`
- `--worker-id`
- `--recover-stalled`
- `--recover-stalled-timeout`
- `--recover-on-startup` / `--no-recover-on-startup`

### Queue inspection

```bash
broccoli queue stats --format table
broccoli queue stats --format json
broccoli queue list --status pending --limit 20
broccoli queue get <task_id>
broccoli queue waiting <parent_id>
```

### Dead-letter operations

```bash
broccoli dead list
broccoli dead list --format json
broccoli dead requeue <task_id>
```

### Chain inspection

```bash
broccoli chain status <chain_id>
broccoli chain tasks <chain_id>
```

### Health

```bash
broccoli health
```

---

## Environment variables

Broccoli CLI defaults can come from environment variables:

- `BROCCOLI_REDIS_URL` (default: `redis://localhost:6379`)
- `BROCCOLI_QUEUE_NAME` (default: `tasks:queue`)
- `BROCCOLI_CHAIN_QUEUE_NAME` (default: `chain_tasks:queue`)
- `BROCCOLI_TASK_PREFIX` (default: `task`)

---

## Programmatic API reference

### TaskQueue

- `push(task, priority=0) -> task_id`
- `pop() -> Task | None`
- `complete(task)`
- `fail(task)`
- `requeue(task_id, priority=None)`
- `requeue_dead(task_id) -> bool`
- `recover_stalled(timeout_seconds=3600) -> int`
- `get_task(task_id) -> Task | None`
- `stats() -> dict`
- `processing_stats() -> dict`
- `get_waiting_for(task_id) -> list[str]`
- `get_waiting_tasks() -> list[str]`
- `is_fully_drained() -> bool`

### TaskChain

- `chain(tasks, shared_payload=None, completion_task=None, completion_payload=None) -> chain_id`
- `get_chain_status(chain_id) -> dict`

### Worker hooks

All workers support lifecycle hooks:

- `add_completion_handler(handler)`
- `add_failure_handler(handler)`
- `add_pre_process_handler(handler)`
- `add_post_process_handler(handler)`
- decorator aliases: `on_complete`, `on_failure`, `on_pre_process`, `on_post_process`

---

## Operational guidance

### 1. Use shared registration modules

Keep task registrations in one importable module and register into each worker instance at startup.

### 2. Prefer idempotent handlers

Retries can execute handlers multiple times. Side effects should be safe to repeat.

### 3. Set realistic timeout and recovery values

Choose `recover_stalled_timeout` high enough to avoid recovering long-running but valid tasks.

### 4. Monitor dead-letter growth

A growing dead-letter set often indicates handler bugs or dependency/data issues.

### 5. Separate queues by workload profile

Use distinct queue names/prefixes for high-latency jobs, chain jobs, or tenant isolation.

---

## Troubleshooting

### Worker starts but does nothing

- Ensure tasks are pushed to the same `queue_name` the worker consumes.
- Verify `task_prefix` alignment between producer and worker.
- Confirm handlers are registered for every `task_type`.

### `No handler registered for task type`

- Register the handler in the same process that runs the worker.
- Import your registration module before calling `worker.start()`.

### Tasks remain `waiting`

- Check parent task status with `broccoli queue get <parent_id>`.
- Confirm parent was completed (not failed/deleted).

### Stuck processing tasks after crash

- Enable startup recovery or run `--recover-stalled` manually.

### Redis connectivity errors

- Run `broccoli health`.
- Verify Redis URL/auth/network.

---

## Development

Install development dependencies:

```bash
pip install -e .[dev]
```

Run tests:

```bash
pytest
```

Format/lint/type-check tools are configured in `pyproject.toml` (`black`, `isort`, `flake8`, `mypy`).

---

## License

MIT License. See `/home/runner/work/Broccoli/Broccoli/LICENSE`.
