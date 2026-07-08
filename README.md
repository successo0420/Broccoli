# Broccoli

Broccoli is a Redis-backed Python task queue with priorities, dependency-aware scheduling, retry handling, dead-letter recovery, and multiple worker models.

## What changed for this release

- **Atomic dependency registration** for `push()` using Redis Lua (no registration race window).
- **Worker startup recovery** via `recover_on_startup=True` (default).
- **Processing health stats** now include `oldest_processing_timestamp`.
- **Dead-letter inspection + requeue** now keeps a dead-letter copy and supports `requeue_dead(task_id)`.
- **Task retry default** remains enabled (`max_retries=3`).

---

## Installation

```bash
pip install broccoli-workers
```

Requirements:
- Python 3.11+
- Redis 6+

---

## Quick start

```python
from broccoli.core.task.task import Task
from broccoli.core.task.task_queue import TaskQueue
from broccoli.workers.threaded_worker import ThreadedWorker

queue = TaskQueue(redis_url="redis://localhost:6379")
worker = ThreadedWorker(redis_url="redis://localhost:6379", max_workers=4)

worker.registry.register("send_email", lambda payload: print(payload["to"]))
queue.push(Task(task_type="send_email", payload={"to": "user@example.com"}), priority=1)

worker.start()
```

---

## Shared handlers pattern (recommended)

If you run multiple worker entrypoints (threaded, async, hybrid, chain), keep handlers in one file and import them everywhere so all workers see the same registrations.

### `handlers.py`

```python
from broccoli.core.task.task_registry import TaskRegistry


def register_handlers(registry: TaskRegistry) -> None:
    registry.register("send_email", send_email)
    registry.register("transcode", transcode)
    registry.register("generate_report", generate_report)


def send_email(payload):
    ...


def transcode(payload):
    ...


def generate_report(payload):
    ...
```

### `worker_threaded.py`

```python
from broccoli.workers.threaded_worker import ThreadedWorker
from handlers import register_handlers

worker = ThreadedWorker(redis_url="redis://localhost:6379", max_workers=8)
register_handlers(worker.registry)
worker.start()
```

### `worker_async.py`

```python
from broccoli.workers.async_worker import AsyncWorker
from handlers import register_handlers

worker = AsyncWorker(redis_url="redis://localhost:6379", max_concurrent=50)
register_handlers(worker.registry)
worker.start()
```

This keeps task-type definitions consistent across worker types and deploy units.

---

## Task model

```python
from broccoli.core.task.task import Task

task = Task(
    task_type="my_task",
    payload={"k": "v"},
    max_retries=3,           # default
    depends_on="parent-id", # optional single dependency
)
```

---

## Dependencies

`TaskQueue.push()` now atomically handles dependency registration:

- parent already completed -> task is enqueued immediately
- parent not completed -> task is marked `waiting` and linked under `dependency:<parent_id>`

Useful helpers:

- `queue.get_waiting_for(parent_id)`
- `queue.get_waiting_tasks()`

---

## Worker startup recovery

All workers now support startup recovery by default:

- `recover_on_startup=True`
- `recover_stalled_timeout=3600`

Example:

```python
from broccoli.workers.hybrid_worker import HybridWorker

worker = HybridWorker(
    redis_url="redis://localhost:6379",
    recover_on_startup=True,
    recover_stalled_timeout=1800,
)
worker.start()
```

CLI flags:

```bash
broccoli worker start --type threaded --recover-on-startup --recover-stalled-timeout 3600
broccoli worker start --type threaded --no-recover-on-startup
```

---

## Queue health and monitoring

Use queue stats for monitoring and alerts:

```python
stats = queue.processing_stats()
print(stats)
# {
#   "runnable": 12,
#   "processing": 3,
#   "dead_letter": 1,
#   "oldest_processing_timestamp": 1720000000.123
# }
```

CLI:

```bash
broccoli queue stats --format json
```

---

## Dead-letter inspection and requeue

On permanent failure, Broccoli stores:

- dead-letter index: `<task_prefix>:dead_letter` (sorted set)
- dead-letter copy: `dl:<task_id>` (hash with error/task metadata)

Requeue API:

```python
queue.requeue_dead(task_id)
```

CLI:

```bash
broccoli dead list
broccoli dead requeue <task_id>
```

---

## Worker types

- `BaseWorker`: single-threaded loop
- `ThreadedWorker`: concurrent thread pool
- `AsyncWorker`: asyncio concurrency
- `HybridWorker`: asyncio dispatch + thread execution
- `ChainWorker`: chain-focused execution

---

## CLI overview

```bash
broccoli worker start --help
broccoli queue stats
broccoli queue list --status waiting
broccoli queue waiting <parent_id>
broccoli dead list
broccoli dead requeue <task_id>
broccoli health
```

---

## Notes

- Multi-dependency fan-in (`depends_on` list), delayed scheduling (`run_at` / `run_after`), and streams migration compatibility are tracked separately as roadmap extensions.
- Existing queue APIs remain backward-compatible for single dependency workflows.
