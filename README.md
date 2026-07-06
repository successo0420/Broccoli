# Broccoli

A lightweight Redis-backed task queue with priority scheduling, dependency chaining, and multiple worker execution models.

---

## Requirements

- Python 3.10+
- Redis 6+
- `redis-py`

---

## Quick start

```python
from broccoli.core.task.task import Task
from broccoli.core.task.task_queue import TaskQueue
from broccoli.workers.base_worker import BaseWorker

# Register a task handler
class MyWorker(BaseWorker):
    pass

worker = MyWorker(redis_url="redis://localhost:6379")
worker.registry.register("send_email", lambda payload: send(payload["to"]))

# Push a task
queue = TaskQueue(redis_url="redis://localhost:6379")
task = Task(task_type="send_email", payload={"to": "user@example.com"})
queue.push(task)

# Start processing (blocking)
worker.start()
```

---

## Worker types

Choose the worker that fits your workload.

### BaseWorker — simple, single-threaded

Processes one task at a time in a blocking loop. Good for low-volume workloads or when you want the simplest possible setup.

```python
from broccoli.workers.base_worker import BaseWorker

class VideoWorker(BaseWorker):
    pass

worker = VideoWorker(redis_url="redis://localhost:6379")
worker.registry.register("transcode", transcode_video)
worker.start()
```

### ThreadedWorker — concurrent, CPU-friendly

Runs a configurable thread pool. Good for I/O-bound tasks or when you want multiple tasks processing in parallel without asyncio.

```python
from broccoli.workers.threaded_worker import ThreadedWorker

worker = ThreadedWorker(redis_url="redis://localhost:6379", max_workers=8)
worker.registry.register("resize_image", resize)
worker.start()
```

### AsyncWorker — high-concurrency async

Dispatches tasks as asyncio coroutines. Good for large numbers of I/O-bound tasks (HTTP calls, DB queries) where thread overhead would be significant.

```python
from broccoli.workers.async_worker import AsyncWorker

worker = AsyncWorker(redis_url="redis://localhost:6379", max_concurrent=50)
worker.registry.register("fetch_url", fetch)
worker.start()
```

### HybridWorker — async dispatch + thread execution

Combines asyncio concurrency control with a ThreadPoolExecutor for the actual handler. The right choice when you want high throughput and your handlers are not async-native.

```python
from broccoli.workers.hybrid_worker import HybridWorker

worker = HybridWorker(
    redis_url="redis://localhost:6379",
    thread_workers=4,
    async_tasks=20,
    result_ttl=3600,   # seconds before result expires in Redis
)
worker.registry.register("process_batch", process)
worker.start()
```

### WorkerPool — run multiple workers

Spawns N workers of any type, each in its own daemon thread.

```python
from broccoli.workers.worker_pool import WorkerPool
from broccoli.workers.threaded_worker import ThreadedWorker

pool = WorkerPool(
    worker_type=ThreadedWorker,
    num_workers=4,
    redis_url="redis://localhost:6379",
)
pool.start()   # blocks until pool.stop() or SIGTERM
```

---

## Task options

```python
from broccoli.core.task.task import Task

task = Task(
    task_type="my_task",
    payload={"key": "value"},
    max_retries=3,          # default: 0 (no retry)
    depends_on="<task_id>", # optional: block until this task completes
)
```

### Priority

Lower number = higher priority. Default is `0`.

```python
queue.push(urgent_task, priority=0)   # processed first
queue.push(normal_task, priority=1)
queue.push(low_task,    priority=5)
```

Tasks with the same priority are processed in FIFO order.

### Dependencies

A task with `depends_on` will not run until its parent task completes.

```python
step1 = Task(task_type="extract",   payload={...})
step2 = Task(task_type="transform", payload={...}, depends_on=step1.task_id)
step3 = Task(task_type="load",      payload={...}, depends_on=step2.task_id)

queue.push(step1)
queue.push(step2)
queue.push(step3)
# step2 runs only after step1 completes; step3 only after step2
```

---

## Lifecycle hooks

Register functions to run at specific points in a task's lifecycle. All registration methods return `self` for chaining.

```python
worker = MyWorker(redis_url="redis://localhost:6379")

# Method-style registration
worker.add_completion_handler(lambda task, result: print(f"Done: {result}"))
worker.add_failure_handler(lambda task, err: alert(str(err)))
worker.add_pre_process_handler(lambda task: task.payload.get("enabled", True))
worker.add_post_process_handler(lambda task, success: metrics.record(success))

# Chained registration
worker \
    .add_completion_handler(notify_slack) \
    .add_failure_handler(notify_pagerduty)

# Decorator-style registration
@worker.on_complete
def on_done(task, result):
    print(f"Task {task.task_id} finished with: {result}")

@worker.on_failure
def on_fail(task, error):
    logger.error(f"Task {task.task_id} failed: {error}")
```

Pre-process handlers can abort a task by returning `False`:

```python
@worker.on_pre_process
def gate(task):
    if task.payload.get("dry_run"):
        return False  # skip this task
    return True
```

---

## Chain tasks

For multi-step pipelines, use `ChainWorker`. Each step in the chain carries a `__chain_id` in its payload; the final result is stored atomically when the last step completes.

```python
from broccoli.workers.chain_worker import ChainWorker

worker = ChainWorker(redis_url="redis://localhost:6379")
worker.registry.register("step_a", do_a)
worker.registry.register("step_b", do_b)
worker.start()
```

---

## Crash recovery

If a worker process dies mid-task, the task remains in the `tasks:processing` sorted set indefinitely. Call `recover_stalled` on startup (or on a schedule) to re-enqueue any tasks that have been in-flight longer than expected:

```python
queue = TaskQueue(redis_url="redis://localhost:6379")
recovered = queue.recover_stalled(timeout_seconds=3600)
print(f"Re-enqueued {recovered} stalled tasks")
```

---

## Logging

Broccoli uses the standard `logging` module under the `broccoli` namespace. Configure it in your application:

```python
import logging
logging.basicConfig(level=logging.INFO)
```

# Command-Line Interface (CLI)

Broccoli ships with a powerful CLI for starting workers, inspecting queues, managing failed tasks, and monitoring chains – all from the terminal.

---

## Installation

Make the CLI executable and available on your `PATH`:

```bash
chmod +x broccoli/cli.py
# Optionally symlink it:
ln -s $(pwd)/broccoli/cli.py /usr/local/bin/broccoli
```

Or run it directly via Python:

```bash
python -m broccoli.cli --help
```

---

## Global Options

| Flag | Description |
|------|-------------|
| `-v`, `--verbose` | Increase logging verbosity (`-v` for INFO, `-vv` for DEBUG). |
| `--help` | Show help for any command or subcommand. |

All subcommands also respect environment variables for common settings:

| Env Variable | Default | Used For |
|--------------|---------|----------|
| `BROCCOLI_REDIS_URL` | `redis://localhost:6379` | Redis connection string |
| `BROCCOLI_QUEUE_NAME` | `tasks:queue` | Regular task queue name |
| `BROCCOLI_CHAIN_QUEUE_NAME` | `chain_tasks:queue` | Chain task queue name |
| `BROCCOLI_TASK_PREFIX` | `task` | Redis key prefix for task hashes |

---

## Worker Management

### Start a single worker

```bash
# Threaded worker with 8 threads
broccoli worker start --type threaded --concurrency 8

# Async worker with 20 concurrent tasks
broccoli worker start --type async --concurrency 20

# Hybrid worker (threads for CPU + asyncio for I/O)
broccoli worker start --type hybrid --thread-workers 4 --async-tasks 10

# Chain worker (for task chains)
broccoli worker start --type chain
```

### Start a worker pool

```bash
# Pool of 4 threaded workers
broccoli worker start --type threaded --pool --num-workers 4

# Pool of 3 async workers
broccoli worker start --type async --pool --num-workers 3
```

### Recover stalled tasks on startup

```bash
# Re-enqueue any tasks that have been in-flight for > 3600 seconds
broccoli worker start --type threaded --recover-stalled 3600
```

### Advanced: custom queue names

```bash
broccoli worker start --type threaded \
    --queue-name myapp:queue \
    --chain-queue-name myapp:chain \
    --task-prefix myapp
```

---

## Queue Inspection

### View queue statistics

```bash
broccoli queue stats
```

Output:
```
runnable: 12
processing: 3
dead_letter: 2
```

With JSON output:
```bash
broccoli queue stats --format json
```

### List tasks by status

```bash
# Pending tasks (ready to run)
broccoli queue list --status pending --limit 10

# In-progress tasks
broccoli queue list --status in_progress

# All tasks (scan all task hashes – use with care on large queues)
broccoli queue list --status all --limit 20
```

### Get a specific task

```bash
broccoli queue get <task_id>
```

Returns full task metadata (payload, result, error, retries, etc.).

### Show tasks waiting for a parent

```bash
# See which tasks are blocked on a particular task
broccoli queue waiting <parent_task_id>
```

This is invaluable for debugging dependency deadlocks.

---

## Dead-Letter Management

When tasks exhaust all retries, they are moved to the dead‑letter set for manual inspection and retry.

### List dead-letter tasks

```bash
broccoli dead list
```

Shows task IDs and the timestamp when they failed.

### Requeue a dead task

```bash
broccoli dead requeue <task_id>
```

This resets the retry count to `0` and pushes the task back to the runnable queue.

---

## Chain Inspection

### Get chain status

```bash
broccoli chain status <chain_id>
```

Returns:
```json
{
  "chain_id": "abc-123",
  "total_tasks": 5,
  "completed_tasks": 3,
  "current_task": 3,
  "status": "in_progress",
  "failed": false
}
```

### List all tasks in a chain

```bash
broccoli chain tasks <chain_id>
```

Shows the full task configuration for every step in the chain, including payloads and task IDs.

---

## Health Check

Useful for monitoring systems (e.g., Kubernetes liveness probes).

```bash
broccoli health
```

- Exits with `0` if Redis is reachable and basic queue operations work.
- Exits with `1` and prints an error on failure.

Example with `curl`‑style usage:
```bash
if broccoli health; then
    echo "Broccoli is ready"
else
    echo "Broccoli is unhealthy" >&2
    exit 1
fi
```

---

## Output Formats

All inspection commands support two output formats:

- **`table`** (default) – human‑readable, aligned columns.
- **`json`** – machine‑readable, ideal for scripting and APIs.

```bash
# Human‑readable
broccoli queue stats

# Machine‑readable
broccoli queue stats --format json
```

---

## Complete Usage Examples

### Development workflow

Start a single threaded worker with verbose logging:
```bash
broccoli -v worker start --type threaded --concurrency 4
```

### Production deployment (systemd / supervisor)

```bash
# Start a pool of 8 async workers, recovering stalled tasks
broccoli worker start --type async --pool --num-workers 8 \
    --concurrency 10 --recover-stalled 3600
```

### Debugging a stuck workflow

1. Check if tasks are waiting:
   ```bash
   broccoli queue waiting <parent_id>
   ```

2. Inspect the task that isn't progressing:
   ```bash
   broccoli queue get <task_id>
   ```

3. If a task failed permanently, requeue it:
   ```bash
   broccoli dead list
   broccoli dead requeue <failed_task_id>
   ```

4. Monitor chain progress:
   ```bash
   broccoli chain status <chain_id>
   ```

### Automation / scripting

Export settings once:
```bash
export BROCCOLI_REDIS_URL="redis://prod-cluster:6379"
export BROCCOLI_QUEUE_NAME="video:queue"
```

Then run commands without repeating arguments:
```bash
broccoli queue stats --format json
broccoli dead list
```

---

## CLI Reference

| Command | Description |
|---------|-------------|
| `worker start` | Start a worker (or pool) with the specified type and concurrency. |
| `queue stats` | Show runnable, processing, and dead‑letter counts. |
| `queue list` | List tasks filtered by status (pending, in_progress, etc.). |
| `queue get <id>` | Fetch and display a task's full metadata. |
| `queue waiting <parent_id>` | Show tasks blocked on a given parent. |
| `dead list` | List all permanently failed tasks. |
| `dead requeue <id>` | Re‑enqueue a failed task (retry). |
| `chain status <id>` | Show completion progress of a task chain. |
| `chain tasks <id>` | List all steps in a chain. |
| `health` | Check Redis connectivity and queue health. |

For detailed options on any command, use `--help`:

```bash
broccoli worker start --help
broccoli queue list --help
```

---
