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