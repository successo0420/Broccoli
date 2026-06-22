Here's the updated documentation with all references changed from "video_scheduler" to "broccoli":

```markdown
# Broccoli
A lightweight, Redis-backed distributed task queue system inspired by Celery. Built for simplicity and flexibility.

## Features

- **Simple API**: Register tasks with decorators, push to queue, process with workers
- **Multiple Worker Types**: Base, Threaded, Async, and Hybrid workers
- **Worker Pools**: Run multiple workers for parallel processing
- **Task Chaining**: Chain tasks together with result passing
- **Redis Backend**: Fast, reliable, and widely supported
- **Retry Logic**: Automatic retries with configurable max attempts
- **Task Status Tracking**: Monitor progress from pending → in_progress → completed/failed
- **Priority Support**: Control task execution order
- **Result Storage**: Store and retrieve task results
- **Graceful Shutdown**: Handle SIGTERM/SIGINT gracefully
- **Extensible**: Customize workers with pre/post processing hooks

## Installation

```bash
pip install broccoli
```

### Requirements

- Python 3.8+
- Redis 5.0+

## Quick Start

### 1. Define Your Tasks

```python
# tasks.py
from broccoli.core.task_registry import TaskRegistry

registry = TaskRegistry()

@registry.register("add_numbers")
def add_numbers(payload):
    """Add two numbers together."""
    result = payload["a"] + payload["b"]
    return {"sum": result, "operation": "addition"}

@registry.register("send_email")
def send_email(payload):
    """Send an email."""
    print(f"Sending to {payload['to']}: {payload['subject']}")
    return {"status": "sent", "message_id": "12345"}

@registry.register("process_video")
def process_video(payload):
    """Process a video file."""
    input_path = payload["input_path"]
    output_path = payload["output_path"]
    
    # Your video processing logic
    return {
        "output_path": output_path,
        "size": 1024,
        "duration": 120
    }
```

### 2. Start a Worker

```bash
# Single threaded worker with 4 threads
python -m broccoli.cli --worker-type threaded --concurrency 4

# Single async worker with 10 concurrent tasks
python -m broccoli.cli --worker-type async --concurrency 10

# Single hybrid worker
python -m broccoli.cli --worker-type hybrid --thread-workers 2 --async-tasks 5

# Worker pool with 3 threaded workers
python -m broccoli.cli --pool --worker-type threaded --num-workers 3 --concurrency 2

# Custom Redis
python -m broccoli.cli --redis-url redis://prod:6379 --worker-type async --concurrency 20
```

### 3. Push Tasks to Queue

```python
# producer.py
from broccoli.core.task import Task
from broccoli.core.task_queue import TaskQueue

queue = TaskQueue(redis_url="redis://localhost:6379")

# Create tasks
task1 = Task(
    task_type="add_numbers",
    payload={"a": 10, "b": 20},
    max_retries=3
)

task2 = Task(
    task_type="send_email",
    payload={
        "to": "user@example.com",
        "subject": "Hello from Broccoli",
        "body": "This is a test email"
    },
    priority=1
)

# Push to queue
task_id1 = queue.push(task1)
task_id2 = queue.push(task2, priority=1)

print(f"Tasks pushed: {task_id1}, {task_id2}")
```

### 4. Check Task Status

```python
# status.py
from broccoli.core.task_queue import TaskQueue

queue = TaskQueue()
task = queue.get_task("your-task-id-here")

if task:
    print(f"Status: {task.status}")
    print(f"Progress: {task.progress}%")
    print(f"Retries: {task.retries}/{task.max_retries}")
    if task.status == "completed":
        print(f"Result: {task.result}")
    elif task.status == "failed":
        print(f"Error: {task.error}")
```

## Worker Types

### BaseWorker (Sequential)
Single-threaded, processes one task at a time. Good for debugging.

```python
from broccoli.workers.base_worker import BaseWorker

worker = BaseWorker(redis_url="redis://localhost:6379")
worker.start()
```

### ThreadedWorker (Multi-threaded)
Processes multiple tasks concurrently using threads. Good for CPU-bound work.

```python
from broccoli.workers.threaded_worker import ThreadedWorker

worker = ThreadedWorker(
    redis_url="redis://localhost:6379",
    max_workers=4
)
worker.start()
```

### AsyncWorker (Asyncio)
Uses asyncio for concurrent I/O operations. Good for network calls, API requests.

```python
from broccoli.workers.async_worker import AsyncWorker

worker = AsyncWorker(
    redis_url="redis://localhost:6379",
    max_concurrent=10
)
worker.start()
```

### HybridWorker (Threads + Async)
Combines threading and asyncio for maximum throughput.

```python
from broccoli.workers.hybrid_worker import HybridWorker

worker = HybridWorker(
    redis_url="redis://localhost:6379",
    thread_workers=4,
    async_tasks=10
)
worker.start()
```

## Custom Worker with Hooks

```python
# worker.py
from broccoli.workers.base_worker import BaseWorker
import os
import shutil

class MyWorker(BaseWorker):
    def pre_process(self, task):
        """Setup before task execution."""
        print(f"Starting task: {task.task_id}")
        
        # Create temp directory
        temp_dir = f"/tmp/worker_{task.task_id}"
        os.makedirs(temp_dir, exist_ok=True)
        task.payload["temp_dir"] = temp_dir
        
        return True
    
    def post_process(self, task, success):
        """Cleanup after task execution."""
        temp_dir = task.payload.get("temp_dir")
        if temp_dir and os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        
        status = "✅" if success else "❌"
        print(f"{status} Task {task.task_id} completed")

# Use custom worker
worker = MyWorker(redis_url="redis://localhost:6379")
worker.start()
```

## Task Chaining

Chain multiple tasks where each task passes its result to the next.

```python
from broccoli.core.task_chain import TaskChain
from broccoli.core.task_registry import TaskRegistry

registry = TaskRegistry()

@registry.register("download")
def download(payload):
    # Download file
    return {"file_path": "/tmp/video.mp4"}

@registry.register("process")
def process(payload):
    # Previous result available in payload
    file_path = payload.get("__previous_result")["file_path"]
    # Process video
    return {"processed": True}

@registry.register("upload")
def upload(payload):
    file_path = payload.get("__previous_result")["file_path"]
    # Upload to cloud
    return {"url": "https://cdn.example.com/video.mp4"}

# Create chain
chain = TaskChain()
chain_id = chain.chain([
    {"task_type": "download", "payload": {"url": "https://example.com/video.mp4"}},
    {"task_type": "process", "payload": {}},
    {"task_type": "upload", "payload": {"bucket": "my-bucket"}}
])

# Check chain status
status = chain.get_chain_status(chain_id)
print(status)
```

### Chain with Worker

```python
from broccoli.core.task_chain import ChainWorkerMixin
from broccoli.workers.hybrid_worker import HybridWorker

class ChainWorker(ChainWorkerMixin, HybridWorker):
    pass

worker = ChainWorker()
worker.start()  # Processes chains automatically
```

## Worker Pool

Run multiple workers concurrently.

```python
from broccoli.core.worker_pool import WorkerPool
from broccoli.workers.threaded_worker import ThreadedWorker

pool = WorkerPool(
    worker_type=ThreadedWorker,
    num_workers=4,
    redis_url="redis://localhost:6379"
)

pool.start()  # Blocks until interrupted
```

## Result Backend

Store and retrieve task results.

```python
from broccoli.core.result import ResultBackend

backend = ResultBackend(redis_url="redis://localhost:6379")

# Store result with TTL (24 hours default)
backend.store("task-123", {"status": "completed"}, ttl=86400)

# Retrieve result
result = backend.get("task-123")
```

## CLI Reference

```bash
# Show help
python -m broccoli.cli --help

# Worker types
--worker-type {base,threaded,async,hybrid}  # Default: threaded

# Common options
--redis-url REDIS_URL                       # Default: redis://localhost:6379
--worker-id WORKER_ID                       # Custom worker identifier

# Pool options
--pool                                      # Run worker pool
--num-workers N                             # Number of workers (default: 4)

# Worker-specific options
--concurrency N                             # Threads for threaded/hybrid or async tasks
--async-tasks N                             # Async tasks for hybrid (default: 10)
--thread-workers N                          # Thread pool for hybrid (default: 4)

# Examples
python -m broccoli.cli --worker-type threaded --concurrency 4
python -m broccoli.cli --worker-type async --concurrency 10
python -m broccoli.cli --pool --worker-type base --num-workers 3
python -m broccoli.cli --pool --worker-type threaded --num-workers 5 --concurrency 2
```

## Architecture

```
┌──────────────┐     ┌─────────────┐     ┌──────────────┐
│  Producer    │────▶│    Redis     │────▶│   Worker     │
│  (pushes     │     │  (queue +    │     │  (processes  │
│   tasks)     │     │   results)   │     │   tasks)     │
└──────────────┘     └─────────────┘     └──────────────┘
                                                     │
                                                     ▼
                                           ┌──────────────┐
                                           │   Task       │
                                           │   Handlers   │
                                           │  (your code) │
                                           └──────────────┘
```

## Task Lifecycle

```
1. pending      → Task created and pushed to queue
2. in_progress  → Worker pops task and starts processing
3. completed    → Processing successful
4. failed       → Processing failed or max retries exceeded
5. retry        → Failed but retries remaining, requeued
```

## Error Handling

### Automatic Retries

```python
task = Task(
    task_type="unreliable_operation",
    payload={...},
    max_retries=5  # Try 5 times
)
```

### Manual Error Handling

```python
@registry.register("safe_operation")
def safe_operation(payload):
    try:
        result = process_data(payload["data"])
        return result
    except ConnectionError as e:
        raise  # Worker handles retry
    except ValueError as e:
        raise ValueError(f"Invalid data: {e}")  # Permanent failure
```

## Best Practices

### Task Design
**DO:** Keep tasks small and focused
```python
@registry.register("process_order")
def process_order(payload):
    return create_order(payload)
```

**DON'T:** Do everything in one task
```python
@registry.register("process_order")
def process_order(payload):
    validate_order(payload)
    process_payment(payload)
    send_confirmation(payload)
    update_inventory(payload)
    return {"status": "done"}
```

### Idempotency
Make tasks safe to run multiple times:
```python
@registry.register("create_record")
def create_record(payload):
    record_id = payload["record_id"]
    existing = db.find(record_id)
    if existing:
        return {"status": "already_exists", "record": existing}
    record = db.create(record_id, payload["data"])
    return {"status": "created", "record": record}
```

## Production Deployment

### Docker

```dockerfile
FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt
COPY . .
CMD ["python", "-m", "broccoli.cli", "--worker-type", "threaded", "--concurrency", "4"]
```

```yaml
# docker-compose.yml
version: '3.8'
services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
  worker:
    build: .
    depends_on:
      - redis
    environment:
      - REDIS_URL=redis://redis:6379
    deploy:
      replicas: 3
```

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `redis_url` | redis://localhost:6379 | Redis connection URL |
| `max_retries` | 3 | Maximum task retry attempts |
| `task_timeout` | 3600 | Task timeout in seconds |
| `max_concurrent` | 10 | Async worker concurrency |
| `max_workers` | 4 | Threaded worker thread count |
| `thread_workers` | 4 | Hybrid worker thread pool |
| `async_tasks` | 10 | Hybrid worker async concurrency |

## License

MIT License


**Made with ❤️ for developers who need a simple, powerful task queue**
```