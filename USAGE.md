# Video Scheduler - Complete Usage Documentation

## Overview
A Celery-like distributed task system with multiple worker types, task chaining, priority queues, and retry logic.

---

## 1. Task Registry

### Register Task Handlers
```python
from video_scheduler.core.task_registry import TaskRegistry

registry = TaskRegistry()

@registry.register("process_video")
def process_video(payload):
    video_id = payload.get("video_id")
    # Processing logic
    return {"status": "processed", "video_id": video_id}

@registry.register("generate_thumbnail")
def generate_thumbnail(payload):
    image_path = payload.get("image_path")
    # Thumbnail generation
    return {"thumbnail": "path/to/thumbnail.jpg"}
```

---

## 2. Creating Tasks

### Basic Task
```python
from video_scheduler.core.task import Task

# Simple task
task = Task(
    task_type="process_video",
    payload={"video_id": "123", "quality": "720p"}
)

# Task with dependencies
task = Task(
    task_type="generate_thumbnail",
    payload={"image_path": "/tmp/video.jpg"},
    depends_on="previous-task-id",  # Wait for this task to complete
    max_retries=5
)
```

### Task Properties
- `task_id`: Auto-generated UUID
- `task_type`: Registered handler name
- `status`: pending/in_progress/completed/failed
- `progress`: 0-100 float
- `retries`: Current retry count
- `max_retries`: Maximum retry attempts (default 3)
- `payload`: Task data dictionary
- `depends_on`: Task ID dependency

---

## 3. Task Queue

### Basic Operations
```python
from video_scheduler.core.task_queue import TaskQueue

queue = TaskQueue(redis_url="redis://localhost:6379")

# Push task (priority 0=highest)
queue.push(task, priority=0)

# Push with lower priority
queue.push(task, priority=10)

# Pop task (blocks until available)
task = queue.pop()

# Get task by ID
task = queue.get_task("task-id-123")

# Requeue failed task
queue.requeue("task-id-123", priority=5)
```

---

## 4. Worker Types

### 4.1 BaseWorker (Sequential)

**Use when:** Simple processing, single-threaded, debugging.

```python
from video_scheduler.workers.base_worker import BaseWorker

class CustomWorker(BaseWorker):
    def pre_process(self, task):
        """Called before processing. Return False to skip."""
        print(f"About to process: {task.task_id}")
        return True
    
    def post_process(self, task, success):
        """Called after processing."""
        print(f"Task {task.task_id} completed: {success}")

worker = CustomWorker()
worker.start()  # Runs forever, processing one task at a time
```

---

### 4.2 ThreadedWorker (Multi-threaded)

**Use when:** CPU-bound tasks, I/O operations, need parallel processing.

```python
from video_scheduler.workers.threaded_worker import ThreadedWorker

# 4 concurrent threads
worker = ThreadedWorker(
    redis_url="redis://localhost:6379",
    worker_id="my-worker-1",
    max_workers=4
)

# Push 10 tasks
for i in range(10):
    task = Task("process_video", payload={"id": i})
    queue.push(task)

worker.start()  # Processes 4 tasks simultaneously
```

**Characteristics:**
- Uses ThreadPoolExecutor
- Good for CPU-bound work
- Each task runs in separate thread
- GIL limits true parallelism for Python code

---

### 4.3 AsyncWorker (Asyncio)

**Use when:** I/O-bound operations, network calls, API requests.

```python
from video_scheduler.workers.async_worker import AsyncWorker

worker = AsyncWorker(
    redis_url="redis://localhost:6379",
    max_concurrent=10  # Concurrent async tasks
)

worker.start()  # Uses asyncio event loop
```

**Characteristics:**
- Non-blocking I/O
- Lower overhead than threads
- Good for HTTP requests, database queries
- Must use async-compatible handlers

---

### 4.4 HybridWorker (Threads + Async)

**Use when:** Mixed workloads, maximum throughput.

```python
from video_scheduler.workers.hybrid_worker import HybridWorker

worker = HybridWorker(
    redis_url="redis://localhost:6379",
    thread_workers=4,  # Thread pool size
    async_tasks=10     # Concurrent async tasks
)

worker.start()
```

**Characteristics:**
- Thread pool for CPU-bound work
- Async semaphore for I/O-bound work
- Best for mixed workloads
- Highest throughput

---

## 5. Worker Pool

### Managing Multiple Workers
```python
from video_scheduler.core.worker_pool import WorkerPool
from video_scheduler.workers.threaded_worker import ThreadedWorker

pool = WorkerPool(
    worker_type=ThreadedWorker,  # Any worker class
    num_workers=4,
    redis_url="redis://localhost:6379"
)

# Start all workers
pool.start()

# Stop all workers
pool.stop()
```

---

## 6. Task Chaining

### Chain Multiple Tasks
```python
from video_scheduler.core.task_chain import TaskChain
from video_scheduler.core.task_registry import TaskRegistry

registry = TaskRegistry()

@registry.register("download")
def download(payload):
    url = payload.get("url")
    # Download file
    return {"file_path": "/tmp/video.mp4"}

@registry.register("process")
def process(payload):
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

### Chain with Worker Mixin
```python
from video_scheduler.core.task_chain import ChainWorkerMixin

class ChainWorker(ChainWorkerMixin, BaseWorker):
    def post_process(self, task, success):
        # Chain handling is automatic via mixin
        super().post_process(task, success)
        # Add custom logic
        if success:
            print(f"Task {task.task_id} succeeded")
```

---

## 7. Result Backend

### Storing and Retrieving Results
```python
from video_scheduler.core.result import ResultBackend

backend = ResultBackend(redis_url="redis://localhost:6379")

# Store result with TTL (24 hours default)
backend.store("task-123", {"status": "completed", "data": {...}}, ttl=86400)

# Retrieve result
result = backend.get("task-123")
```

---

## 8. Complete Example: Video Processing Pipeline

```python
# video_pipeline.py
import time
from video_scheduler.core.task import Task
from video_scheduler.core.task_queue import TaskQueue
from video_scheduler.core.task_registry import TaskRegistry
from video_scheduler.workers.hybrid_worker import HybridWorker
from video_scheduler.core.task_chain import TaskChain, ChainWorkerMixin

# Setup
registry = TaskRegistry()
queue = TaskQueue()
chain = TaskChain()

# Register handlers
@registry.register("download")
def download_video(payload):
    print(f"Downloading: {payload['url']}")
    time.sleep(2)
    return {"local_path": "/tmp/video.mp4", "size": 100}

@registry.register("transcode")
def transcode_video(payload):
    path = payload.get("__previous_result")["local_path"]
    print(f"Transcoding: {path}")
    time.sleep(3)
    return {"output_path": "/tmp/video_720p.mp4", "format": "h264"}

@registry.register("thumbnail")
def generate_thumbnail(payload):
    path = payload.get("__previous_result")["output_path"]
    print(f"Generating thumbnail from: {path}")
    time.sleep(1)
    return {"thumbnail": "/tmp/thumb.jpg"}

@registry.register("upload")
def upload_file(payload):
    path = payload.get("__previous_result")["output_path"]
    print(f"Uploading: {path}")
    time.sleep(2)
    return {"url": "https://cdn.com/video.mp4"}

# Custom worker with chain support
class PipelineWorker(ChainWorkerMixin, HybridWorker):
    pass

# Create pipeline
chain_id = chain.chain([
    {"task_type": "download", "payload": {"url": "https://example.com/video.mp4"}},
    {"task_type": "transcode", "payload": {}},
    {"task_type": "thumbnail", "payload": {}},
    {"task_type": "upload", "payload": {"bucket": "my-videos"}}
])

print(f"Pipeline started: {chain_id}")

# Process with hybrid worker
worker = PipelineWorker(
    thread_workers=2,
    async_tasks=4
)

# Start in background
import threading
thread = threading.Thread(target=worker.start)
thread.start()

# Monitor progress
while True:
    status = chain.get_chain_status(chain_id)
    print(f"Progress: {status['completed_tasks']}/{status['total_tasks']}")
    if status['status'] == 'completed':
        break
    time.sleep(2)

worker.stop()
```

---

## 9. Error Handling and Retries

```python
# Custom retry logic
class RetryWorker(BaseWorker):
    def process(self, task):
        try:
            # Attempt processing
            return super().process(task)
        except Exception as e:
            # Check if retryable
            if "network" in str(e).lower():
                task.max_retries = 5  # More retries for network issues
            raise

    def post_process(self, task, success):
        if not success and task.retries == task.max_retries:
            # Send to dead letter queue
            self.queue.redis.lpush("tasks:dlq", task.task_id)
            print(f"Task {task.task_id} moved to DLQ")
```

---

## 10. Monitoring and Maintenance

```python
# Get queue length
queue_length = queue.redis.zcard("tasks:queue")

# Get task status
task_data = queue.redis.hgetall("task:task-id-123")

# Get all tasks in queue
tasks = queue.redis.zrange("tasks:queue", 0, -1)

# Clear queue (dangerous!)
queue.redis.delete("tasks:queue")

# Get chain progress
chain_status = chain.get_chain_status("chain-id-123")

# Monitor worker health
def monitor_workers():
    workers = queue.redis.keys("worker:*")
    for worker_key in workers:
        heartbeat = queue.redis.hget(worker_key, "last_heartbeat")
        if heartbeat and time.time() - float(heartbeat) > 60:
            print(f"Worker {worker_key} appears dead")
```

---

## 11. Production Deployment

```python
# production.py
import signal
import logging
from video_scheduler.core.worker_pool import WorkerPool
from video_scheduler.workers.hybrid_worker import HybridWorker

logging.basicConfig(level=logging.INFO)

def main():
    pool = WorkerPool(
        worker_type=HybridWorker,
        num_workers=4,
        redis_url="redis://prod-redis:6379"
    )
    
    # Handle shutdown gracefully
    def signal_handler(sig, frame):
        print("Shutting down...")
        pool.stop()
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    pool.start()

if __name__ == "__main__":
    main()
```

---

## 12. Configuration Reference

| Parameter | Default | Description |
|-----------|---------|-------------|
| `redis_url` | redis://localhost:6379 | Redis connection URL |
| `max_retries` | 3 | Maximum task retry attempts |
| `task_timeout` | 3600 | Task timeout in seconds |
| `max_concurrent` | 10 | Async worker concurrency |
| `max_workers` | 4 | Threaded worker thread count |
| `thread_workers` | 4 | Hybrid worker thread pool |
| `async_tasks` | 10 | Hybrid worker async concurrency |