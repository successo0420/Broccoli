# Broccoli Use-Case Examples

This file shows practical patterns for using Broccoli in real applications.

---

## 1) Basic background jobs (email + webhook)

```python
from broccoli.core.task.task import Task
from broccoli.core.task.task_queue import TaskQueue
from broccoli.workers.threaded_worker import ThreadedWorker

queue = TaskQueue(redis_url="redis://localhost:6379")
worker = ThreadedWorker(redis_url="redis://localhost:6379", max_workers=4)

@worker.registry.register("send_email")
def send_email(payload):
    print(f"Email -> {payload['to']}")
    return {"sent": True}

@worker.registry.register("post_webhook")
def post_webhook(payload):
    print(f"Webhook -> {payload['url']}")
    return {"posted": True}

queue.push(Task(task_type="send_email", payload={"to": "user@example.com"}), priority=1)
queue.push(Task(task_type="post_webhook", payload={"url": "https://api.example.com/hook"}), priority=2)

worker.start()
```

---

## 2) Priority queues for urgent vs normal work

```python
from broccoli.core.task.task import Task
from broccoli.core.task.task_queue import TaskQueue

queue = TaskQueue(redis_url="redis://localhost:6379")

# Higher priority (0)
queue.push(Task(task_type="fraud_check", payload={"order_id": "A-100"}), priority=0)

# Lower priority (5)
queue.push(Task(task_type="weekly_report", payload={"team": "ops"}), priority=5)
```

Use this pattern when you want critical jobs to run before batch/maintenance work.

---

## 3) Dependency flow (validate -> process)

```python
from broccoli.core.task.task import Task
from broccoli.core.task.task_queue import TaskQueue

queue = TaskQueue(redis_url="redis://localhost:6379")

validate_task = Task(task_type="validate_invoice", payload={"invoice_id": "INV-7"})
queue.push(validate_task)

process_task = Task(
    task_type="process_invoice",
    payload={"invoice_id": "INV-7"},
    depends_on=validate_task.task_id,
)
queue.push(process_task)
```

`process_invoice` waits automatically until `validate_invoice` is completed.

---

## 4) Retry + dead-letter operations

```python
from broccoli.core.task.task import Task
from broccoli.core.task.task_queue import TaskQueue

queue = TaskQueue(redis_url="redis://localhost:6379")

queue.push(
    Task(
        task_type="charge_card",
        payload={"payment_id": "pay_123"},
        max_retries=5,
    )
)
```

If retries are exhausted, inspect/replay from CLI:

```bash
broccoli dead list
broccoli dead requeue <task_id>
```

---

## 5) Async worker for I/O-heavy integrations

```python
from broccoli.workers.async_worker import AsyncWorker

worker = AsyncWorker(
    redis_url="redis://localhost:6379",
    max_concurrent=50,
)

@worker.registry.register("sync_partner")
def sync_partner(payload):
    # Often network-heavy / API-bound
    return {"partner": payload["partner"], "ok": True}

worker.start()
```

Use this for API calls, notifications, and web integration pipelines.

---

## 6) Hybrid worker for mixed CPU + I/O

```python
from broccoli.workers.hybrid_worker import HybridWorker

worker = HybridWorker(
    redis_url="redis://localhost:6379",
    thread_workers=8,
    async_tasks=100,
)

@worker.registry.register("resize_image")
def resize_image(payload):
    # CPU-heavy image processing
    return {"path": payload["path"], "resized": True}

@worker.registry.register("notify_user")
def notify_user(payload):
    # I/O-heavy HTTP/email call
    return {"user_id": payload["user_id"], "notified": True}

worker.start()
```

---

## 7) Chain pipeline (extract -> transform -> publish)

```python
from broccoli.core.chain.task_chain import TaskChain
from broccoli.workers.chain_worker import ChainWorker

chain = TaskChain(redis_url="redis://localhost:6379")
worker = ChainWorker(redis_url="redis://localhost:6379")

@worker.registry.register("extract")
def extract(payload):
    return {"rows": [1, 2, 3]}

@worker.registry.register("transform")
def transform(payload):
    prev = payload.get("__previous_result", {})
    return {"rows": [x * 10 for x in prev.get("rows", [])]}

@worker.registry.register("publish")
def publish(payload):
    prev = payload.get("__previous_result", {})
    return {"published": len(prev.get("rows", []))}

chain_id = chain.chain(
    [
        {"task_type": "extract", "payload": {}},
        {"task_type": "transform", "payload": {}},
        {"task_type": "publish", "payload": {}},
    ]
)

print("started chain:", chain_id)
worker.start()
```

---

## 8) Worker pool for horizontal scale

```bash
broccoli worker start --type threaded --pool --num-workers 6 --concurrency 4
```

This launches six worker instances (each with threaded execution), useful when one process is not enough.

---

## 9) Queue inspection and debugging from CLI

```bash
broccoli queue stats --format json
broccoli queue list --status pending --limit 25
broccoli queue list --status in_progress --limit 25
broccoli queue get <task_id>
broccoli queue waiting <parent_id>
broccoli health
```

---

## 10) Recovery after worker crash/redeploy

Run one-shot recovery before normal startup:

```bash
broccoli worker start --type async --concurrency 20 --recover-stalled 900
```

Or rely on startup recovery (enabled by default):

```bash
broccoli worker start --type async --concurrency 20 --recover-on-startup
```

---

## 11) Multi-tenant/isolated queue setup

```bash
# tenant A workers
broccoli worker start --type threaded --redis-url redis://localhost:6379 --queue-name tenantA:queue --task-prefix tenantA:task

# tenant B workers
broccoli worker start --type threaded --redis-url redis://localhost:6379 --queue-name tenantB:queue --task-prefix tenantB:task
```

This isolates queue state and task metadata by queue and prefix.

---

## 12) Production readiness checklist (practical)

- Register all task handlers before `worker.start()`.
- Keep handlers idempotent (safe on retry).
- Set `max_retries` per task type based on business criticality.
- Monitor `broccoli dead list` and queue stats regularly.
- Tune concurrency by worker type and workload profile.
- Use recovery flags during deployments and incident response.
