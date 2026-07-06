# Technical review — Broccoli task queue

This document covers the architecture of the redesigned system, the reasoning behind key decisions, remaining limitations, and recommendations for production use.

---

## Architecture overview

The redesign moves from a scan-and-requeue model to an event-driven one. The central principle: by the time a worker calls `pop()`, the task is guaranteed to be runnable. Dependency resolution happens entirely at the boundary events — `push()` and `complete()` — so the hot path is clean.

### Redis key layout

| Key | Type | Purpose |
|-----|------|---------|
| `<ns>:queue` | Sorted set | Runnable tasks. Score = `priority * 10^12 + seq`. |
| `<ns>:processing` | Sorted set | In-flight tasks. Score = Unix timestamp of when popped. |
| `<ns>:sequence` | String (int) | Monotonic counter; `INCR` on every enqueue. |
| `<prefix>:<task_id>` | Hash | Task metadata (type, payload, status, retries, result…). |
| `dependency:<task_id>` | Set | Task IDs waiting for `<task_id>` to complete. |
| `result:<task_id>` | String (JSON) | Stored results, used by `HybridWorker` with a TTL. |
| `chain:<chain_id>` | Hash | Chain metadata, used by `ChainWorker`. |
| `chain:<chain_id>:tasks` | String (JSON) | Ordered list of task IDs in a chain. |

The two sorted sets (`queue` and `processing`) together form an explicit state machine. A task ID exists in exactly one of them at any given time, or in neither (completed/cleaned up) or the `dependency:<id>` set (waiting).

### Task state machine

```
[push, no dependency]  →  pending  (in :queue)
[push, has dependency] →  waiting  (in dependency:<parent>)
[pop]                  →  in_progress (in :processing)
[complete]             →  completed   (removed from :processing)
[fail, retries left]   →  pending     (back in :queue via requeue)
[fail, no retries]     →  failed      (removed from :processing)
[dependency released]  →  pending     (moved from dependency set to :queue)
```

### Dependency resolution

`push()` uses a register-then-verify pattern to close the TOCTOU race window that a check-then-register approach creates:

1. Register the task in `dependency:<parent_id>` first.
2. Then read the parent's status.
3. If the parent already completed (raced past us), remove from the dependency set and enqueue immediately.

Without this ordering, a parent could complete between the status check and the `SADD`, leaving the dependent permanently stuck. A Lua script would close the remaining window entirely; the current approach reduces it to near-zero in practice.

`complete()` reads the `dependency:<task_id>` set, enqueues each waiting task (using the priority stored in the task hash at push time), then deletes the set. The parent's status is written to Redis (`_update_task`) before `complete()` is called, so any concurrent `push()` that checks the parent's status will see `completed` and enqueue immediately rather than waiting.

### Priority and FIFO ordering

Score in the runnable queue is `priority * 1_000_000_000_000 + monotonic_seq`. This gives:

- Strict ordering across priority tiers: every priority-0 task is processed before any priority-1 task.
- True FIFO within a tier: tasks at the same priority level are processed in the order they were enqueued.
- No timestamp dependency: FIFO is guaranteed by the sequence counter, not wall-clock time, which makes it deterministic under high concurrency.

The `sequence` counter is per-namespace, shared across all workers connecting to the same queue. `INCR` is atomic in Redis so there are no races.

### Processing set and crash recovery

The processing sorted set uses Unix timestamps as scores (not the FIFO score). This makes `recover_stalled` straightforward: any task with a score older than `time.time() - timeout_seconds` has been in-flight too long and gets re-enqueued. Workers connecting at startup should call `queue.recover_stalled()` to handle tasks orphaned by a previous crash.

---

## Worker execution models

All workers inherit `_handle_task_result` from `BaseWorker`. This is the single implementation of the retry state machine and queue transition logic. Previously, `ThreadedWorker`, `AsyncWorker`, and `HybridWorker` each had their own copy, which had drifted out of sync.

### BaseWorker

Single-threaded blocking loop. Simple and correct. Use for low-volume workloads where simplicity outweighs throughput.

### ThreadedWorker

A `ThreadPoolExecutor` runs task handlers concurrently. The main loop checks available capacity before popping; tasks track in `active_tasks` (with a lock) and are removed in the `finally` block of `process_task`. Redis operations happen on worker threads — fine for `redis-py` which is thread-safe.

### AsyncWorker

Dispatches tasks as asyncio coroutines via `asyncio.create_task`. Blocking handlers run in the default thread-pool executor via `run_in_executor`. The semaphore provides backpressure. Suitable for large numbers of I/O-bound tasks where thread-per-task overhead would be significant. `asyncio.get_running_loop()` is used throughout (not the deprecated `get_event_loop()`).

### HybridWorker

Asyncio controls concurrency; a dedicated `ThreadPoolExecutor` runs the blocking handlers. Compared to `AsyncWorker`, this gives explicit control over the thread pool size (separate from the default executor shared with the rest of the process). `HybridWorker` uses its own TTL-based result storage (`setex` to `result:<id>`) rather than `ResultBackend`. It overrides `post_process` to be a no-op for result storage and callbacks — those are handled in `_handle_hybrid_result` — preventing double-firing.

### WorkerPool

Spawns N instances of any worker type, each in a daemon thread. The `shutdown_flag` event blocks the calling thread until `stop()` or a signal handler fires. Workers created in `__init__` are not re-created in `start()` (that was the original bug — double instantiation).


## Known limitations

### No atomic push + dependency registration

The register-then-verify pattern in `push()` significantly reduces the race window but does not eliminate it entirely. Between the `SADD` and the subsequent `HGET`, a concurrent `complete()` could read the dependency set (without the new task) and delete it. The new task would then be stuck.

The correct fix is a Lua script that makes the check-and-register atomic. This trades the current two-round-trip approach for a single `EVALSHA`. The script would:

```lua
local parent_status = redis.call('HGET', KEYS[1], 'status')
if parent_status == 'completed' then
    redis.call('ZADD', KEYS[2], ARGV[1], ARGV[2])   -- enqueue directly
    return 1
else
    redis.call('SADD', KEYS[3], ARGV[2])              -- register as waiting
    redis.call('HSET', KEYS[4], 'status', 'waiting')
    return 0
end
```

### No fan-out dependencies

A task can have at most one parent (`depends_on` is a single ID). Fan-in (wait for multiple parents) is not supported. For pipelines requiring fan-in, the current approach is to wrap the final fan-out tasks in a chain.

### ChainWorkerMixin not reviewed

`ChainWorkerMixin` (not included in this codebase) is responsible for setting `depends_on` on chain steps. The `ChainWorker` relies on this being correct. If `ChainWorkerMixin` was written against the old queue semantics (where dependency resolution happened inside `pop()`), it may need updating to push tasks with `depends_on` set rather than doing its own sequencing.

### No delayed tasks

There is no mechanism to schedule a task for future execution (e.g. "run in 30 minutes"). This would require a separate scheduled set and a scheduler process that moves tasks into `:queue` at the right time. `redis-py`'s `zadd` with a future-timestamp score and a polling loop is the standard approach.

### Single-node Redis

The queue assumes a single Redis instance. Under network partition or failover, in-flight tasks in `:processing` may be lost or duplicated. For stronger delivery guarantees, consider Redis Cluster with `RedLock` for distributed locking, or migrate to a purpose-built broker (Redis Streams, RabbitMQ).

### No visibility into waiting tasks

There is no API to list tasks currently in the `waiting` state or to inspect the `dependency:<id>` sets. Adding a `get_waiting(task_id)` method to `TaskQueue` and a health-check endpoint would make debugging dependency deadlocks significantly easier.

---

## Recommendations

**Add the Lua script for atomic push.** This is the highest-priority remaining item. The current register-then-verify approach is safe in the vast majority of cases but has a theoretical failure mode under high concurrency on slow Redis connections.

**Call `recover_stalled` on worker startup.** It is now correct and usable. Workers that restart after a crash should call it before entering their main loop to avoid tasks sitting in `:processing` indefinitely.

**Set `max_retries` explicitly on all tasks.** The default of 0 means no retries. A transient Redis connection error during handler execution will permanently fail the task. Even `max_retries=2` provides meaningful resilience.

**Monitor the `:processing` set size.** If it grows and doesn't shrink, tasks are being popped but not completing (and `recover_stalled` is not being called). This is the primary indicator of a stalled or crashed worker.

**Consider Redis Streams for production.** Redis Streams provide consumer groups, at-least-once delivery semantics, and built-in acknowledgement — which is what this queue is approximating manually with the `:processing` set. The current implementation is well-suited for development and moderate production workloads; for high-availability requirements, Streams reduce the amount of reliability logic you have to maintain yourself.