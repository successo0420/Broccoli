# Broccoli – Next Release Roadmap

Based on community feedback, production experience, and a thorough design review, we’ve identified the following priority improvements for the next major release.

---

## 🔴 Critical – Must Fix

### 1. Atomic `push` + dependency registration (Lua script)

The current **register‑then‑verify** pattern eliminates the most common race condition, but a theoretical window remains:

- Worker A pushes task `T` depending on task `P`.
- Worker A adds `T` to `dependency:P` and reads `P`’s status.
- Worker B completes `P` **after** the `SADD` but **before** Worker A reads the status. Worker B sees an empty dependency set, deletes it, and releases no tasks.
- Worker A then sees `P` as `"in_progress"` (not `"completed"`) and leaves `T` waiting forever.

**Fix:** Replace the two‑round‑trip with a single Lua script that atomically checks the parent’s status, either enqueues the task immediately (if parent is already complete) or registers the dependency and sets status to `"waiting"`.

**Implementation:** Use `EVALSHA` with keys for parent hash, queue, dependency set, and task hash. This makes the operation fully atomic and eliminates the remaining race.

---

### 2. Call `recover_stalled()` on worker startup

Workers that restart after a crash (or after being killed) currently leave tasks in the `:processing` set. Unless an administrator explicitly runs `recover_stalled()`, these tasks stay stuck until the timeout fires.

**Improvement:** Add an optional `recover_on_startup` flag to workers (default `True`) that calls `queue.recover_stalled()` once before entering the main loop. This ensures any orphaned tasks are re‑enqueued immediately, minimising downtime.

---

### 3. Monitor `:processing` set size

A growing `:processing` set without corresponding completion/failure is the best indicator of worker crashes, network issues, or stuck handlers. Yet today there is no built‑in monitoring.

**Feature:** Add a health‑check endpoint or a simple CLI command that reports:
- `runnable` count
- `processing` count
- `dead_letter` count
- **oldest processing timestamp** (so you can see if any task has been in flight for longer than your typical execution time)

This can be exposed via `queue.processing_stats()` and used in alerting systems.

---

## 🟡 High‑Priority Enhancements

### 4. Fan‑out / multi‑dependency support

Currently a task can have only one parent (`depends_on` is a string). For real‑world workflows, you often need to wait for **several** upstream tasks before starting the next stage (e.g., after all chunks are processed).

**Solution:** Support a `depends_on` list or set. The push logic would need to:
- Store the list of parent IDs in the task hash.
- Add the task to each parent’s `dependency:<parent_id>` set.
- When a parent completes, it checks all waiting tasks; for each, it decrements a remaining‑dependencies counter. When that counter reaches zero, the task is enqueued.

This adds complexity, but many users have asked for it. We can implement it using a Redis hash `deps:<task_id>` with a counter and a Lua script for atomic decrement‑and‑release.

---

### 5. Delayed / scheduled tasks

No native support for “run this task in 30 minutes” or “at 2 PM”.

**Implementation plan:**
- Add a **scheduled sorted set** `<base>:scheduled` with score = Unix timestamp of desired execution.
- A separate scheduler thread (or lightweight process) polls the scheduled set using `ZRANGEBYSCORE` and moves tasks to the runnable queue when their time arrives.
- Workers can push tasks with a `run_after` or `run_at` parameter.
- Ensure the scheduler is idempotent and resilient to restarts (use Redis `SETNX` for leader election if running multiple schedulers).

---

### 6. Visibility into waiting tasks

Currently you can see runnable tasks and in‑flight ones, but there’s no easy way to list tasks that are blocked on a dependency. This makes debugging deadlocks painful.

**Add:**
- `queue.get_waiting_for(task_id)` – returns list of task IDs waiting on a given parent.
- `queue.get_waiting_tasks()` – returns all task IDs with status `"waiting"` (requires scanning or a separate index).
- A simple admin CLI command to dump all waiting tasks and their dependency trees.

<!-- ---

### 7. ChainWorkerMixin audit & documentation - this is done.

The `ChainWorkerMixin` was written for an older queue implementation (where `pop()` handled dependencies). Its logic may now be redundant or conflict with the new `depends_on`‑based approach.

**Action:**
- Review `ChainWorkerMixin` to ensure it uses `Task.depends_on` to link steps, rather than manually managing a separate sequencing mechanism.
- Update documentation with clear examples of chain creation, step payloads, and the `__previous_result` injection.

--- -->

## 🟢 Performance & Production Readiness

### 8. Better defaults for `max_retries`

The current default is 0 – no retries. A transient network hiccup or a temporary file‑lock can permanently fail a task. For most workloads, 2 or 3 retries are more appropriate.

**Change:** Set `Task.max_retries` default to 2 (or 3) in the `Task` constructor, and document that you can override it per task.

---

### 9. Redis Streams migration guide

The current sorted‑set + dependency‑set design works well for moderate scale, but Redis Streams offer consumer groups, at‑least‑once delivery, and built‑in acknowledgments. For production environments requiring high availability and guaranteed delivery, Streams reduce the amount of custom reliability logic.

**Deliverable:** Provide a documented migration path and a compatibility layer that can use either the old “manual” queue or a Streams‑backed queue, allowing users to choose based on their requirements.

---

### 10. Dead‑letter inspection & manual requeue

Tasks in the dead‑letter set are currently just IDs. It’s hard to see why they failed or to retry them manually.

**Improvements:**
- Store the full task hash (or a copy) in the dead‑letter set, or create a separate hash `dl:<task_id>` with the final error and payload.
- Add a `requeue_dead(task_id)` method that moves a task back to the runnable queue (resetting retry count) – useful for administrative fixes.
- Provide a CLI tool to list dead‑letter tasks with their errors.

---

### 11. Integration with structured logging / OpenTelemetry

Add optional trace‑IDs, span creation, and context propagation to make it easier to track a task’s lifecycle across multiple services. This can be implemented as a set of optional middleware handlers that users can plug in.

---

## 📚 Documentation & Testing

- **Comprehensive benchmarking** – publish performance numbers for different worker types (tasks per second, latency percentiles) to help users choose the right concurrency model.
- **More examples** – include a “real‑world” example (e.g., video processing pipeline, ETL, report generation) in the repository.
- **Test coverage** – increase unit and integration tests, especially around the dependency‑release logic and the retry state machine.

---

## 💡 How to Contribute

If you’re interested in working on any of these items, please open an issue or a draft PR. We’re happy to provide guidance and review.

We believe these improvements will make Broccoli even more robust and production‑ready, while keeping its core simplicity intact.

**Happy queuing!**


Talk about making all the handlers in one file and then importing the file to the different files you want to use different workers with so they all see the same handlers.