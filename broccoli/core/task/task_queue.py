# broccoli/core/task/task_queue.py
import logging
import time
from typing import Optional

from broccoli.core.redis_controller import RedisController
from broccoli.core.task.task import Task

logger = logging.getLogger(__name__)

# Max tasks per priority tier before FIFO scores would collide.
# 10^12 tasks per tier is effectively unlimited in practice.
_FIFO_TIER_SIZE = 1_000_000_000_000

# Redis field used to carry a task's original priority through the waiting state
# so it can be restored when the dependency is released.
_PRIORITY_FIELD = "_broccoli_priority"


class TaskQueue:
    """
    Priority queue backed by three Redis structures:

    ``<base>:queue``       — sorted set of runnable task IDs
                             score = priority_tier * _FIFO_TIER_SIZE + monotonic_seq
    ``<base>:processing``  — sorted set of in-flight task IDs
                             score = Unix timestamp of when the task was popped
                             (used for crash recovery, not for ordering)
    ``<base>:sequence``    — monotonic counter for FIFO ordering within a priority tier
    ``dependency:<id>``    — set of task IDs waiting for task <id> to complete

    Dependency resolution happens at *push time* and *completion time*, never
    inside ``pop()``.  Workers only ever see runnable tasks.
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        queue_name: str = "tasks:queue",
        task_prefix: str = "task",
    ):
        self.redis_url = redis_url
        self._redis = RedisController(redis_url).get_client()
        self.queue_key = queue_name
        self.task_prefix = task_prefix

        # Derive sibling keys from queue_name so all keys share a namespace.
        # e.g. "tasks:queue" → "tasks:processing", "tasks:sequence"
        base = (
            queue_name[: -len(":queue")]
            if queue_name.endswith(":queue")
            else queue_name
        )
        self.processing_key = f"{base}:processing"
        self.sequence_key = f"{base}:sequence"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def push(self, task: Task, priority: int = 0) -> str:
        """
        Persist task metadata then either enqueue it or register it as waiting.

        If the task has no dependency, or its dependency is already completed,
        it goes straight into the runnable queue.  Otherwise it is stored as
        ``waiting`` and added to ``dependency:<parent_id>`` so ``complete()``
        can release it later.

        Race condition handled with a register-then-verify pattern:
          1. Register in the dependency set first.
          2. Re-check the parent's status.
          3. If parent is already completed (raced past us), release immediately.
        This eliminates the TOCTOU window present in a check-then-register approach.
        """
        # Always persist task metadata first so workers can read it after pop().
        self._redis.hset(f"{self.task_prefix}:{task.task_id}", mapping=task.to_dict())

        if task.depends_on:
            # Mark as waiting immediately and register in the dependency set.
            # We do this BEFORE reading the parent's status to close the
            # check-then-register race window.
            task.status = "waiting"
            pipe = self._redis.pipeline()
            pipe.hset(f"{self.task_prefix}:{task.task_id}", "status", "waiting")
            # Store the requested priority so complete() can restore it.
            pipe.hset(
                f"{self.task_prefix}:{task.task_id}", _PRIORITY_FIELD, str(priority)
            )
            pipe.sadd(f"dependency:{task.depends_on}", task.task_id)
            pipe.execute()

            # Now verify: if the parent already completed before our sadd, release now.
            dep_status = self._redis.hget(
                f"{self.task_prefix}:{task.depends_on}", "status"
            )
            parent_done = (
                dep_status
                and (
                    dep_status.decode() if isinstance(dep_status, bytes) else dep_status
                )
                == "completed"
            )

            if parent_done:
                # Parent completed before (or concurrently with) our registration.
                # Release ourselves immediately and clean up.
                self._redis.srem(f"dependency:{task.depends_on}", task.task_id)
                self._enqueue(task.task_id, priority)
                logger.debug(
                    f"Task {task.task_id}: dependency {task.depends_on} already "
                    "completed, enqueued immediately"
                )
            else:
                logger.debug(
                    f"Task {task.task_id} waiting on dependency {task.depends_on}"
                )
        else:
            self._enqueue(task.task_id, priority)

        return task.task_id

    def pop(self) -> Optional[Task]:
        """
        Pop the highest-priority runnable task and move it to the processing set.

        No dependency checking happens here — that was handled at push / complete
        time.  Everything in the queue is ready to run.

        Returns ``None`` if the queue is empty after the blocking timeout.
        """
        result = self._redis.bzpopmin(self.queue_key, timeout=1)
        if result is None:
            return None

        _, task_id, _fifo_score = result
        task_id = task_id.decode() if isinstance(task_id, bytes) else task_id

        task_data = self._redis.hgetall(f"{self.task_prefix}:{task_id}")
        if not task_data:
            logger.warning(f"Task data missing for {task_id!r}; dropping from queue")
            return None

        # Score in the processing set is a Unix timestamp so recover_stalled()
        # can identify tasks that have been in-flight longer than expected.
        now = time.time()
        self._redis.zadd(self.processing_key, {task_id: now})
        self._redis.hset(f"{self.task_prefix}:{task_id}", "status", "in_progress")

        task = Task.from_dict(task_data)
        task.status = "in_progress"
        return task

    def complete(self, task: Task) -> None:
        """
        Called after a task finishes successfully.

        1. Removes the task from the processing set.
        2. Reads any tasks that were waiting on this one and enqueues them,
           restoring their original priority.
        3. Deletes the dependency set.
        """
        task_id = task.task_id
        self._redis.zrem(self.processing_key, task_id)

        dep_key = f"dependency:{task_id}"
        waiting_ids = self._redis.smembers(dep_key)
        if waiting_ids:
            for raw_id in waiting_ids:
                wid = raw_id.decode() if isinstance(raw_id, bytes) else raw_id
                # Restore the priority the caller supplied when pushing the waiting task.
                raw_pri = self._redis.hget(f"{self.task_prefix}:{wid}", _PRIORITY_FIELD)
                priority = int(raw_pri) if raw_pri else 0
                self._enqueue(wid, priority)
                logger.info(
                    f"Released waiting task {wid} "
                    f"(dependency {task_id} completed, priority={priority})"
                )
            self._redis.delete(dep_key)

    def fail(self, task: Task) -> None:
        """Remove a permanently failed (or skipped) task from the processing set."""
        self._redis.zrem(self.processing_key, task.task_id)

    def requeue(self, task_id: str, priority: int = 0) -> None:
        """
        Move a task from the processing set back to the runnable queue (retry).

        A fresh FIFO sequence number is issued so the retry goes to the back of
        its priority tier rather than jumping the queue.
        """
        self._redis.zrem(self.processing_key, task_id)
        self._enqueue(task_id, priority)

    def recover_stalled(self, timeout_seconds: int = 3600) -> int:
        """
        Crash-recovery helper: re-enqueue tasks that have been in the processing
        set for longer than ``timeout_seconds``.

        The processing set uses Unix timestamps as scores (set by ``pop()``),
        so the threshold is simply ``time.time() - timeout_seconds``.

        Returns the number of tasks recovered.
        """
        threshold = time.time() - timeout_seconds
        stalled = self._redis.zrangebyscore(self.processing_key, "-inf", threshold)
        for raw_id in stalled:
            tid = raw_id.decode() if isinstance(raw_id, bytes) else raw_id
            self.requeue(tid)
            logger.warning(f"Crash-recovery: re-enqueued stalled task {tid}")
        return len(stalled)

    # ------------------------------------------------------------------
    # Convenience helpers
    # ------------------------------------------------------------------

    def get_task(self, task_id: str) -> Optional[Task]:
        """Fetch task metadata by ID, or ``None`` if it doesn't exist."""
        task_data = self._redis.hgetall(f"{self.task_prefix}:{task_id}")
        if not task_data:
            return None
        return Task.from_dict(task_data)

    def get_queue_name(self) -> str:
        return self.queue_key

    def is_empty(self) -> bool:
        return self._redis.zcard(self.queue_key) == 0

    def pop_with_timeout(self, timeout: int = 1) -> Optional[Task]:
        """Alias for ``pop()``; timeout is already handled internally."""
        return self.pop()

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _enqueue(self, task_id: str, priority: int = 0) -> None:
        """
        Add *task_id* to the runnable sorted set.

        Score = priority * _FIFO_TIER_SIZE + monotonic_seq

        This gives strict priority ordering across tiers *and* FIFO within a
        tier, without requiring timestamps or Lua scripts.
        """
        seq = self._redis.incr(self.sequence_key)
        score = priority * _FIFO_TIER_SIZE + seq
        self._redis.zadd(self.queue_key, {task_id: score})
        self._redis.hset(f"{self.task_prefix}:{task_id}", "status", "pending")
        logger.debug(f"Enqueued task {task_id} (priority={priority}, seq={seq})")

    def is_fully_drained(self) -> bool:
        """True only when both the runnable queue and processing set are empty."""
        pipe = self._redis.pipeline()
        pipe.zcard(self.queue_key)
        pipe.zcard(self.processing_key)
        runnable, processing = pipe.execute()
        return runnable == 0 and processing == 0
