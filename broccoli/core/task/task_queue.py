# broccoli/core/task/task_queue.py
import json
import logging
import time
from typing import List, Optional

from broccoli.core.redis_controller import RedisController
from broccoli.core.task.task import Task

logger = logging.getLogger(__name__)

# Max tasks per priority tier before FIFO scores would collide.
# 10^12 tasks per tier is effectively unlimited in practice.
_FIFO_TIER_SIZE = 1_000_000_000_000

# Redis field used to carry a task's original priority through the waiting state
# so it can be restored when dependencies are released.
_PRIORITY_FIELD = "_broccoli_priority"
_DEAD_LETTER_PREFIX = "dl"

# New field to track how many dependencies are still incomplete for a waiting task.
# Stored under this key in the task's hash. When it reaches 0, the task is enqueued.
_REMAINING_DEPS_FIELD = "_remaining_deps"


class TaskQueue:
    """
    Priority queue backed by Redis structures.

    Supports multiple dependencies per task. Tasks with unresolved dependencies
    are stored in sets associated with each dependency (``dependency:<id>``).
    A counter (``_remaining_deps``) tracks how many dependencies are still pending.
    When all dependencies complete, the task is atomically enqueued.

    Structures:
        ``<base>:queue``       — sorted set of runnable task IDs
                                 score = priority_tier * _FIFO_TIER_SIZE + monotonic_seq
        ``<base>:processing``  — sorted set of in-flight task IDs
                                 score = Unix timestamp of when popped (for crash recovery)
        ``<base>:sequence``    — monotonic counter for FIFO ordering within a priority tier
        ``dependency:<id>``    — set of task IDs waiting for task <id> to complete
        ``<task_prefix>:<id>`` — hash storing all task metadata
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        queue_name: str = "tasks:queue",
        task_prefix: str = "task",
        decode_responses: bool = True,
        redis_config: Optional[dict] = None,
    ):
        self.redis_url = redis_url
        redis_config = redis_config or {}
        self._redis = RedisController(
            redis_url,
            decode_responses=decode_responses,
            **redis_config,
        ).get_client()
        self.queue_key = queue_name
        self.task_prefix = task_prefix

        # Derive sibling keys from queue_name so all keys share a namespace.
        # e.g. "tasks:queue" → "tasks:processing", "tasks:sequence"
        base = (
            queue_name[: -len(":queue")]
            if queue_name.endswith(":queue")
            else queue_name
        )
        self.base = base
        self.processing_key = f"{base}:processing"
        self.sequence_key = f"{base}:sequence"
        self.dead_letter_key = f"{self.task_prefix}:dead_letter"

        # ------------------------------------------------------------------
        # Lua script for pushing a task with multiple dependencies.
        #
        # KEYS layout:
        #   KEYS[1] = task_hash_key (e.g., "task:abc123")
        #   KEYS[2] = queue_key (sorted set of runnable)
        #   KEYS[3] = sequence_key (counter)
        #   KEYS[4] = parent_hash_key for dependency 1
        #   KEYS[5] = dependency_set_key for dependency 1 (e.g., "dependency:parent1")
        #   KEYS[6] = parent_hash_key for dependency 2
        #   KEYS[7] = dependency_set_key for dependency 2
        #   ... and so on (paired).
        #
        # ARGV:
        #   ARGV[1] = task_id
        #   ARGV[2] = priority (int)
        #   ARGV[3] = tier_size (int)
        #   ARGV[4] = priority_field name (string)
        #
        # Returns:
        #   1 if all dependencies are already completed → task enqueued immediately
        #   0 if at least one dependency is incomplete → task set to 'waiting'
        # ------------------------------------------------------------------
        self._push_with_dependencies_script = self._redis.register_script("""
            local task_hash_key = KEYS[1]
            local queue_key = KEYS[2]
            local seq_key = KEYS[3]
            local task_id = ARGV[1]
            local priority = tonumber(ARGV[2]) or 0
            local tier_size = tonumber(ARGV[3]) or 1000000000000
            local priority_field = ARGV[4]

            local remaining = 0
            -- Iterate over pairs of keys (parent_hash, dep_set)
            -- Note: KEYS[4..N] are pairs; we increment by 2 each time.
            local i = 4
            while i <= #KEYS do
                local parent_hash_key = KEYS[i]
                local dep_set_key = KEYS[i+1]
                -- Check if the parent is already completed
                local status = redis.call('HGET', parent_hash_key, 'status')
                if status ~= 'completed' then
                    -- Parent not done → register this task as waiting on it
                    redis.call('SADD', dep_set_key, task_id)
                    remaining = remaining + 1
                end
                i = i + 2
            end

            if remaining == 0 then
                -- All dependencies are already completed → enqueue now
                local seq = redis.call('INCR', seq_key)
                local score = (priority * tier_size) + seq
                redis.call('ZADD', queue_key, score, task_id)
                redis.call('HSET', task_hash_key,
                    'status', 'pending',
                    priority_field, tostring(priority)
                )
                return 1
            else
                -- Some dependencies are incomplete → store remaining count
                redis.call('HSET', task_hash_key,
                    'status', 'waiting',
                    priority_field, tostring(priority),
                    '_remaining_deps', tostring(remaining)
                )
                return 0
            end
        """)

        # ------------------------------------------------------------------
        # Lua script for completing a task and releasing its dependents.
        #
        # KEYS layout:
        #   KEYS[1] = task_hash_key (the completed task) – unused but kept for symmetry
        #   KEYS[2] = dependency_set_key (e.g., "dependency:task_id")
        #   KEYS[3] = queue_key (sorted set)
        #   KEYS[4] = sequence_key (counter)
        #
        # ARGV:
        #   ARGV[1] = tier_size (int)
        #   ARGV[2] = priority_field name (string)
        #   ARGV[3] = remaining_deps_field name (string)
        #   ARGV[4] = task_prefix (string, used to build hash keys for dependents)
        #
        # For each waiting task, decrement its remaining dependency count.
        # When it reaches 0, enqueue the task with its restored priority.
        # Finally, delete the dependency set.
        # ------------------------------------------------------------------
        self._complete_script = self._redis.register_script("""
            local dep_set_key = KEYS[2]
            local queue_key = KEYS[3]
            local seq_key = KEYS[4]
            local tier_size = tonumber(ARGV[1]) or 1000000000000
            local priority_field = ARGV[2]
            local remaining_field = ARGV[3]
            local task_prefix = ARGV[4]

            -- Get all tasks waiting on this completed task
            local waiting_ids = redis.call('SMEMBERS', dep_set_key)
            for _, wid in ipairs(waiting_ids) do
                local task_key = task_prefix .. ':' .. wid
                -- Read the current remaining dependency count
                local remaining = redis.call('HGET', task_key, remaining_field)
                if remaining then
                    remaining = tonumber(remaining) - 1
                    if remaining <= 0 then
                        -- All dependencies now satisfied → enqueue
                        local priority = redis.call('HGET', task_key, priority_field) or 0
                        priority = tonumber(priority) or 0
                        local seq = redis.call('INCR', seq_key)
                        local score = (priority * tier_size) + seq
                        redis.call('ZADD', queue_key, score, wid)
                        redis.call('HSET', task_key,
                            'status', 'pending',
                            remaining_field, '0'
                        )
                    else
                        -- Still waiting on other dependencies → update count
                        redis.call('HSET', task_key, remaining_field, tostring(remaining))
                    end
                else
                    -- If missing, the task may have been enqueued already; skip.
                    -- This can happen if dependencies are released multiple times,
                    -- but we only ever call complete() once per task.
                end
            end

            -- Clean up the dependency set for this completed task
            redis.call('DEL', dep_set_key)
        """)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def push(self, task: Task, priority: int = 0) -> str:
        """
        Persist task metadata then either enqueue it or register it as waiting.

        If the task has no dependencies, it goes straight into the runnable queue.
        If it has dependencies, we atomically check each dependency's status:
          - If all are already completed, the task is enqueued immediately.
          - Otherwise, the task is added to each incomplete dependency's waiting set
            and its ``_remaining_deps`` counter is set to the number of incomplete deps.

        Dependency registration is atomic via a Lua script.
        """
        # Always persist task metadata first so workers can read it after pop().
        self._redis.hset(f"{self.task_prefix}:{task.task_id}", mapping=task.to_dict())

        if task.depends_on:
            # Build the key list for the Lua script:
            #   [task_hash, queue, seq, (parent_hash, dep_set) for each dep]
            keys = [
                f"{self.task_prefix}:{task.task_id}",  # KEYS[1]
                self.queue_key,  # KEYS[2]
                self.sequence_key,  # KEYS[3]
            ]
            for dep_id in task.depends_on:
                keys.append(f"{self.task_prefix}:{dep_id}")  # parent hash
                keys.append(f"dependency:{dep_id}")  # waiting set

            args = [
                task.task_id,
                str(priority),
                str(_FIFO_TIER_SIZE),
                _PRIORITY_FIELD,
            ]
            result = self._push_with_dependencies_script(keys=keys, args=args)
            if int(result) == 1:
                logger.debug(
                    f"Task {task.task_id} enqueued immediately (all deps done)"
                )
            else:
                logger.debug(f"Task {task.task_id} waiting on dependencies")
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
            logger.error(
                f"Task data missing for {task_id!r}; moving to dead-letter set"
            )
            now = time.time()
            self._redis.zadd(self.dead_letter_key, {task_id: now})
            self._redis.hset(
                f"{_DEAD_LETTER_PREFIX}:{task_id}",
                mapping={
                    "task_id": task_id,
                    "status": "failed",
                    "error": "Task hash missing at pop()",
                    "failed_at": str(now),
                },
            )
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
        2. Reads any tasks that were waiting on this one and decrements their
           remaining dependency counter. When the counter reaches zero, they are
           enqueued with their original priority.
        3. Deletes the dependency set.

        The entire release process is atomic via Lua script.
        """
        task_id = task.task_id
        self._redis.zrem(self.processing_key, task_id)

        dep_key = f"dependency:{task_id}"
        keys = [
            f"{self.task_prefix}:{task_id}",  # KEYS[1] (unused, but kept)
            dep_key,  # KEYS[2]
            self.queue_key,  # KEYS[3]
            self.sequence_key,  # KEYS[4]
        ]
        args = [
            str(_FIFO_TIER_SIZE),
            _PRIORITY_FIELD,
            _REMAINING_DEPS_FIELD,
            self.task_prefix,
        ]
        self._complete_script(keys=keys, args=args)

    def fail(self, task: Task) -> None:
        """Remove a permanently failed (or skipped) task from the processing set."""
        self._redis.zrem(self.processing_key, task.task_id)

    def requeue_dead(self, task_id: str) -> bool:
        """
        Requeue a dead-letter task by ID.

        Returns ``True`` if the task was restored and queued, ``False`` if the
        task was not found in dead letter storage.
        """
        if self._redis.zscore(self.dead_letter_key, task_id) is None:
            return False

        dead_data = self._redis.hgetall(f"{_DEAD_LETTER_PREFIX}:{task_id}")
        task_data = dead_data or self._redis.hgetall(f"{self.task_prefix}:{task_id}")
        if not task_data:
            return False

        decoded = {}
        for key, value in task_data.items():
            if isinstance(key, bytes):
                key = key.decode()
            if isinstance(value, bytes):
                value = value.decode()
            decoded[key] = value

        task = Task.from_dict(decoded)
        task.retries = 0
        task.status = "pending"
        task.error = None
        task.secondary_error = None

        raw_pri = decoded.get(_PRIORITY_FIELD)
        priority = int(raw_pri) if raw_pri else 0

        pipe = self._redis.pipeline()
        pipe.zrem(self.dead_letter_key, task_id)
        pipe.delete(f"{_DEAD_LETTER_PREFIX}:{task_id}")
        pipe.execute()
        self.push(task, priority=priority)
        return True

    def requeue(self, task_id: str, priority: Optional[int] = None) -> None:
        """
        Move a task from the processing set back to the runnable queue (retry).

        A fresh FIFO sequence number is issued so the retry goes to the back of
        its priority tier rather than jumping the queue.

        If ``priority`` isn't given explicitly, the task's originally-requested
        priority (stashed under ``_PRIORITY_FIELD`` at push time) is restored,
        matching the behaviour of ``complete()`` when it releases dependents.
        Without this, every retry would silently reset to priority 0 and could
        jump ahead of lower-priority tasks still waiting in the queue.

        Note: Dependencies have already been satisfied when the task was first
        enqueued, so we can safely re-enqueue without re-checking deps.
        """
        self._redis.zrem(self.processing_key, task_id)
        if priority is None:
            raw_pri = self._redis.hget(f"{self.task_prefix}:{task_id}", _PRIORITY_FIELD)
            priority = int(raw_pri) if raw_pri else 0
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
        # Reset remaining deps to 0 and set status to pending
        self._redis.hset(
            f"{self.task_prefix}:{task_id}",
            mapping={
                "status": "pending",
                _PRIORITY_FIELD: str(priority),
                _REMAINING_DEPS_FIELD: "0",  # no remaining deps
            },
        )
        logger.debug(f"Enqueued task {task_id} (priority={priority}, seq={seq})")

    def is_fully_drained(self) -> bool:
        """True only when both the runnable queue and processing set are empty."""
        pipe = self._redis.pipeline()
        pipe.zcard(self.queue_key)
        pipe.zcard(self.processing_key)
        runnable, processing = pipe.execute()
        return runnable == 0 and processing == 0

    def get_waiting_for(self, task_id: str) -> List[str]:
        """Return the IDs of tasks currently blocked on ``task_id``."""
        return [
            tid.decode() if isinstance(tid, bytes) else tid
            for tid in self._redis.smembers(f"dependency:{task_id}")
        ]

    def get_waiting_tasks(self) -> List[str]:
        """Return all task IDs currently marked as waiting."""
        waiting = []
        for key in self._redis.scan_iter(match=f"{self.task_prefix}:*"):
            status = self._redis.hget(key, "status")
            if not status:
                continue
            status = status.decode() if isinstance(status, bytes) else status
            if status != "waiting":
                continue
            task_id = self._redis.hget(key, "task_id")
            if not task_id:
                continue
            task_id = task_id.decode() if isinstance(task_id, bytes) else task_id
            waiting.append(task_id)
        return waiting

    def stats(self) -> dict:
        """
        Snapshot of queue depth for monitoring/diagnostics.

        ``dead_letter`` reflects tasks whose hash went missing on pop() —
        see ``pop()`` for how they get there.
        """
        pipe = self._redis.pipeline()
        pipe.zcard(self.queue_key)
        pipe.zcard(self.processing_key)
        pipe.zcard(self.dead_letter_key)
        runnable, processing, dead_letter = pipe.execute()

        oldest = self._redis.zrange(self.processing_key, 0, 0, withscores=True)
        oldest_processing_timestamp = oldest[0][1] if oldest else None
        return {
            "runnable": runnable,
            "processing": processing,
            "dead_letter": dead_letter,
            "oldest_processing_timestamp": oldest_processing_timestamp,
        }

    def processing_stats(self) -> dict:
        """Alias for queue stats with processing age visibility."""
        return self.stats()
