# video_scheduler/core/health.py
class HealthCheck:
    def __init__(self, worker):
        self.worker = worker

    def check(self) -> dict:
        return {
            "status": "healthy" if self.worker.running else "unhealthy",
            "worker_id": self.worker.worker_id,
            "redis": self._check_redis(),
            "tasks_processed": self.worker.tasks_processed,
            "active_task": self.worker.current_task_id,
        }

    def _check_redis(self):
        try:
            self.worker.queue.redis.ping()
            return {"status": "connected"}
        except:
            return {"status": "disconnected"}
