# video_scheduler/config.py
import os
from dataclasses import dataclass


@dataclass
class Config:
    redis_url: str = os.getenv("REDIS_URL", "redis://localhost:6379")
    queue_key: str = os.getenv("QUEUE_KEY", "tasks:queue")
    default_max_retries: int = int(os.getenv("DEFAULT_MAX_RETRIES", 3))
    task_timeout: int = int(os.getenv("TASK_TIMEOUT", 3600))
    worker_concurrency: int = int(os.getenv("WORKER_CONCURRENCY", 1))

    @classmethod
    def from_env(cls):
        return cls()


config = Config.from_env()
