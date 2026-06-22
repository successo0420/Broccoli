# video_scheduler/cli.py
import argparse
import sys

from broccoli.workers.async_worker import AsyncWorker
from broccoli.workers.base_worker import BaseWorker
from broccoli.workers.hybrid_worker import HybridWorker
from broccoli.workers.threaded_worker import ThreadedWorker
from broccoli.workers.worker_pool import WorkerPool


def main():
    parser = argparse.ArgumentParser(description="Video Scheduler Task Worker")

    # Worker type
    parser.add_argument(
        "--worker-type",
        choices=["base", "threaded", "async", "hybrid"],
        default="threaded",
        help="Worker type (default: threaded)",
    )

    # Common arguments
    parser.add_argument("--redis-url", default="redis://localhost:6379")
    parser.add_argument("--worker-id", default=None)

    # Pool arguments
    parser.add_argument(
        "--pool", action="store_true", help="Run multiple workers in a pool"
    )
    parser.add_argument(
        "--num-workers",
        type=int,
        default=4,
        help="Number of workers in pool (default: 4)",
    )

    # Worker-specific arguments
    parser.add_argument(
        "--concurrency",
        type=int,
        default=4,
        help="Thread count for threaded/hybrid or async tasks for async/hybrid",
    )
    parser.add_argument(
        "--async-tasks",
        type=int,
        default=10,
        help="Async task concurrency for hybrid worker (default: 10)",
    )
    parser.add_argument(
        "--thread-workers",
        type=int,
        default=4,
        help="Thread pool size for hybrid worker (default: 4)",
    )

    args = parser.parse_args()

    try:
        if args.pool:
            # Map worker type to class
            worker_classes = {
                "base": BaseWorker,
                "threaded": ThreadedWorker,
                "async": AsyncWorker,
                "hybrid": HybridWorker,
            }
            worker_class = worker_classes[args.worker_type]

            pool = WorkerPool(
                worker_type=worker_class,
                num_workers=args.num_workers,
                redis_url=args.redis_url,
            )
            pool.start()
        else:
            # Single worker
            worker = create_worker(args)
            worker.start()

    except KeyboardInterrupt:
        print("\nShutting down...")
        sys.exit(0)


def create_worker(args):
    """Create a single worker based on type."""
    if args.worker_type == "base":
        return BaseWorker(redis_url=args.redis_url, worker_id=args.worker_id)

    elif args.worker_type == "threaded":
        return ThreadedWorker(
            redis_url=args.redis_url,
            worker_id=args.worker_id,
            max_workers=args.concurrency,
        )

    elif args.worker_type == "async":
        return AsyncWorker(
            redis_url=args.redis_url,
            worker_id=args.worker_id,
            max_concurrent=args.concurrency,
        )

    elif args.worker_type == "hybrid":
        return HybridWorker(
            redis_url=args.redis_url,
            worker_id=args.worker_id,
            thread_workers=args.thread_workers,
            async_tasks=args.async_tasks,
        )


if __name__ == "__main__":
    main()
