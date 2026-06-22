# test_comprehensive.py
import hashlib
import os
import subprocess
import time
import uuid
from pathlib import Path

from broccoli.core.redis_controller import RedisController
from broccoli.core.task import Task
from broccoli.core.task_chain import ChainWorkerMixin, TaskChain
from broccoli.core.task_queue import TaskQueue
from broccoli.core.task_registry import TaskRegistry
from broccoli.workers.async_worker import AsyncWorker
from broccoli.workers.chain_worker import ChainWorker
from broccoli.workers.hybrid_worker import HybridWorker
from broccoli.workers.threaded_worker import ThreadedWorker
from broccoli.workers.worker_pool import WorkerPool

# Initialize
registry = TaskRegistry()
queue = TaskQueue()
chain = TaskChain()

# ============ TASK DEFINITIONS ============


@registry.register("check_if_file_exists")
def check_if_file_exists(payload):
    time.sleep(0.1)  # Simulate I/O
    exists = Path(payload["file_path"]).exists()
    return {"exists": exists, "path": payload["file_path"]}


@registry.register("get_file_info")
def get_file_info(payload):
    time.sleep(0.2)  # Simulate I/O
    file_path = Path(payload["file_path"])
    stat = os.stat(file_path)
    return {
        "size": stat.st_size,
        "modified": stat.st_mtime,
        "created": stat.st_ctime,
        "is_file": os.path.isfile(file_path),
        "is_dir": os.path.isdir(file_path),
    }


@registry.register("calculate_file_hash")
def calculate_file_hash(payload):
    # CPU intensive - hash large file
    hash_func = hashlib.new(payload.get("algorithm", "sha256"))
    file_path = payload["file_path"]

    # Simulate large file processing
    size = os.path.getsize(file_path)
    chunks = 0
    with open(file_path, "rb") as f:
        while chunk := f.read(8192):
            hash_func.update(chunk)
            chunks += 1
            if chunks % 100 == 0:
                time.sleep(0.001)  # Simulate CPU work

    print(f"Hashed {file_path} in {chunks} chunks")

    return {
        "hash": hash_func.hexdigest(),
        "algorithm": payload.get("algorithm", "sha256"),
        "chunks": chunks,
    }


@registry.register("copy_large_file")
def copy_large_file(payload):
    # CPU/I/O intensive - copy file
    source = payload["source_path"]
    dest = payload["destination_path"]
    chunk_size = 1024 * 1024  # 1 MB

    total_bytes = 0
    with open(source, "rb") as src:
        with open(dest, "wb") as dst:
            while True:
                chunk = src.read(chunk_size)
                if not chunk:
                    break
                dst.write(chunk)
                total_bytes += len(chunk)
                time.sleep(0.001)  # Simulate I/O wait

    return {"bytes_copied": total_bytes, "destination": dest, "success": True}


@registry.register("delete_file")
def delete_file(payload):
    time.sleep(0.1)  # Simulate I/O
    file_path = Path(payload["file_path"])
    if file_path.exists():
        file_path.unlink()
        return {"deleted": True, "path": str(file_path)}
    return {"deleted": False, "path": str(file_path)}


@registry.register("find_duplicate_files")
def find_duplicate_files(payload):
    # CPU intensive - hash multiple files
    file_paths = [Path(f) for f in payload["file_paths"]]
    hashes = {}
    duplicates = []

    for file_path in file_paths:
        if file_path.exists():
            time.sleep(0.05)  # Simulate processing time
            with open(file_path, "rb") as f:
                file_hash = hashlib.md5(f.read()).hexdigest()
            if file_hash in hashes:
                duplicates.append(
                    {
                        "original": str(hashes[file_hash]),
                        "duplicate": str(file_path),
                        "hash": file_hash,
                    }
                )
            else:
                hashes[file_hash] = file_path

    return {
        "total_files": len(file_paths),
        "duplicates_found": len(duplicates),
        "duplicates": duplicates,
    }


@registry.register("transcode_video_ffmpeg")
def transcode_video_ffmpeg(payload):
    """
    Transcode video using FFmpeg.

    Payload:
        input_path: str - Path to input video
        output_path: str - Path for output video
        video_codec: str - Video codec (default: libx264)
        audio_codec: str - Audio codec (default: aac)
        video_bitrate: str - Video bitrate (default: 1M)
        resolution: str - Output resolution (default: 1280x720)
        format: str - Output format (default: mp4)
    """
    input_path = payload["input_path"]
    output_path = payload["output_path"]

    # Build FFmpeg command
    cmd = [
        "ffmpeg",
        "-i",
        input_path,
        "-c:v",
        payload.get("video_codec", "libx264"),
        "-c:a",
        payload.get("audio_codec", "aac"),
        "-b:v",
        payload.get("video_bitrate", "1M"),
        "-s",
        payload.get("resolution", "1280x720"),
        "-f",
        payload.get("format", "mp4"),
        "-y",  # Overwrite output file
        output_path,
    ]

    # Run FFmpeg
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        raise RuntimeError(f"FFmpeg failed: {result.stderr}")

    # Get output file info
    stat = os.stat(output_path)

    return {
        "input": input_path,
        "output": output_path,
        "size_bytes": stat.st_size,
        "size_mb": stat.st_size / (1024 * 1024),
        "codec": payload.get("video_codec", "libx264"),
        "resolution": payload.get("resolution", "1280x720"),
        "success": True,
    }


@registry.register("generate_report")
def generate_report(payload):
    # CPU intensive - report generation
    data = payload["data"]
    report_id = str(uuid.uuid4())

    # Simulate complex calculations
    total = sum(data)
    average = total / len(data) if data else 0
    time.sleep(0.5)

    return {
        "report_id": report_id,
        "total": total,
        "average": average,
        "count": len(data),
    }


@registry.register("send_notification")
def send_notification(payload):
    # I/O intensive - API call simulation
    time.sleep(0.3)
    return {
        "sent": True,
        "to": payload["email"],
        "message": payload["message"],
        "notification_id": str(uuid.uuid4()),
    }


# ============ DEPENDENCY TASKS ============


@registry.register("validate_file")
def validate_file(payload):
    """Validate file exists and is readable."""
    path = Path(payload["file_path"])
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    if not os.access(path, os.R_OK):
        raise PermissionError(f"Cannot read file: {path}")
    return {"valid": True, "path": str(path)}


@registry.register("compress_file")
def compress_file(payload):
    """Compress file (depends on validate_file)."""
    file_path = payload["file_path"]
    compressed_path = f"{file_path}.gz"

    # Simulate compression
    time.sleep(0.5)
    Path(compressed_path).touch()

    return {"original": file_path, "compressed": compressed_path, "ratio": 0.7}


@registry.register("upload_to_cloud")
def upload_to_cloud(payload):
    """Upload file to cloud (depends on compress_file)."""
    file_path = payload["file_path"]

    # Simulate upload
    time.sleep(0.5)

    return {
        "url": f"https://cloud.example.com/{Path(file_path).name}",
        "uploaded": True,
    }


# ============ TEST SETUP ============


def create_test_files():
    """Create test files for operations."""
    files = []
    for i in range(5):
        file_path = f"test_file_{i}.txt"
        with open(file_path, "w") as f:
            f.write("x" * (1024 * 100 * (i + 1)))  # 100KB to 500KB
        files.append(str(file_path))

    # Create duplicate files
    for i in range(2):
        file_path = f"duplicate_{i}.txt"
        with open(file_path, "w") as f:
            f.write("DUPLICATE CONTENT" * 1000)
        files.append(str(file_path))

    # Video file (simulated)
    video_path = "example_video.mp4"
    with open(video_path, "w") as f:
        f.write("DUMMY VIDEO CONTENT" * 5000)
    files.append(str(video_path))

    return files


# ============ TEST SCENARIOS ============


def test_basic_worker():
    """Test ThreadedWorker with basic tasks."""
    print("\n=== TEST 1: Basic ThreadedWorker ===")

    files = create_test_files()
    worker = ThreadedWorker(max_workers=3)

    # Push tasks
    tasks = []
    for file_path in files[:3]:
        task = Task(task_type="get_file_info", payload={"file_path": file_path})
        queue.push(task)
        tasks.append(task)

    print(f"Pushed {len(tasks)} tasks")
    worker.start()  # Run for 10 seconds then stop


def test_chain_worker():
    """Test task chaining with ChainWorkerMixin."""
    print("\n=== TEST 2: Task Chaining ===")

    files = create_test_files()

    worker = ChainWorker(max_workers=2)

    # Create chain: validate → compress → upload
    chain_id = chain.chain(
        [
            {"task_type": "validate_file", "payload": {"file_path": files[0]}},
            {"task_type": "compress_file", "payload": {"file_path": files[0]}},
            {"task_type": "upload_to_cloud", "payload": {"file_path": files[0]}},
        ]
    )

    print(f"Chain started: {chain_id}")
    worker.start()

    status = chain.get_chain_status(chain_id)
    print(f"Chain status: {status}")


def test_dependency_worker():
    """Test tasks with dependencies."""
    print("\n=== TEST 3: Task Dependencies ===")

    test_dir, files = create_test_files()
    worker = ThreadedWorker(max_workers=2)

    # Task 1: Check if file exists
    task1 = Task(task_type="check_if_file_exists", payload={"file_path": files[0]})
    queue.push(task1)

    # Task 2: Get file info (depends on task1 completing)
    task2 = Task(
        task_type="get_file_info",
        payload={"file_path": files[0]},
        depends_on=task1.task_id,
    )
    queue.push(task2)

    # Task 3: Calculate hash (depends on task2 completing)
    task3 = Task(
        task_type="calculate_file_hash",
        payload={"file_path": files[0], "algorithm": "sha256"},
        depends_on=task2.task_id,
    )
    queue.push(task3)

    print(f"Task chain: {task1.task_id} → {task2.task_id} → {task3.task_id}")
    worker.start()

    # Check results
    result_task3 = queue.get_task(task3.task_id)
    if result_task3 and result_task3.status == "completed":
        print(f"Final result: {result_task3.result}")


def test_async_worker_io_intensive():
    """Test AsyncWorker with I/O intensive tasks."""
    print("\n=== TEST 4: AsyncWorker (I/O Intensive) ===")

    test_dir, files = create_test_files()
    worker = AsyncWorker(10)

    # Push many I/O tasks
    tasks = []
    for i, file_path in enumerate(files):
        task = Task(
            task_type="send_notification",
            payload={
                "email": f"user{i}@example.com",
                "message": f"Processing file: {file_path}",
            },
        )
        queue.push(task)
        tasks.append(task)

    print(f"Pushed {len(tasks)} I/O tasks")
    worker.start()


def test_hybrid_worker_mixed():
    """Test HybridWorker with mixed CPU and I/O tasks."""
    print("\n=== TEST 5: HybridWorker (Mixed Workload) ===")

    test_dir, files = create_test_files()
    worker = HybridWorker(
        thread_workers=3,  # For CPU tasks
        async_tasks=5,  # For I/O tasks
    )

    # CPU tasks (hash, transcode)
    cpu_tasks = []
    for file_path in files[:3]:
        task = Task(
            task_type="calculate_file_hash",
            payload={"file_path": file_path, "algorithm": "sha256"},
        )
        queue.push(task)
        cpu_tasks.append(task)

    # I/O tasks (notifications)
    io_tasks = []
    for i in range(10):
        task = Task(
            task_type="send_notification",
            payload={
                "email": f"user{i}@example.com",
                "message": f"Processing batch {i}",
            },
        )
        queue.push(task)
        io_tasks.append(task)

    print(f"Pushed {len(cpu_tasks)} CPU tasks and {len(io_tasks)} I/O tasks")
    worker.start()


def test_worker_pool():
    """Test WorkerPool with multiple workers."""
    print("\n=== TEST 6: WorkerPool ===")

    test_dir, files = create_test_files()

    pool = WorkerPool(
        worker_type=ThreadedWorker, num_workers=3, redis_url="redis://localhost:6379"
    )

    # Push many tasks
    for i in range(15):
        task = Task(task_type="generate_report", payload={"data": list(range(100))})
        queue.push(task)

    print("Pushed 15 report generation tasks")
    print("Starting pool of 3 workers...")
    pool.start()


def test_complex_chain_with_dependencies():
    """Test complex chain with branching and dependencies."""
    print("\n=== TEST 7: Complex Chain with Dependencies ===")

    files = create_test_files()

    worker = ChainWorker()

    # Chain: duplicate check → hash → copy → notify
    chain_id = chain.chain(
        [
            {"task_type": "find_duplicate_files", "payload": {"file_paths": files}},
            {
                "task_type": "calculate_file_hash",
                "payload": {"file_path": files[0], "algorithm": "sha256"},
            },
            {
                "task_type": "copy_large_file",
                "payload": {
                    "source_path": files[0],
                    "destination_path": "copy_output.txt",
                },
            },
            {
                "task_type": "send_notification",
                "payload": {
                    "email": "admin@example.com",
                    "message": "Complex processing completed",
                },
            },
        ]
    )

    print(f"Complex chain started: {chain_id}")
    worker.start()

    status = chain.get_chain_status(chain_id)
    print(f"Chain status: {status}")


# ============ RUN ALL TESTS ============


def run_all_tests():
    """Run all test scenarios."""
    print("\n" + "=" * 60)
    print("BROCCOLI COMPREHENSIVE TEST SUITE")
    print("=" * 60)

    # Make sure Redis is running
    try:
        queue.redis.ping()
        print("✅ Redis connected")
    except:
        print("❌ Redis not running! Start Redis first.")
        return

    test_functions = [
        # test_basic_worker,
        # test_chain_worker,
        # test_dependency_worker,
        # test_async_worker_io_intensive,
        # test_hybrid_worker_mixed,
        # test_worker_pool,
        test_complex_chain_with_dependencies,
    ]

    for test_func in test_functions:
        try:
            test_func()
        except Exception as e:
            print(f"❌ Test failed: {e}")
        time.sleep(2)

    print("\n" + "=" * 60)
    print("ALL TESTS COMPLETED")
    print("=" * 60)


if __name__ == "__main__":
    WorkerPool(worker_type=ThreadedWorker, num_workers=4).start()
    # run_all_tests()
