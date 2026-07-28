import backend.handlers
from broccoli.workers.hybrid_worker import HybridWorker

HybridWorker(decode_responses=False).start()
