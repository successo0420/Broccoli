import backend.handlers
from broccoli.workers.base_worker import BaseWorker
from broccoli.workers.hybrid_worker import HybridWorker

BaseWorker(decode_responses=False).start()
