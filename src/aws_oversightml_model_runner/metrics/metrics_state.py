from queue import Queue

from .metrics_config import MetricsConfig
from .metrics_worker import MetricsWorker


class MetricsState:
    metrics_queue: Queue = Queue()
    metrics_background_thread: MetricsWorker = None
    metrics_config: MetricsConfig = None
