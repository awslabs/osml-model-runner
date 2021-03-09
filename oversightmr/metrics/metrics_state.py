from queue import Queue

from metrics.metrics_config import MetricsConfig
from metrics.metrics_worker import MetricsWorker


class MetricsState:
    metrics_queue: Queue = Queue()
    metrics_background_thread: MetricsWorker = None
    metrics_config: MetricsConfig = None
