from queue import Queue

from .metrics_config import MetricsConfig
from .metrics_context import MetricsContext
from .metrics_scope import metric_scope
from .metrics_state import MetricsState
from .metrics_utils import now
from .metrics_worker import MetricsWorker

# Explicitly specify all classes that are going to be exported to avoid linter issues
__all__ = [
    "MetricsConfig",
    "MetricsContext",
    "metric_scope",
    "MetricsState",
    "now",
    "MetricsWorker",
]


def configure_metrics(namespace, output_type):
    MetricsState.metrics_config = MetricsConfig(namespace, output_type)


def start_metrics():
    MetricsState.metrics_queue = Queue()
    MetricsState.metrics_background_thread = MetricsWorker(
        MetricsState.metrics_queue, MetricsState.metrics_config
    )
    MetricsState.metrics_background_thread.start()


def stop_metrics():
    MetricsState.metrics_queue.put(None)
    MetricsState.metrics_background_thread.join()
