
import inspect
from functools import wraps

from metrics.metrics_state import MetricsState
from metrics.metrics_context import MetricsContext


def metric_scope(fn):  # type: ignore

    @wraps(fn)
    def wrapper(*args, **kwargs):  # type: ignore
        metrics_context = create_metrics_context()
        if "metrics" in inspect.signature(fn).parameters:
            kwargs["metrics"] = metrics_context
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            raise e
        finally:
            MetricsState.metrics_queue.put(metrics_context)

    return wrapper


def create_metrics_context():
    return MetricsContext(namespace=MetricsState.metrics_config.namespace)