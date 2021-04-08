import logging
import queue
from threading import Thread

import boto3

from .metrics_config import MetricsConfig
from .metrics_context import MetricsContext


class MetricsWorker(Thread):

    def __init__(self, metrics_queue: queue.Queue, metrics_config: MetricsConfig):
        super().__init__()
        self.metrics_queue = metrics_queue
        self.metrics_config = metrics_config
        if metrics_config.output_type == "cw":
            self.cw_client = self.cw_client = boto3.client('cloudwatch')

    def run(self) -> None:
        while True:
            try:
                # Blocked get with a timeout of one second to avoid unnecessary tight loop when no metrics are there
                # in the input queue.
                metrics_context: MetricsContext = self.metrics_queue.get(timeout=1)
                if metrics_context is None:
                    # A None object in the input queue can be used by the main worker process
                    # to indicate to cloudwatch worker process to quit gracefully.
                    break
                self.store_metrics(metrics_context)
            except queue.Empty:
                # This is Normal. Continue
                pass

    def store_metrics(self, context: MetricsContext) -> None:

        try:
            dimensions = []
            for dimension_set in context.get_dimensions():
                for name, value in dimension_set.items():
                    dimensions.append({
                        'Name': name,
                        'Value': value
                    })

            metric_data = []
            for metric_name, metric in context.metrics.items():
                data = {
                    'MetricName': metric_name,
                    'Dimensions': dimensions,
                    'Unit': metric.unit,
                    'Values': metric.values
                }
                metric_data.append(data)

            # TODO: Better encapsulate these in a Sink
            if self.metrics_config.output_type == "cw":
                response = self.cw_client.put_metric_data(
                    MetricData=metric_data,
                    Namespace=context.namespace
                )
            elif self.metrics_config.output_type == "stdout":
                logging.info("Writing metrics to {}".format(context.namespace))
                logging.info(metric_data)


        except Exception as e:
            logging.warning("Unable to store metrics", e)

