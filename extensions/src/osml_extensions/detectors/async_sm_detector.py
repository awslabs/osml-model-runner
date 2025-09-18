#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
from io import BufferedReader
from typing import Dict

from aws_embedded_metrics.logger.metrics_logger import MetricsLogger
from aws_embedded_metrics.metric_scope import metric_scope
from aws_embedded_metrics.unit import Unit
from geojson import FeatureCollection

from aws.osml.model_runner.inference.sm_detector import SMDetector

logger = logging.getLogger(__name__)


class AsyncSMDetector(SMDetector):
    """
    AsyncSMDetector extends SMDetector with asynchronous processing capabilities.

    This class maintains full compatibility with the base SMDetector while adding
    enhanced features for improved performance.
    """

    def __init__(self, endpoint: str, assumed_credentials: Dict[str, str]) -> None:
        """
        Initializes the AsyncSMDetector.

        :param endpoint: str = The name of the SageMaker endpoint to invoke.
        :param assumed_credentials: Dict[str, str] = Optional credentials for invoking the SageMaker model.
        """
        super().__init__(endpoint, assumed_credentials)
        logger.info(f"AsyncSMDetector initialized for endpoint: {endpoint}")

    @metric_scope
    def find_features(self, payload: BufferedReader, metrics: MetricsLogger) -> FeatureCollection:
        """
        Enhanced feature detection with metadata addition.

        This method extends the base implementation while maintaining full compatibility.

        :param payload: BufferedReader = The data to be sent to the SageMaker model for feature detection.
        :param metrics: MetricsLogger = The metrics logger to capture system performance and log metrics.

        :return: FeatureCollection = A geojson FeatureCollection containing the detected features.

        :raises ClientError: Raised if there is an error while invoking the SageMaker endpoint.
        :raises JSONDecodeError: Raised if there is an error decoding the model's response.
        """
        logger.debug(f"AsyncSMDetector processing request for endpoint: {self.endpoint}")

        try:
            # Add custom metrics
            if isinstance(metrics, MetricsLogger):
                metrics.put_metric("AsyncSMDetector.Invocations", 1, str(Unit.COUNT.value))

            # Call parent implementation
            features = super().find_features(payload, metrics)

            logger.debug(f"AsyncSMDetector completed processing: " f"{len(features.get('features', []))} features detected")

            return features

        except Exception as e:
            logger.error(f"AsyncSMDetector error: {e}")
            if isinstance(metrics, MetricsLogger):
                metrics.put_metric("AsyncSMDetector.Errors", 1, str(Unit.COUNT.value))
            raise
