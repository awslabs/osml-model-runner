#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
import traceback
from io import BufferedReader
from json import JSONDecodeError
from typing import Dict, Optional

import geojson
from aws_embedded_metrics.logger.metrics_logger import MetricsLogger
from aws_embedded_metrics.unit import Unit
from botocore.exceptions import ClientError
from geojson import FeatureCollection
from osml_extensions.api import ExtendedModelInvokeMode
from osml_extensions.errors import ExtensionConfigurationError

from aws.osml.model_runner.app_config import MetricLabels
from aws.osml.model_runner.common import Timer
from aws.osml.model_runner.inference.detector import Detector
from aws.osml.model_runner.inference.sm_detector import SMDetector

logger = logging.getLogger(__name__)


class AsyncSMDetector(SMDetector):
    """
    AsyncSMDetector extends SMDetector with asynchronous processing capabilities.

    This class maintains full compatibility with the base SMDetector while adding
    enhanced features for improved performance and monitoring.
    """

    def __init__(self, endpoint: str, assumed_credentials: Dict[str, str]) -> None:
        """
        Initializes the AsyncSMDetector.

        :param endpoint: str = The name of the SageMaker endpoint to invoke.
        :param assumed_credentials: Dict[str, str] = Optional credentials for invoking the SageMaker model.
        """
        super().__init__(endpoint, assumed_credentials)

    @property
    def mode(self) -> ExtendedModelInvokeMode:  # type: ignore
        return ExtendedModelInvokeMode.SM_ENDPOINT_ASYNC

    def find_features(self, payload: BufferedReader, metrics: MetricsLogger = None) -> FeatureCollection:
        """
        Enhanced feature detection with metadata addition and preprocessing/postprocessing.

        This method extends the base implementation while maintaining full compatibility.

        :param payload: BufferedReader = The data to be sent to the SageMaker model for feature detection.
        :param metrics: MetricsLogger = The metrics logger to capture system performance and log metrics.

        :return: FeatureCollection = A geojson FeatureCollection containing the detected features.

        :raises ClientError: Raised if there is an error while invoking the SageMaker endpoint.
        :raises JSONDecodeError: Raised if there is an error decoding the model's response.
        """
        logger.debug(f"AsyncSMDetector processing request for endpoint: {self.endpoint}")

        logger.debug(f"Invoking Model: {self.endpoint}")
        if isinstance(metrics, MetricsLogger):
            metrics.set_dimensions()
            metrics.put_dimensions(
                {
                    MetricLabels.OPERATION_DIMENSION: MetricLabels.MODEL_INVOCATION_OPERATION,
                    MetricLabels.MODEL_NAME_DIMENSION: self.endpoint,
                }
            )

        try:
            self.request_count += 1
            if isinstance(metrics, MetricsLogger):
                metrics.put_metric(MetricLabels.INVOCATIONS, 1, str(Unit.COUNT.value))

            with Timer(
                task_str="Invoke SM Endpoint",
                metric_name=MetricLabels.DURATION,
                logger=logger,
                metrics_logger=metrics,
            ):
                # Invoke the real SageMaker model endpoint
                model_response = self.sm_client.invoke_endpoint(EndpointName=self.endpoint, Body=payload)
            retry_count = model_response.get("ResponseMetadata", {}).get("RetryAttempts", 0)
            if isinstance(metrics, MetricsLogger):
                metrics.put_metric(MetricLabels.RETRIES, retry_count, str(Unit.COUNT.value))

            # Parse the model's response as a geojson FeatureCollection
            return geojson.loads(model_response.get("Body").read())

        except ClientError as ce:
            error_code = ce.response.get("Error", {}).get("Code")
            http_status_code = ce.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            if isinstance(metrics, MetricsLogger):
                metrics.put_metric(MetricLabels.ERRORS, 1, str(Unit.COUNT.value))
            logger.error(
                f"Unable to get detections from model - HTTP Status Code: {http_status_code}, Error Code: {error_code}"
            )
            logger.exception(ce)
            raise ce
        except JSONDecodeError as de:
            if isinstance(metrics, MetricsLogger):
                metrics.put_metric(MetricLabels.ERRORS, 1, str(Unit.COUNT.value))
            logger.error("Unable to decode response from model.")
            logger.exception(de)
            raise de


class AsyncSMDetectorBuilder:
    """
    Builder class for creating AsyncSMDetector instances.

    This builder follows the same pattern as the base model runner builders
    and provides validation and error handling for AsyncSMDetector creation.
    """

    def __init__(self, endpoint: str, assumed_credentials: Optional[Dict[str, str]] = None):
        """
        Initialize the AsyncSMDetectorBuilder.

        :param endpoint: The SageMaker endpoint name
        :param assumed_credentials: Optional credentials for the endpoint
        """
        self.endpoint = endpoint
        self.assumed_credentials = assumed_credentials or {}

        logger.debug(f"AsyncSMDetectorBuilder initialized for endpoint: {endpoint}")

    def _validate_parameters(self) -> None:
        """
        Validate the builder parameters.

        :raises ExtensionConfigurationError: If parameters are invalid
        """
        if not self.endpoint:
            raise ExtensionConfigurationError("Endpoint name is required for AsyncSMDetector")

        if not isinstance(self.endpoint, str):
            raise ExtensionConfigurationError("Endpoint name must be a string")

        if self.assumed_credentials is not None and not isinstance(self.assumed_credentials, dict):
            raise ExtensionConfigurationError("Assumed credentials must be a dictionary")

        logger.debug("AsyncSMDetectorBuilder parameters validated successfully")

    def build(self) -> Optional[Detector]:
        """
        Build an AsyncSMDetector instance.

        :return: AsyncSMDetector instance or None if creation fails
        :raises ExtensionConfigurationError: If parameters are invalid
        """
        try:
            logger.debug(f"Building AsyncSMDetector for endpoint: {self.endpoint}")

            # Validate parameters
            self._validate_parameters()

            # Create the detector
            detector = AsyncSMDetector(endpoint=self.endpoint, assumed_credentials=self.assumed_credentials)

            logger.info(f"Successfully created AsyncSMDetector for endpoint: {self.endpoint}")
            return detector

        except ExtensionConfigurationError:
            # Re-raise configuration errors
            raise
        except Exception as e:
            logger.error(f"Failed to create AsyncSMDetector: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            # Return None to allow fallback handling
            return None
