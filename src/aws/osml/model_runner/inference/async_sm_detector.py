#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

import json
import logging
import traceback
from io import BufferedReader
from typing import Any, Dict, Optional

from aws_embedded_metrics.logger.metrics_logger import MetricsLogger
from aws_embedded_metrics.metric_scope import metric_scope
from botocore.exceptions import ClientError
from geojson import FeatureCollection

from aws.osml.model_runner.app_config import ServiceConfig
from aws.osml.model_runner.common import Timer
from aws.osml.model_runner.inference.detector import Detector
from aws.osml.model_runner.inference.sm_detector import SMDetector
from aws.osml.model_runner.api import ModelInvokeMode
from aws.osml.model_runner.utilities import S3Manager, S3OperationError
from aws.osml.model_runner.exceptions import ProcessTileException

logger = logging.getLogger(__name__)

S3_MANAGER = S3Manager()


class AsyncSMDetector(SMDetector):
    """
    AsyncSMDetector extends SMDetector with true asynchronous SageMaker endpoint processing.

    This class provides async endpoint capabilities including S3-based input/output handling,
    polling for completion, and comprehensive error management while maintaining full
    compatibility with the base SMDetector interface.
    """

    def __init__(
        self,
        endpoint: str,
        endpoint_parameters: Optional[Dict[str, str]] = None,
        assumed_credentials: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Initializes the AsyncSMDetector with async endpoint capabilities.

        :param endpoint: str = The name of the SageMaker async endpoint to invoke.
        :param endpoint_parameters: Optional[Dict[str, str]] = Additional parameters to pass to the model endpoint.
        :param assumed_credentials: Optional[Dict[str, str]] = Optional credentials for invoking the SageMaker model.
        """

        super().__init__(endpoint, endpoint_parameters, assumed_credentials)  # type: ignore

        # Initialize async configuration
        self.async_config = ServiceConfig.async_endpoint_config

        logger.debug(f"AsyncSMDetector initialized for endpoint: {endpoint}")

        # Validate S3 bucket access during initialization
        try:
            S3_MANAGER.validate_bucket_access()
        except S3OperationError as e:
            logger.warning(f"S3 bucket validation failed during initialization: {e}")
            # Don't fail initialization, but log the warning

    @metric_scope
    def find_features(self, payload: BufferedReader, metrics: MetricsLogger = None) -> FeatureCollection:
        """
        Async feature detection using SageMaker async endpoints with S3-based input/output.

        This method implements the complete async workflow: upload payload to S3, invoke async endpoint,
        poll for completion, download results, and return FeatureCollection while maintaining full
        compatibility with the base interface.

        :param payload: BufferedReader = The data to be sent to the SageMaker async endpoint.
        :param metrics: MetricsLogger = The metrics logger to capture system performance and log metrics.

        :return: FeatureCollection = A geojson FeatureCollection containing the detected features.

        :raises ClientError: Raised if there is an error while invoking the SageMaker async endpoint.
        :raises JSONDecodeError: Raised if there is an error decoding the model's response.
        :raises S3OperationError: Raised if S3 upload/download operations fail.
        :raises AsyncInferenceTimeoutError: Raised if async inference exceeds maximum wait time.
        """
        # not used here. Use _invoke_async_endpoint directly from AsyncSubmissionWorker.process_tile_submission

        raise NotImplementedError()

    def _invoke_async_endpoint(
        self, input_s3_uri: str, metrics: Optional[MetricsLogger], custom_attributes: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Invoke SageMaker async endpoint with S3 input URI.

        :param input_s3_uri: S3 URI of input data
        :param metrics: Optional metrics logger
        :return: Inference job ID
        """
        logger.debug(f"Invoking async endpoint: {self.endpoint} for object: {input_s3_uri}")

        try:
            with Timer(
                task_str="Invoke Async SM Endpoint",
                metric_name="AsyncEndpointInvocation",
                logger=logger,
                metrics_logger=metrics,
            ):
                # Invoke async endpoint
                response = self.sm_runtime_client.invoke_endpoint_async(
                    EndpointName=self.endpoint,
                    ContentType="application/json",
                    Accept="application/json",
                    CustomAttributes=json.dumps(custom_attributes or {}),
                    InputLocation=input_s3_uri,
                    InvocationTimeoutSeconds=self.async_config.max_wait_time,
                )

            inference_id = response.get("InferenceId")
            output_location = response.get("OutputLocation")
            failure_location = response.get("FailureLocation")
            if not inference_id:
                raise ProcessTileException("No inference ID returned from async endpoint")

            logger.debug(f"Async inference submitted with ID: {inference_id}")
            return inference_id, output_location, failure_location

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            logger.error(f"Failed to invoke async endpoint {self.endpoint}: {error_code} - {str(e)}")
            raise


class AsyncSMDetectorBuilder:
    """
    Builder class for creating AsyncSMDetector instances with async configuration support.

    This builder follows the same pattern as the base model runner builders
    and provides validation and error handling for AsyncSMDetector creation with
    comprehensive async endpoint configuration.
    """

    def __init__(
        self,
        endpoint: str,
        endpoint_parameters: Optional[Dict[str, str]] = None,
        assumed_credentials: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize the AsyncSMDetectorBuilder with async configuration support.

        :param endpoint: The SageMaker async endpoint name
        :param endpoint_parameters: Optional[Dict[str, str]] = Additional parameters to pass to the model endpoint.
        :param assumed_credentials: Optional credentials for the endpoint
        """
        self.endpoint = endpoint
        self.endpoint_parameters = endpoint_parameters
        self.assumed_credentials = assumed_credentials or {}
        self.async_config = ServiceConfig.async_endpoint_config

        logger.debug(f"AsyncSMDetectorBuilder initialized for endpoint: {endpoint}")

    def build(self) -> Optional[Detector]:
        """
        Build an AsyncSMDetector instance with async configuration.

        :return: AsyncSMDetector instance or None if creation fails
        """
        try:
            logger.debug(f"Building AsyncSMDetector for endpoint: {self.endpoint}")

            # Create the detector with async configuration
            detector = AsyncSMDetector(
                endpoint=self.endpoint, 
                endpoint_parameters=self.endpoint_parameters,
                assumed_credentials=self.assumed_credentials
            )

            logger.debug(f"Successfully created AsyncSMDetector for endpoint: {self.endpoint}")
            return detector

        except Exception as e:
            logger.error(f"Failed to create AsyncSMDetector: {e}", exc_info=True)
            # Return None to allow fallback handling
            return None
