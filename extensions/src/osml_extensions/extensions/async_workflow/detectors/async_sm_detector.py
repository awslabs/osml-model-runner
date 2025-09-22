#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
import traceback
from io import BufferedReader
from json import JSONDecodeError
from typing import Dict, Optional

from aws_embedded_metrics.logger.metrics_logger import MetricsLogger
from aws_embedded_metrics.unit import Unit
from botocore.exceptions import ClientError
from geojson import FeatureCollection

from aws.osml.model_runner.app_config import MetricLabels
from aws.osml.model_runner.common import Timer
from aws.osml.model_runner.inference.detector import Detector
from aws.osml.model_runner.inference.sm_detector import SMDetector

from ...errors import ExtensionConfigurationError
from ..async_app_config import AsyncEndpointConfig, AsyncServiceConfig
from ..s3 import S3Manager, S3OperationError
from ..api import ExtendedModelInvokeMode
from ..polling import AsyncInferencePoller, AsyncInferenceTimeoutError
from ..utils import CleanupPolicy, ResourceManager

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
        assumed_credentials: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Initializes the AsyncSMDetector with async endpoint capabilities.

        :param endpoint: str = The name of the SageMaker async endpoint to invoke.
        :param assumed_credentials: Optional[Dict[str, str]] = Optional credentials for invoking the SageMaker model.
        """

        super().__init__(endpoint, assumed_credentials)  # type: ignore

        # Initialize async configuration
        self.async_config = AsyncServiceConfig.async_endpoint_config

        # Initialize S3 manager and poller
        self.poller = AsyncInferencePoller(self.sm_client, self.async_config)

        # Initialize resource manager for cleanup
        self.resource_manager = ResourceManager(self.async_config)
        self.resource_manager.start_cleanup_worker()

        logger.debug(f"AsyncSMDetector initialized for endpoint: {endpoint}")

        # Validate S3 bucket access during initialization
        try:
            S3_MANAGER.validate_bucket_access()
        except S3OperationError as e:
            logger.warning(f"S3 bucket validation failed during initialization: {e}")
            # Don't fail initialization, but log the warning

    @property
    def mode(self) -> ExtendedModelInvokeMode:  # type: ignore
        return ExtendedModelInvokeMode.SM_ENDPOINT_ASYNC

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
        logger.debug(f"AsyncSMDetector processing async request for endpoint: {self.endpoint}")

        if isinstance(metrics, MetricsLogger):
            metrics.set_dimensions()
            metrics.put_dimensions(
                {
                    MetricLabels.OPERATION_DIMENSION: MetricLabels.MODEL_INVOCATION_OPERATION,
                    MetricLabels.MODEL_NAME_DIMENSION: self.endpoint,
                }
            )

        inference_id = None
        # job_resource_id = None

        try:
            self.request_count += 1
            if isinstance(metrics, MetricsLogger):
                metrics.put_metric(MetricLabels.INVOCATIONS, 1, str(Unit.COUNT.value))

            with Timer(
                task_str="Async SM Endpoint Processing",
                metric_name="TotalAsyncDuration",
                logger=logger,
                metrics_logger=metrics,
            ):
                # Step 1: Upload payload to S3
                input_key = S3_MANAGER.generate_unique_key("input")
                input_s3_uri = AsyncServiceConfig._upload_to_s3(payload, input_key, metrics)

                # Register S3 input object for managed cleanup
                cleanup_policy = CleanupPolicy(self.async_config.cleanup_policy)
                self.resource_manager.register_s3_object(input_s3_uri, cleanup_policy)

                # Step 2: Invoke async endpoint
                inference_id = self._invoke_async_endpoint(input_s3_uri, metrics)

                # Register inference job for comprehensive resource management
                job_data = {
                    "input_s3_uri": input_s3_uri,
                    "temp_files": [],  # Add any temp files created during processing
                }
                _ = self.resource_manager.register_inference_job(inference_id, job_data, cleanup_policy)  # job_resource_id

                # Step 3: Poll for completion
                completed_output_uri = self._poll_for_completion(inference_id, metrics)

                # Register output S3 object for cleanup
                self.resource_manager.register_s3_object(completed_output_uri, cleanup_policy)

                # Step 4: Download and parse results
                feature_collection = AsyncServiceConfig._download_from_s3(completed_output_uri, metrics)

                if isinstance(metrics, MetricsLogger):
                    metrics.put_metric("AsyncInferenceSuccess", 1, str(Unit.COUNT.value))

                logger.info(f"AsyncSMDetector completed successfully for endpoint: {self.endpoint}")
                return feature_collection

        except (ClientError, S3OperationError, AsyncInferenceTimeoutError) as e:
            if isinstance(metrics, MetricsLogger):
                metrics.put_metric(MetricLabels.ERRORS, 1, str(Unit.COUNT.value))

            logger.error(f"AsyncSMDetector error for endpoint {self.endpoint}: {str(e)}")
            logger.exception(e)

            # Clean up resources for failed job
            if inference_id:
                try:
                    self.resource_manager.cleanup_failed_job_resources(inference_id)
                except Exception as cleanup_error:
                    logger.warning(f"Failed to cleanup resources for failed job {inference_id}: {cleanup_error}")

            raise

        except JSONDecodeError as de:
            if isinstance(metrics, MetricsLogger):
                metrics.put_metric(MetricLabels.ERRORS, 1, str(Unit.COUNT.value))
            logger.error("Unable to decode async response from model.")
            logger.exception(de)

            # Clean up resources for failed job
            if inference_id:
                try:
                    self.resource_manager.cleanup_failed_job_resources(inference_id)
                except Exception as cleanup_error:
                    logger.warning(f"Failed to cleanup resources for failed job {inference_id}: {cleanup_error}")

            raise de

        except Exception as e:
            if isinstance(metrics, MetricsLogger):
                metrics.put_metric(MetricLabels.ERRORS, 1, str(Unit.COUNT.value))

            logger.error(f"Unexpected error in AsyncSMDetector: {str(e)}")
            logger.exception(e)

            # Clean up resources for failed job
            if inference_id:
                try:
                    self.resource_manager.cleanup_failed_job_resources(inference_id)
                except Exception as cleanup_error:
                    logger.warning(f"Failed to cleanup resources for failed job {inference_id}: {cleanup_error}")

            raise

    def _invoke_async_endpoint(self, input_s3_uri: str, metrics: Optional[MetricsLogger]) -> str:
        """
        Invoke SageMaker async endpoint with S3 input URI.

        :param input_s3_uri: S3 URI of input data
        :param metrics: Optional metrics logger
        :return: Inference job ID
        """
        logger.debug(f"Invoking async endpoint: {self.endpoint}")

        try:
            with Timer(
                task_str="Invoke Async SM Endpoint",
                metric_name="AsyncEndpointInvocation",
                logger=logger,
                metrics_logger=metrics,
            ):
                # Invoke async endpoint
                response = self.sm_client.invoke_endpoint_async(
                    EndpointName=self.endpoint,
                    InputLocation=input_s3_uri,
                    ContentType="application/json",
                    Accept="application/json",
                    InvocationTimeoutInSeconds=self.async_config.max_wait_time,
                )

            inference_id = response.get("InferenceId")
            if not inference_id:
                raise ExtensionConfigurationError("No inference ID returned from async endpoint")

            logger.debug(f"Async inference submitted with ID: {inference_id}")
            return inference_id

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code")
            logger.error(f"Failed to invoke async endpoint {self.endpoint}: {error_code} - {str(e)}")
            raise

    def _poll_for_completion(self, inference_id: str, metrics: Optional[MetricsLogger]) -> str:
        """
        Poll for async inference completion.

        :param inference_id: The inference job ID to poll
        :param metrics: Optional metrics logger
        :return: Output S3 URI when job completes
        """
        logger.debug(f"Polling for completion of inference job: {inference_id}")
        return self.poller.poll_until_complete(inference_id, metrics)

    def cleanup_resources(self, force: bool = False) -> int:
        """
        Clean up all managed resources.

        :param force: Force cleanup even if policy is disabled
        :return: Number of resources successfully cleaned up
        """
        logger.info("Cleaning up AsyncSMDetector resources")
        return self.resource_manager.cleanup_all_resources(force=force)

    def get_resource_stats(self) -> Dict:
        """
        Get statistics about managed resources.

        :return: Dictionary of resource statistics
        """
        return self.resource_manager.get_resource_stats()

    def __del__(self):
        """Destructor to ensure resource cleanup."""
        try:
            if hasattr(self, "resource_manager"):
                # Clean up all resources and stop the cleanup worker
                self.resource_manager.cleanup_all_resources(force=True)
                self.resource_manager.stop_cleanup_worker(timeout=5.0)
        except Exception as e:
            # Don't raise exceptions in destructor
            logger.warning(f"Error during AsyncSMDetector cleanup: {e}")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit with cleanup."""
        self.cleanup_resources(force=True)
        self.resource_manager.stop_cleanup_worker()


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
        assumed_credentials: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize the AsyncSMDetectorBuilder with async configuration support.

        :param endpoint: The SageMaker async endpoint name
        :param assumed_credentials: Optional credentials for the endpoint
        """
        self.endpoint = endpoint
        self.assumed_credentials = assumed_credentials or {}
        self.async_config = AsyncServiceConfig.async_endpoint_config

        logger.debug(f"AsyncSMDetectorBuilder initialized for endpoint: {endpoint}")

    def _validate_parameters(self) -> None:
        """
        Validate the builder parameters including async configuration.

        :raises ExtensionConfigurationError: If parameters are invalid
        """
        if not self.endpoint:
            raise ExtensionConfigurationError("Endpoint name is required for AsyncSMDetector")

        if not isinstance(self.endpoint, str):
            raise ExtensionConfigurationError("Endpoint name must be a string")

        if self.assumed_credentials is not None and not isinstance(self.assumed_credentials, dict):
            raise ExtensionConfigurationError("Assumed credentials must be a dictionary")

        # Validate async configuration if provided
        if self.async_config is not None and not isinstance(self.async_config, AsyncEndpointConfig):
            raise ExtensionConfigurationError("async_config must be an AsyncEndpointConfig instance")

        logger.debug("AsyncSMDetectorBuilder parameters validated successfully")

    def build(self) -> Optional[Detector]:
        """
        Build an AsyncSMDetector instance with async configuration.

        :return: AsyncSMDetector instance or None if creation fails
        :raises ExtensionConfigurationError: If parameters are invalid
        """
        try:
            logger.debug(f"Building AsyncSMDetector for endpoint: {self.endpoint}")

            # Validate parameters
            self._validate_parameters()

            # Create the detector with async configuration
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
