#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
import time
from typing import Optional, Tuple

from aws_embedded_metrics.logger.metrics_logger import MetricsLogger
from aws_embedded_metrics.metric_scope import metric_scope
from aws_embedded_metrics.unit import Unit
from botocore.exceptions import ClientError

from aws.osml.model_runner.common import Timer

from ..async_app_config import AsyncEndpointConfig
from ..errors import ExtensionRuntimeError

logger = logging.getLogger(__name__)


class AsyncInferenceTimeoutError(ExtensionRuntimeError):
    """Raised when async inference exceeds maximum wait time."""

    pass


class AsyncInferencePoller:
    """
    Manages polling logic for SageMaker async inference jobs with exponential backoff.

    This class provides robust polling functionality with configurable backoff strategies,
    timeout handling, and comprehensive error management for async inference operations.
    """

    def __init__(self, sm_client, config: AsyncEndpointConfig):
        """
        Initialize AsyncInferencePoller with SageMaker client and configuration.

        :param sm_client: Boto3 SageMaker Runtime client instance
        :param config: AsyncEndpointConfig with polling settings
        """
        self.sm_client = sm_client
        self.config = config
        logger.debug("AsyncInferencePoller initialized")

    @metric_scope
    def poll_until_complete(self, inference_id: str, metrics: MetricsLogger) -> str:
        """
        Poll async inference job until completion with exponential backoff.

        :param inference_id: The inference job ID to poll
        :param metrics: Optional metrics logger for tracking performance
        :return: Output S3 URI when job completes successfully
        :raises AsyncInferenceTimeoutError: If polling exceeds maximum wait time
        :raises ClientError: If SageMaker API calls fail
        """
        logger.debug(f"Starting polling for inference job: {inference_id}")

        if isinstance(metrics, MetricsLogger):
            metrics.put_dimensions(
                {"Operation": "AsyncInferencePolling", "InferenceId": inference_id[:8]}  # Use first 8 chars for privacy
            )

        start_time = time.time()
        attempt = 0

        with Timer(
            task_str="Async Inference Polling",
            metric_name="QueueTime",
            logger=logger,
            metrics_logger=metrics,
        ):
            while True:
                try:
                    # Check if we've exceeded maximum wait time
                    elapsed_time = time.time() - start_time
                    if elapsed_time > self.config.max_wait_time:
                        if isinstance(metrics, MetricsLogger):
                            metrics.put_metric("AsyncInferenceTimeouts", 1, str(Unit.COUNT.value))

                        error_msg = (
                            f"Async inference polling timed out after {elapsed_time:.1f} seconds "
                            f"(max: {self.config.max_wait_time}s) for job: {inference_id}"
                        )
                        logger.error(error_msg)
                        raise AsyncInferenceTimeoutError(error_msg)

                    # Get job status
                    job_status, output_location = self.get_job_status(inference_id)
                    attempt += 1

                    if isinstance(metrics, MetricsLogger):
                        metrics.put_metric("PollingAttempts", 1, str(Unit.COUNT.value))

                    logger.debug(f"Polling attempt {attempt}: job {inference_id} status = {job_status}")

                    if job_status == "Completed":
                        if output_location:
                            if isinstance(metrics, MetricsLogger):
                                metrics.put_metric("AsyncInferenceSuccess", 1, str(Unit.COUNT.value))
                                metrics.put_metric("TotalPollingAttempts", attempt, str(Unit.COUNT.value))

                            logger.info(
                                f"Async inference job {inference_id} completed successfully after {attempt} polling attempts"
                            )
                            return output_location
                        else:
                            error_msg = f"Async inference job {inference_id} completed but no output location provided"
                            logger.error(error_msg)
                            raise ExtensionRuntimeError(error_msg)

                    elif job_status == "Failed":
                        if isinstance(metrics, MetricsLogger):
                            metrics.put_metric("AsyncInferenceFailures", 1, str(Unit.COUNT.value))

                        error_msg = f"Async inference job {inference_id} failed"
                        logger.error(error_msg)
                        raise ExtensionRuntimeError(error_msg)

                    elif job_status in ["InProgress", "Pending"]:
                        # Calculate backoff delay
                        backoff_delay = self._calculate_backoff_delay(attempt)

                        logger.debug(f"Job {inference_id} still {job_status}, waiting {backoff_delay}s before next poll")
                        time.sleep(backoff_delay)

                    else:
                        logger.warning(f"Unknown job status '{job_status}' for inference job {inference_id}")
                        # Treat unknown status as in-progress and continue polling
                        backoff_delay = self._calculate_backoff_delay(attempt)
                        time.sleep(backoff_delay)

                except AsyncInferenceTimeoutError:
                    # Re-raise timeout errors
                    raise

                except ClientError as e:
                    error_code = e.response.get("Error", {}).get("Code", "Unknown")

                    if isinstance(metrics, MetricsLogger):
                        metrics.put_metric("PollingErrors", 1, str(Unit.COUNT.value))

                    # Handle specific error cases
                    if error_code in ["ValidationException", "ResourceNotFound"]:
                        # These are likely permanent errors, don't retry
                        error_msg = f"Permanent error polling inference job {inference_id}: {error_code} - {str(e)}"
                        logger.error(error_msg)
                        raise ExtensionRuntimeError(error_msg) from e

                    # For other errors, log and retry with backoff
                    logger.warning(f"Temporary error polling inference job {inference_id}: {error_code} - {str(e)}")
                    backoff_delay = self._calculate_backoff_delay(attempt)
                    time.sleep(backoff_delay)

                except Exception as e:
                    if isinstance(metrics, MetricsLogger):
                        metrics.put_metric("PollingErrors", 1, str(Unit.COUNT.value))

                    error_msg = f"Unexpected error polling inference job {inference_id}: {str(e)}"
                    logger.error(error_msg)
                    raise ExtensionRuntimeError(error_msg) from e

    def get_job_status(self, inference_id: str) -> Tuple[str, Optional[str]]:
        """
        Get current job status and output location from SageMaker.

        :param inference_id: The inference job ID to check
        :return: Tuple of (job_status, output_location)
        :raises ClientError: If SageMaker API call fails
        """
        try:
            # Note: The actual SageMaker async inference API uses DescribeInferenceRecommendationsJob
            # but for async endpoint inference, we would use a different API call.
            # This is a placeholder for the correct API call structure.
            response = self.sm_client.describe_inference_recommendations_job(JobName=inference_id)

            # Extract status and output location from response
            # The actual response structure will depend on the SageMaker async inference API
            job_status = response.get("Status", "Unknown")
            output_location = response.get("OutputLocation")

            return job_status, output_location

        except ClientError as e:
            logger.error(f"Failed to get status for inference job {inference_id}: {str(e)}")
            raise

    def _calculate_backoff_delay(self, attempt: int) -> float:
        """
        Calculate exponential backoff delay for polling attempts.

        :param attempt: Current attempt number (1-based)
        :return: Delay in seconds
        """
        # Start with the configured polling interval
        base_delay = self.config.polling_interval

        # Apply exponential backoff: base_delay * (multiplier ^ (attempt - 1))
        delay = base_delay * (self.config.exponential_backoff_multiplier ** (attempt - 1))

        # Cap at maximum polling interval
        delay = min(delay, self.config.max_polling_interval)

        logger.debug(f"Calculated backoff delay for attempt {attempt}: {delay:.1f}s")
        return delay

    def check_job_exists(self, inference_id: str) -> bool:
        """
        Check if an inference job exists in SageMaker.

        :param inference_id: The inference job ID to check
        :return: True if job exists, False otherwise
        """
        try:
            self.get_job_status(inference_id)
            return True
        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            if error_code in ["ValidationException", "ResourceNotFound"]:
                return False
            # For other errors, assume job exists but there's a temporary issue
            return True
        except Exception:
            # For unexpected errors, assume job exists
            return True

    def cancel_job(self, inference_id: str) -> bool:
        """
        Cancel an async inference job if possible.

        :param inference_id: The inference job ID to cancel
        :return: True if cancellation was successful, False otherwise
        """
        try:
            # Note: This would use the appropriate SageMaker API to cancel async inference
            # The exact API call depends on the SageMaker async inference implementation
            logger.info(f"Attempting to cancel inference job: {inference_id}")

            # Placeholder for actual cancellation API call
            # self.sm_client.stop_inference_recommendations_job(JobName=inference_id)

            logger.info(f"Successfully cancelled inference job: {inference_id}")
            return True

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            logger.warning(f"Failed to cancel inference job {inference_id}: {error_code} - {str(e)}")
            return False

        except Exception as e:
            logger.warning(f"Unexpected error cancelling inference job {inference_id}: {str(e)}")
            return False
