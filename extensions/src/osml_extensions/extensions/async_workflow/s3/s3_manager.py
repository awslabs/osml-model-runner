#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
import time
import uuid
from datetime import datetime
from io import BufferedReader
from json import JSONDecodeError
from typing import Dict, Optional
from urllib.parse import urlparse

import boto3
import geojson
from aws_embedded_metrics.logger.metrics_logger import MetricsLogger
from aws_embedded_metrics.metric_scope import metric_scope
from aws_embedded_metrics.unit import Unit
from botocore.exceptions import ClientError, NoCredentialsError
from geojson import FeatureCollection

from aws.osml.model_runner.app_config import BotoConfig
from aws.osml.model_runner.common import Timer

from ..async_app_config import AsyncServiceConfig
from ..errors import ExtensionRuntimeError

logger = logging.getLogger(__name__)


class S3OperationError(ExtensionRuntimeError):
    """Raised when S3 upload/download operations fail."""

    pass


class S3Manager:
    """
    Handles all S3 operations for async endpoint processing with proper error handling and retries.

    This class provides robust S3 operations including upload, download, cleanup, and unique key generation
    with comprehensive retry logic and timing metrics.
    """

    def __init__(self, assumed_credentials: Optional[Dict] = None):
        """
        Initialize S3Manager with client and configuration.

        :param s3_client: Boto3 S3 client instance
        """

        # Initialize S3 client with same credentials as SageMaker client
        if assumed_credentials is not None:
            self.s3_client = boto3.client(
                "s3",
                config=BotoConfig.default,
                aws_access_key_id=assumed_credentials.get("AccessKeyId"),
                aws_secret_access_key=assumed_credentials.get("SecretAccessKey"),
                aws_session_token=assumed_credentials.get("SessionToken"),
            )
        else:
            self.s3_client = boto3.client("s3", config=BotoConfig.default)

        self.config = AsyncServiceConfig.async_endpoint_config
        logger.debug(
            f"S3Manager initialized with buckets: input={self.config.input_bucket}, output={self.config.output_bucket}"
        )

    @metric_scope
    def upload_payload(self, payload: BufferedReader, key: str, metrics: MetricsLogger) -> str:
        """
        Upload payload to S3 input bucket with retry logic and timing metrics.

        :param payload: BufferedReader containing the data to upload
        :param key: S3 key for the object
        :param metrics: metrics logger for tracking performance
        :return: S3 URI of the uploaded object
        :raises S3OperationError: If upload fails after all retries
        """
        s3_uri = self.config.get_input_s3_uri(key)
        logger.debug(f"Uploading payload to S3: {s3_uri}")

        if isinstance(metrics, MetricsLogger):
            metrics.put_dimensions({"Operation": "S3Upload", "Bucket": self.config.input_bucket})

        # Read payload data
        payload.seek(0)  # Ensure we're at the beginning
        payload_data = payload.read()
        payload_size = len(payload_data)

        if isinstance(metrics, MetricsLogger):
            metrics.put_metric("S3UploadSize", payload_size, str(Unit.BYTES.value))

        for attempt in range(self.config.max_retries + 1):
            try:
                with Timer(
                    task_str=f"S3 Upload (attempt {attempt + 1})",
                    metric_name="S3UploadDuration",
                    logger=logger,
                    metrics_logger=metrics,
                ):
                    self.s3_client.put_object(
                        Bucket=self.config.input_bucket,
                        Key=f"{self.config.input_prefix}{key}",
                        Body=payload_data,
                        ContentType="application/json",
                    )

                if isinstance(metrics, MetricsLogger):
                    metrics.put_metric("S3UploadSuccess", 1, str(Unit.COUNT.value))

                logger.debug(f"Successfully uploaded payload to {s3_uri}")
                return s3_uri

            except (ClientError, NoCredentialsError) as e:
                error_code = getattr(e, "response", {}).get("Error", {}).get("Code", "Unknown")
                logger.warning(f"S3 upload attempt {attempt + 1} failed: {error_code} - {str(e)}")

                if isinstance(metrics, MetricsLogger):
                    metrics.put_metric("S3UploadRetries", 1, str(Unit.COUNT.value))

                if attempt == self.config.max_retries:
                    if isinstance(metrics, MetricsLogger):
                        metrics.put_metric("S3UploadErrors", 1, str(Unit.COUNT.value))

                    error_msg = f"Failed to upload payload to S3 after {self.config.max_retries + 1} attempts: {str(e)}"
                    logger.error(error_msg)
                    raise S3OperationError(error_msg) from e

                # Exponential backoff
                backoff_delay = (2**attempt) * 1.0  # 1s, 2s, 4s, etc.
                logger.debug(f"Retrying S3 upload in {backoff_delay} seconds...")
                time.sleep(backoff_delay)

            except Exception as e:
                if isinstance(metrics, MetricsLogger):
                    metrics.put_metric("S3UploadErrors", 1, str(Unit.COUNT.value))

                error_msg = f"Unexpected error during S3 upload: {str(e)}"
                logger.error(error_msg)
                raise S3OperationError(error_msg) from e

    @metric_scope
    def download_results(self, s3_uri: str, metrics: MetricsLogger) -> bytes:
        """
        Download results from S3 with retry logic and timing metrics.

        :param s3_uri: S3 URI of the object to download
        :param metrics: metrics logger for tracking performance
        :return: Downloaded data as bytes
        :raises S3OperationError: If download fails after all retries
        """
        logger.debug(f"Downloading results from S3: {s3_uri}")

        # Parse S3 URI
        parsed_uri = urlparse(s3_uri)
        bucket = parsed_uri.netloc
        key = parsed_uri.path.lstrip("/")

        if isinstance(metrics, MetricsLogger):
            metrics.put_dimensions({"Operation": "S3Download", "Bucket": bucket})

        for attempt in range(self.config.max_retries + 1):
            try:
                with Timer(
                    task_str=f"S3 Download (attempt {attempt + 1})",
                    metric_name="S3DownloadDuration",
                    logger=logger,
                    metrics_logger=metrics,
                ):
                    response = self.s3_client.get_object(Bucket=bucket, Key=key)
                    data = response["Body"].read()

                download_size = len(data)
                if isinstance(metrics, MetricsLogger):
                    metrics.put_metric("S3DownloadSize", download_size, str(Unit.BYTES.value))
                    metrics.put_metric("S3DownloadSuccess", 1, str(Unit.COUNT.value))

                logger.debug(f"Successfully downloaded {download_size} bytes from {s3_uri}")
                return data

            except ClientError as e:
                error_code = e.response.get("Error", {}).get("Code", "Unknown")
                logger.warning(f"S3 download attempt {attempt + 1} failed: {error_code} - {str(e)}")

                if isinstance(metrics, MetricsLogger):
                    metrics.put_metric("S3DownloadRetries", 1, str(Unit.COUNT.value))

                # Handle specific error cases
                if error_code == "NoSuchKey":
                    # Don't retry for missing keys, but wait a bit in case of eventual consistency
                    if attempt < self.config.max_retries:
                        time.sleep(2**attempt)
                        continue

                if attempt == self.config.max_retries:
                    if isinstance(metrics, MetricsLogger):
                        metrics.put_metric("S3DownloadErrors", 1, str(Unit.COUNT.value))

                    error_msg = f"Failed to download results from S3 after {self.config.max_retries + 1} attempts: {str(e)}"
                    logger.error(error_msg)
                    raise S3OperationError(error_msg) from e

                # Exponential backoff
                backoff_delay = (2**attempt) * 1.0
                logger.debug(f"Retrying S3 download in {backoff_delay} seconds...")
                time.sleep(backoff_delay)

            except Exception as e:
                if isinstance(metrics, MetricsLogger):
                    metrics.put_metric("S3DownloadErrors", 1, str(Unit.COUNT.value))

                error_msg = f"Unexpected error during S3 download: {str(e)}"
                logger.error(error_msg)
                raise S3OperationError(error_msg) from e

    def delete_object(self, s3_uri: str) -> None:
        """
        Delete S3 object with error handling.

        :param s3_uri: S3 URI of the object to delete
        """
        if not self.config.cleanup_enabled:
            logger.debug(f"Cleanup disabled, skipping deletion of {s3_uri}")
            return

        logger.debug(f"Deleting S3 object: {s3_uri}")

        try:
            # Parse S3 URI
            parsed_uri = urlparse(s3_uri)
            bucket = parsed_uri.netloc
            key = parsed_uri.path.lstrip("/")

            self.s3_client.delete_object(Bucket=bucket, Key=key)
            logger.debug(f"Successfully deleted {s3_uri}")

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            # Don't raise exceptions for cleanup failures, just log them
            logger.warning(f"Failed to delete S3 object {s3_uri}: {error_code} - {str(e)}")

        except Exception as e:
            logger.warning(f"Unexpected error deleting S3 object {s3_uri}: {str(e)}")

    def generate_unique_key(self, prefix: str = "") -> str:
        """
        Generate unique S3 key using timestamp and UUID.

        :param prefix: Optional prefix for the key
        :return: Unique S3 key
        """
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        unique_id = str(uuid.uuid4())[:8]  # Use first 8 characters of UUID

        if prefix:
            return f"{prefix}_{timestamp}_{unique_id}"
        else:
            return f"{timestamp}_{unique_id}"

    def cleanup_s3_objects(self, s3_uris: list) -> None:
        """
        Clean up multiple S3 objects.

        :param s3_uris: List of S3 URIs to delete
        """
        if not self.config.cleanup_enabled:
            logger.debug("Cleanup disabled, skipping deletion of S3 objects")
            return

        logger.debug(f"Cleaning up {len(s3_uris)} S3 objects")

        for s3_uri in s3_uris:
            self.delete_object(s3_uri)

    def validate_bucket_access(self) -> None:
        """
        Validate that the configured S3 buckets are accessible.

        :raises S3OperationError: If buckets are not accessible
        """
        logger.debug("Validating S3 bucket access")

        try:
            # Check input bucket
            self.s3_client.head_bucket(Bucket=self.config.input_bucket)
            logger.debug(f"Input bucket {self.config.input_bucket} is accessible")

            # Check output bucket
            self.s3_client.head_bucket(Bucket=self.config.output_bucket)
            logger.debug(f"Output bucket {self.config.output_bucket} is accessible")

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            error_msg = f"S3 bucket access validation failed: {error_code} - {str(e)}"
            logger.error(error_msg)
            raise S3OperationError(error_msg) from e

        except Exception as e:
            error_msg = f"Unexpected error during S3 bucket validation: {str(e)}"
            logger.error(error_msg)
            raise S3OperationError(error_msg) from e

    def _upload_to_s3(self, payload: BufferedReader, key: str, metrics: Optional[MetricsLogger]) -> str:
        """
        Upload payload to S3 input bucket with unique key generation.

        :param payload: BufferedReader containing the data to upload
        :param key: S3 key for the object
        :param metrics: Optional metrics logger
        :return: S3 URI of uploaded object
        """
        logger.debug("Uploading payload to S3 for async inference")
        return self.upload_payload(payload, key, metrics)

    def _download_from_s3(self, output_s3_uri: str, metrics: Optional[MetricsLogger]) -> FeatureCollection:
        """
        Download and parse results from S3 output location.

        :param output_s3_uri: S3 URI of the output data
        :param metrics: Optional metrics logger
        :return: Parsed FeatureCollection
        """
        logger.debug(f"Downloading results from S3: {output_s3_uri}")

        try:
            # Download results
            result_data = self.download_results(output_s3_uri, metrics)

            # Parse as geojson FeatureCollection
            feature_collection = geojson.loads(result_data.decode("utf-8"))

            logger.debug(
                f"Successfully parsed FeatureCollection with {len(feature_collection.get('features', []))} features"
            )
            return feature_collection

        except (UnicodeDecodeError, JSONDecodeError) as e:
            logger.error(f"Failed to parse async inference results: {str(e)}")
            raise JSONDecodeError(f"Failed to parse async inference results: {str(e)}", "", 0)

    # def _cleanup_s3_objects(self, s3_uris: list) -> None:
    #     """
    #     Clean up temporary S3 objects.

    #     :param s3_uris: List of S3 URIs to delete
    #     """
    #     if self.async_config.cleanup_enabled:
    #         logger.debug(f"Cleaning up {len(s3_uris)} S3 objects")
    #         self.cleanup_s3_objects(s3_uris)
