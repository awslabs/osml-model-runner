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

from aws.osml.model_runner.app_config import BotoConfig, ServiceConfig
from aws.osml.model_runner.common import Timer
from aws.osml.model_runner.exceptions import S3OperationError

logger = logging.getLogger(__name__)


class S3Manager:
    """
    Handles all S3 operations for async endpoint processing with proper error handling and retries.

    This class provides robust S3 operations including upload, download, cleanup, and unique key generation
    with comprehensive retry logic and timing metrics.
    """

    def __init__(self, assumed_credentials: Optional[Dict] = None):
        """
        Initialize S3Manager with client and configuration.

        :param assumed_credentials: Optional AWS credentials for S3 client
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

        self.config = ServiceConfig.async_endpoint_config
        if not ServiceConfig.input_bucket:
            raise ValueError("Input (artifact) bucket is mandatory for async processing")
        logger.debug(f"S3Manager initialized with input bucket: {ServiceConfig.input_bucket}")

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

        if isinstance(metrics, MetricsLogger):
            metrics.put_dimensions({"Operation": "S3Upload", "Bucket": ServiceConfig.input_bucket})

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
                        Bucket=ServiceConfig.input_bucket,
                        Key=key,
                        Body=payload_data,
                        ContentType="application/json",
                    )

                s3_uri = f"s3://{ServiceConfig.input_bucket}/{key}"
                logger.debug(f"Uploading payload to S3: {s3_uri}")

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

    def validate_bucket_access(self) -> None:
        """
        Validate that the configured S3 input bucket is accessible.

        :raises S3OperationError: If bucket is not accessible
        """
        logger.debug("Validating S3 bucket access")

        try:
            # Check input bucket
            self.s3_client.head_bucket(Bucket=ServiceConfig.input_bucket)
            logger.debug(f"Input bucket {ServiceConfig.input_bucket} is accessible")

        except ClientError as e:
            error_code = e.response.get("Error", {}).get("Code", "Unknown")
            error_msg = f"S3 bucket access validation failed: {error_code} - {str(e)}"
            logger.error(error_msg)
            raise S3OperationError(error_msg) from e

        except Exception as e:
            error_msg = f"Unexpected error during S3 bucket validation: {str(e)}"
            logger.error(error_msg)
            raise S3OperationError(error_msg) from e


    def _download_from_s3(self, output_s3_uri: str) -> FeatureCollection:
        """
        Download and parse results from S3 output location.

        :param output_s3_uri: S3 URI of the output data
        :return: Parsed FeatureCollection
        """
        logger.debug(f"Downloading results from S3: {output_s3_uri}")

        try:
            # Download results
            result_data = self.download_results(output_s3_uri)

            # Parse as geojson FeatureCollection
            feature_collection = geojson.loads(result_data.decode("utf-8"))

            logger.debug(
                f"Successfully parsed FeatureCollection with {len(feature_collection.get('features', []))} features"
            )
            return feature_collection

        except (UnicodeDecodeError, JSONDecodeError) as e:
            logger.error(f"Failed to parse async inference results: {str(e)}")
            raise JSONDecodeError(f"Failed to parse async inference results: {str(e)}", "", 0)

    def does_object_exist(self, s3_uri: str):
        try:
            # Parse S3 URI
            parsed_uri = urlparse(s3_uri)
            bucket = parsed_uri.netloc
            key = parsed_uri.path.lstrip("/")

            # head_object is the fastest approach to determine if it exists in S3
            # also its less expensive to do the head_object approach
            self.s3_client.head_object(Bucket=bucket, Key=key)
            return True
        except Exception as err: #"This image does not exist!
            return False