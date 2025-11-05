#!/usr/bin/env python3

#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
Unified integration test runner for OSML Model Runner.

This module provides a clean, simple interface for running integration tests
without external dependencies on scripts/integration/test.py.
"""

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime
from secrets import token_hex
from typing import Any, Dict, List, Optional, Tuple

# Add the project root to Python path before importing other modules
# This allows imports like 'test.integration.integ_config' to work when running the script directly
_project_root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "../..")
sys.path.insert(0, _project_root)

# Get the script directory for resolving relative paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

from test.integration.feature_validator import FeatureValidator  # noqa: E402

# Import IntegConfig to build configuration from task definition
from test.integration.integ_config import IntegConfig  # noqa: E402
from test.integration.integ_types import ImageRequest, ModelInvokeMode  # noqa: E402

import boto3  # noqa: E402
import geojson  # noqa: E402
from botocore.exceptions import ClientError, ParamValidationError  # noqa: E402
from geojson import Feature  # noqa: E402


class IntegRunner:
    """
    Unified integration test runner for OSML Model Runner.

    This class provides a clean interface for running integration tests
    with minimal configuration and maximum simplicity.
    """

    def __init__(self, verbose: bool = False) -> None:
        """
        Initialize the integration test runner.

        :param verbose: Enable verbose logging output.
        """
        self.setup_logging(verbose)
        self.logger = logging.getLogger(__name__)
        self.clients = self._get_aws_clients()
        self.config = IntegConfig()
        self.validator = FeatureValidator()
        self._account_placeholder_logged = False

    def setup_logging(self, verbose: bool = False) -> None:
        """
        Set up logging configuration.

        :param verbose: Enable verbose (DEBUG) logging if True, otherwise INFO level.
        """
        level = logging.DEBUG if verbose else logging.INFO

        # Use a cleaner format: just time and message for INFO, full details for others
        class InfoFormatter(logging.Formatter):
            def format(self, record):
                if record.levelno == logging.INFO:
                    return record.getMessage()
                else:
                    return f"⚠️  {record.getMessage()}"

        handler = logging.StreamHandler()
        handler.setFormatter(InfoFormatter())

        root_logger = logging.getLogger()
        root_logger.setLevel(level)
        root_logger.handlers = []
        root_logger.addHandler(handler)

    def _get_aws_clients(self) -> Dict[str, Any]:
        """
        Get AWS service clients for the test.

        :returns: Dictionary mapping service names to boto3 clients/resources.
        """
        return {
            "sqs": boto3.resource("sqs"),
            "s3": boto3.client("s3"),
            "kinesis": boto3.client("kinesis"),
            "ddb": boto3.resource("dynamodb"),
            "elb": boto3.client("elbv2"),
        }

    def run_test(
        self,
        image_request: ImageRequest,
        expected_output_path: Optional[str] = None,
        timeout_minutes: int = 30,
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Run a complete integration test.

        :param image_request: ImageRequest object containing all request parameters.
        :param expected_output_path: Path to expected output file for validation.
        :param timeout_minutes: Maximum time to wait for completion.
        :returns: Tuple of (success: bool, results: dict).
        """
        self.logger.info(f"\n{'=' * 60}")
        self.logger.info("🧪  OSML Model Runner Integration Test")
        self.logger.info(f"{'=' * 60}")
        self.logger.info(f"Image: {image_request.image_url}")
        self.logger.info(f"Model: {image_request.model_name}")
        self.logger.info(f"Timeout: {timeout_minutes} minutes")
        self.logger.info(f"{'=' * 60}\n")

        try:
            # Prepare endpoint and result destinations
            endpoint, endpoint_type = self._resolve_endpoint(image_request)
            result_stream, result_bucket = self._resolve_result_destinations(image_request)

            # Build and submit request
            image_processing_request = self._build_image_processing_request(
                endpoint=endpoint,
                endpoint_type=endpoint_type,
                image_url=image_request.image_url,
                model_variant=(
                    image_request.model_endpoint_parameters.get("TargetVariant")
                    if image_request.model_endpoint_parameters
                    else None
                ),
                target_container=(
                    image_request.model_endpoint_parameters.get("TargetContainerHostname")
                    if image_request.model_endpoint_parameters
                    else None
                ),
                tile_size=image_request.tile_size_scalar,
                tile_overlap=image_request.tile_overlap_scalar,
                tile_format=image_request.tile_format,
                tile_compression=image_request.tile_compression,
                region_of_interest=image_request.region_of_interest,
                result_stream=result_stream,
                result_bucket=result_bucket,
            )

            # Get Kinesis shard iterator BEFORE submitting the job
            shard_iter = self._get_kinesis_shard(result_stream)

            # Submit the request
            message_id = self._queue_image_processing_job(image_processing_request)
            self.logger.info(f"📤 Request submitted (message ID: {message_id[:16]}...)")

            # Monitor the job
            job_id = image_processing_request["jobId"]
            image_id = f"{job_id}:{image_request.image_url}"
            self._monitor_job_status(image_id, timeout_minutes)

            # Build results and validate
            results = {
                "job_id": job_id,
                "image_id": image_id,
                "image_uri": image_request.image_url,
                "model_name": image_request.model_name,
                "endpoint_type": endpoint_type,
                "status": "completed",
                "timestamp": datetime.now().isoformat(),
            }

            # Validate results
            validation_result = self._validate_results(
                image_request, image_processing_request, job_id, image_id, shard_iter, expected_output_path
            )
            results.update(validation_result)

            self.logger.info("✅ Integration test completed successfully!")
            return True, results

        except Exception as e:
            self.logger.error(f"Integration test failed: {e}")
            return False, {"error": str(e), "timestamp": datetime.now().isoformat()}

    def _get_kinesis_shard(self, stream_name: str) -> str:
        """
        Get a Kinesis shard iterator for result monitoring.

        :param stream_name: Name of the Kinesis stream.
        :returns: Shard iterator string for reading records.
        """
        stream_desc = self.clients["kinesis"].describe_stream(StreamName=stream_name)["StreamDescription"]
        return self.clients["kinesis"].get_shard_iterator(
            StreamName=stream_name, ShardId=stream_desc["Shards"][0]["ShardId"], ShardIteratorType="LATEST"
        )["ShardIterator"]

    def _build_image_processing_request(
        self,
        endpoint: str,
        endpoint_type: str,
        image_url: str,
        result_stream: str,
        result_bucket: str,
        model_variant: Optional[str] = None,
        target_container: Optional[str] = None,
        tile_size: int = 512,
        tile_overlap: int = 128,
        tile_format: str = "GTIFF",
        tile_compression: str = "NONE",
        post_processing: str = (
            '[{"step": "FEATURE_DISTILLATION", ' '"algorithm": {"algorithmType": "NMS", "iouThreshold": 0.75}}]'
        ),
        region_of_interest: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Build an image processing request for submission to ModelRunner.

        :param endpoint: Model endpoint name or URL.
        :param endpoint_type: Type of endpoint ("SM_ENDPOINT" or "HTTP_ENDPOINT").
        :param image_url: URL of image to process.
        :param result_stream: Full Kinesis stream name for results.
        :param result_bucket: Full S3 bucket name for results.
        :param model_variant: Optional SageMaker model variant.
        :param target_container: Optional target container hostname.
        :param tile_size: Size of image tiles for processing.
        :param tile_overlap: Overlap between tiles.
        :param tile_format: Format for tile output.
        :param tile_compression: Compression for tile output.
        :param post_processing: JSON string defining post-processing steps.
        :param region_of_interest: Optional region of interest specification.
        :returns: Complete image processing request dictionary.
        """
        job_id = token_hex(16)
        job_name = f"test-{job_id}"

        request: Dict[str, Any] = {
            "jobName": job_name,
            "jobId": job_id,
            "imageUrls": [image_url],
            "outputs": [
                {"type": "S3", "bucket": result_bucket, "prefix": f"{job_name}/"},
                {"type": "Kinesis", "stream": result_stream, "batchSize": 1000},
            ],
            "imageProcessor": {"name": endpoint, "type": endpoint_type},
            "imageProcessorTileSize": tile_size,
            "imageProcessorTileOverlap": tile_overlap,
            "imageProcessorTileFormat": tile_format,
            "imageProcessorTileCompression": tile_compression,
            "postProcessing": json.loads(post_processing),
            "regionOfInterest": region_of_interest,
        }

        # Add endpoint parameters if provided
        if model_variant or target_container:
            request["imageProcessorParameters"] = {}
            if model_variant:
                request["imageProcessorParameters"]["TargetVariant"] = model_variant
            if target_container:
                request["imageProcessorParameters"]["TargetContainerHostname"] = target_container

        return request

    def _queue_image_processing_job(self, image_processing_request: Dict[str, Any]) -> Optional[str]:
        """
        Submit an image processing request to the SQS queue.

        :param image_processing_request: The request to submit.
        :returns: Message ID of the queued message.
        :raises ClientError: If SQS operation fails.
        :raises ParamValidationError: If request validation fails.
        """
        try:
            queue = self.clients["sqs"].get_queue_by_name(
                QueueName=self.config.IMAGE_QUEUE_NAME, QueueOwnerAWSAccountId=self.config.ACCOUNT
            )
            response = queue.send_message(MessageBody=json.dumps(image_processing_request))

            message_id = response.get("MessageId")

            return message_id

        except ClientError as error:
            self.logger.error(f"Unable to send job request to SQS queue: {self.config.IMAGE_QUEUE_NAME}")
            self.logger.error(f"{error}")
            raise

        except ParamValidationError as error:
            self.logger.error("Invalid SQS API request; validation failed")
            self.logger.error(f"{error}")
            raise

    def _get_http_endpoint_url(self) -> str:
        """
        Get the HTTP endpoint URL by looking up the load balancer DNS name.

        :returns: Full HTTP endpoint URL (e.g., "http://test-http-model-endpoint-xxx.elb.amazonaws.com/invocations").
        :raises ValueError: If load balancer DNS name cannot be found.
        """
        elb_name = "test-http-model-endpoint"
        self.logger.info(f"Looking up load balancer: {elb_name}")

        try:
            response = self.clients["elb"].describe_load_balancers(Names=[elb_name])
            dns_name = response.get("LoadBalancers", [{}])[0].get("DNSName")

            if not dns_name:
                raise ValueError(f"Could not find DNS name for load balancer: {elb_name}")

            http_url = f"http://{dns_name}/invocations"
            self.logger.info(f"Resolved HTTP endpoint URL: {http_url}")
            return http_url

        except Exception as e:
            self.logger.error(f"Failed to resolve HTTP endpoint: {e}")
            raise

    def _monitor_job_status(self, image_id: str, timeout_minutes: int = 30) -> None:
        """
        Monitor job status until completion or timeout.

        :param image_id: Image ID to monitor.
        :param timeout_minutes: Maximum time to wait for completion.
        :raises TimeoutError: If job doesn't complete within timeout.
        :raises AssertionError: If job fails.
        """
        done = False
        max_retries = timeout_minutes * 12  # 12 retries per minute (5 second intervals)
        retry_interval = 5

        queue = self.clients["sqs"].get_queue_by_name(
            QueueName=self.config.IMAGE_STATUS_QUEUE_NAME, QueueOwnerAWSAccountId=self.config.ACCOUNT
        )

        self.logger.info(f"⏳ Monitoring job progress (timeout: {timeout_minutes} minutes)...")

        start_time = time.time()

        while not done and max_retries > 0:
            try:
                # Use WaitTimeSeconds for long polling to reduce API calls
                messages = queue.receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=5, VisibilityTimeout=30)

                # Process all messages in the batch
                for message in messages:
                    try:
                        # Parse the SNS message format
                        message_body = json.loads(message.body)
                        message_attributes = message_body.get("MessageAttributes", {})

                        # Extract values from SNS MessageAttributes format
                        message_image_id = message_attributes.get("image_id", {}).get("Value")
                        message_image_status = message_attributes.get("status", {}).get("Value")

                        if message_image_status == "IN_PROGRESS" and message_image_id == image_id:
                            elapsed = int(time.time() - start_time)
                            self.logger.info(f"📊 IN_PROGRESS - Image processing started (elapsed: {elapsed}s)")

                        elif message_image_status == "SUCCESS" and message_image_id == image_id:
                            processing_duration = message_attributes.get("processing_duration", {}).get("Value")
                            if processing_duration is not None:
                                assert float(processing_duration) > 0
                            done = True
                            elapsed = int(time.time() - start_time)
                            if processing_duration is not None:
                                self.logger.info(
                                    f"\n✓ Processing completed in " f"{processing_duration}s (total wait: {elapsed}s)\n"
                                )
                            else:
                                self.logger.info(f"\n✓ Processing completed (total wait: {elapsed}s)\n")

                        elif (
                            message_image_status == "FAILED" or message_image_status == "PARTIAL"
                        ) and message_image_id == image_id:
                            failure_message = ""
                            try:
                                message_body = json.loads(message.body).get("Message", "")
                                failure_message = str(message_body)
                            except Exception as e:
                                self.logger.warning(f"Failed to extract failure message from SNS message: {e}")
                            self.logger.error(
                                f"❌ FAILED - Image processing failed with status: {message_image_status}. {failure_message}"
                            )
                            raise AssertionError(f"Image processing failed with status: {message_image_status}")

                        else:
                            # Only log every 30 seconds to reduce noise
                            if max_retries % 12 == 0:  # 12 retries = 60 seconds
                                elapsed = int(time.time() - start_time)
                                self.logger.info(f"⏳ Still waiting... (elapsed: {elapsed // 60}m {elapsed % 60}s)")

                    except json.JSONDecodeError as e:
                        self.logger.warning(f"Failed to parse message body as JSON: {e}")
                    except Exception as e:
                        self.logger.warning(f"Error processing message: {e}")

                # Delete all messages after processing the batch
                for message in messages:
                    try:
                        message.delete()
                    except Exception as e:
                        self.logger.warning(f"Failed to delete message: {e}")

                # If we found success, break out of the main retry loop
                if done:
                    break

            except ClientError as err:
                self.logger.warning(f"ClientError in monitor_job_status: {err}")
                # Don't raise immediately, continue retrying

            except Exception as err:
                self.logger.error(f"Unexpected error in monitor_job_status: {err}")
                raise

            max_retries -= 1
            time.sleep(retry_interval)

        if not done:
            elapsed = int(time.time() - start_time)
            self.logger.error(f"Maximum retries reached waiting for {image_id}.")
            self.logger.error(f"Total time waited: {elapsed} seconds ({timeout_minutes} minutes)")
            raise TimeoutError(f"Image processing timed out after {timeout_minutes} minutes for image {image_id}")

        assert done

    # ============================================================================
    # Validation Helper Methods
    # ============================================================================

    def _resolve_endpoint(self, image_request: ImageRequest) -> Tuple[str, str]:
        """
        Resolve endpoint URL and type from image request.

        :param image_request: Image request containing model information.
        :returns: Tuple of (endpoint_url, endpoint_type).
        """
        endpoint_type = "SM_ENDPOINT" if image_request.model_invoke_mode.value == "SM_ENDPOINT" else "HTTP_ENDPOINT"
        endpoint = image_request.model_name
        if endpoint_type == "HTTP_ENDPOINT":
            endpoint = self._get_http_endpoint_url()
        return endpoint, endpoint_type

    def _resolve_result_destinations(self, image_request: ImageRequest) -> Tuple[str, str]:
        """
        Resolve result stream and bucket names from image request or config defaults.

        :param image_request: Image request containing optional destination names.
        :returns: Tuple of (result_stream_name, result_bucket_name).
        """
        result_stream = (
            image_request.kinesis_stream_name or f"{self.config.KINESIS_RESULTS_STREAM_PREFIX}-{self.config.ACCOUNT}"
        )
        result_bucket = image_request.s3_bucket_name or f"{self.config.S3_RESULTS_BUCKET_PREFIX}-{self.config.ACCOUNT}"
        return result_stream, result_bucket

    def _validate_results(
        self,
        image_request: ImageRequest,
        image_processing_request: Dict[str, Any],
        job_id: str,
        image_id: str,
        shard_iter: Optional[str],
        expected_output_path: Optional[str],
    ) -> Dict[str, Any]:
        """
        Validate test results based on model type and expected output.

        :param image_request: Original image request.
        :param image_processing_request: The processing request that was submitted.
        :param job_id: Job ID for result correlation.
        :param image_id: Image ID for result correlation.
        :param shard_iter: Kinesis shard iterator for streaming results.
        :param expected_output_path: Path to expected output file, if provided.
        :returns: Dictionary containing validation results.
        :raises AssertionError: If validation fails.
        """
        results = {}

        # Check if this is a flood model test
        endpoint_params = image_request.model_endpoint_parameters or {}
        target_container = endpoint_params.get("TargetContainerHostname")
        is_flood_test = image_request.model_name == "flood" or target_container == "flood-container"

        if is_flood_test:
            self.logger.info("\n🔍 Validating flood model results (count-based)...")
            variant = endpoint_params.get("TargetVariant")
            expected_counts, expected_region = self._get_flood_model_expectations(image_request.image_url, variant)

            validation_result = self.validator.validate_by_count(
                image_id=image_id,
                expected_feature_counts=expected_counts,
                expected_region_count=expected_region,
                ddb_clients=self.clients["ddb"],
                config=self.config,
            )

            results.update(
                {
                    "feature_count": validation_result["feature_count"],
                    "region_request_count": validation_result["region_request_count"],
                    "expected_counts": expected_counts,
                    "expected_region_count": expected_region,
                    "validation": "passed" if validation_result["success"] else "failed",
                }
            )

            if not validation_result["success"]:
                raise AssertionError(validation_result["message"])

        elif expected_output_path:
            resolved_path = self._resolve_expected_output_path(expected_output_path)
            if resolved_path and os.path.exists(resolved_path):
                self.logger.info("\n🔍 Validating results...")
                kinesis_cache: Dict[str, List] = {}
                self._validate_features_match(
                    image_processing_request=image_processing_request,
                    job_id=job_id,
                    shard_iter=shard_iter,
                    result_file=resolved_path,
                    kinesis_features_cache=kinesis_cache,
                )
                results["validation"] = "passed"
            else:
                self.logger.warning(f"⚠️  Expected output file not found: {resolved_path or expected_output_path}")
                results["validation"] = "skipped - file not found"
        else:
            self.logger.info("⏭️  No expected output provided - skipping validation")
            results["validation"] = "skipped"

        return results

    def _resolve_expected_output_path(self, path: str) -> Optional[str]:
        """
        Resolve expected output path with fallbacks.

        Tries absolute path, then relative to current directory, then relative to script directory.

        :param path: Path to resolve.
        :returns: Resolved path if found, None otherwise.
        """
        if os.path.isabs(path):
            return path
        resolved = os.path.abspath(path)
        if os.path.exists(resolved):
            return resolved
        return os.path.join(SCRIPT_DIR, path)

    def _get_flood_model_expectations(self, image_url: str, variant: Optional[str] = None) -> Tuple[List[int], int]:
        """
        Get expected feature and region counts for flood model based on image type and variant.

        :param image_url: URL of the image being processed.
        :param variant: Optional model variant (e.g., 'flood-50', 'flood-100').
        :returns: Tuple of (expected_feature_counts, expected_region_count).
        """
        # For large.tif images
        if "large" in image_url:
            expected = 112200
            expected_region_count = 4
        else:
            # For other images, use smaller expected counts
            expected = 10000  # Generic fallback
            expected_region_count = 1

        # Adjust based on variant
        if variant == "flood-50":
            feature_counts = [int(expected / 2)]
        elif variant == "flood-100":
            feature_counts = [expected]
        else:
            # For default flood model, accept either full or half
            feature_counts = [expected, int(expected / 2)]

        return feature_counts, expected_region_count

    def _validate_features_match(
        self,
        image_processing_request: Dict[str, Any],
        job_id: str,
        shard_iter: Optional[str] = None,
        result_file: Optional[str] = None,
        kinesis_features_cache: Optional[Dict[str, List[Feature]]] = None,
    ) -> None:
        """
        Validate that processing results match expected features.

        :param image_processing_request: The original processing request.
        :param job_id: Job ID for result correlation.
        :param shard_iter: Kinesis shard iterator for streaming results.
        :param result_file: Path to expected results file.
        :param kinesis_features_cache: Optional cache for Kinesis features.
        :raises ValueError: If result_file is not provided.
        :raises AssertionError: If results don't match expected features.
        """
        # result_file should always be provided if validation is being performed
        if result_file is None:
            raise ValueError("result_file must be provided for feature validation")

        with open(result_file, "r") as geojson_file:
            expected_features = geojson.load(geojson_file)["features"]

        outputs: List[Dict[str, Any]] = image_processing_request["outputs"]
        found_outputs = 0

        # Check each output sink once - no retries
        for output in outputs:
            if output["type"] == "S3" and self.clients["s3"]:
                if self.validator.validate_s3_features(
                    self.clients["s3"], output["bucket"], output["prefix"], expected_features
                ):
                    found_outputs += 1
            elif output["type"] == "Kinesis":
                # Check if we have cached features first
                stream_name = output["stream"]
                if kinesis_features_cache and stream_name in kinesis_features_cache:
                    cached_features = kinesis_features_cache[stream_name]
                    if self.validator.feature_collections_equal(expected_features, cached_features):
                        found_outputs += 1
                elif self.clients["kinesis"]:
                    # Try to read from Kinesis if we don't have cached features
                    try:
                        if self.validator.validate_kinesis_features(
                            self.clients["kinesis"],
                            job_id,
                            stream_name,
                            shard_iter,
                            expected_features,
                            kinesis_features_cache,
                        ):
                            found_outputs += 1
                    except Exception as e:
                        self.logger.error(
                            f"Exception occurred during Kinesis feature validation for stream '{stream_name}': {e}",
                            exc_info=True,
                        )
                else:
                    pass

        # Fail immediately if not all outputs validated
        if found_outputs != len(outputs):
            self.logger.error(
                f"Validation failed immediately. Found {found_outputs} out of {len(outputs)} expected outputs."
            )
            raise AssertionError(
                f"Feature validation failed - only {found_outputs} out of {len(outputs)} output sinks validated"
            )

    # ============================================================================
    # Test Suite Helper Methods
    # ============================================================================

    def _create_image_request(
        self,
        image_url: str,
        model_name: str,
        model_invoke_mode: ModelInvokeMode,
        model_variant: Optional[str] = None,
        target_container: Optional[str] = None,
        region_of_interest: Optional[str] = None,
    ) -> ImageRequest:
        """
        Create an ImageRequest from parameters.

        :param image_url: URL of the image to process.
        :param model_name: Name of the model to use.
        :param model_invoke_mode: How to invoke the model (SM_ENDPOINT or HTTP_ENDPOINT).
        :param model_variant: Optional SageMaker model variant.
        :param target_container: Optional target container hostname.
        :param region_of_interest: Optional region of interest specification.
        :returns: ImageRequest object.
        """
        job_id = token_hex(16)
        endpoint_params = None
        if model_variant:
            endpoint_params = {"TargetVariant": model_variant}
        elif target_container:
            endpoint_params = {"TargetContainerHostname": target_container}

        return ImageRequest(
            job_id=job_id,
            image_id=f"{job_id}:{image_url}",
            image_url=image_url,
            model_name=model_name,
            model_invoke_mode=model_invoke_mode,
            model_endpoint_parameters=endpoint_params,
            region_of_interest=region_of_interest,
        )

    def _create_image_request_from_test_case(self, test_case: Dict[str, Any]) -> ImageRequest:
        """
        Create an ImageRequest object from a test case dictionary.

        :param test_case: Test case dictionary containing image_uri, model_name, etc.
        :returns: ImageRequest object.
        """
        endpoint_type = test_case.get("endpoint_type", "SM_ENDPOINT")
        model_invoke_mode = ModelInvokeMode.SM_ENDPOINT if endpoint_type == "SM_ENDPOINT" else ModelInvokeMode.HTTP_ENDPOINT

        image_request = self._create_image_request(
            image_url=test_case["image_uri"],
            model_name=test_case["model_name"],
            model_invoke_mode=model_invoke_mode,
            model_variant=test_case.get("model_variant"),
            target_container=test_case.get("target_container"),
            region_of_interest=test_case.get("region_of_interest"),
        )

        # Resolve result destination prefixes to full names if provided
        if "kinesis_stream_prefix" in test_case:
            image_request.kinesis_stream_name = f"{test_case['kinesis_stream_prefix']}-{self.config.ACCOUNT}"
        elif "kinesis_stream_name" in test_case:
            image_request.kinesis_stream_name = test_case["kinesis_stream_name"]

        if "s3_bucket_prefix" in test_case:
            image_request.s3_bucket_name = f"{test_case['s3_bucket_prefix']}-{self.config.ACCOUNT}"
        elif "s3_bucket_name" in test_case:
            image_request.s3_bucket_name = test_case["s3_bucket_name"]

        return image_request

    def _replace_placeholders(self, text: str) -> str:
        """
        Replace placeholders in text with environment values (e.g., ${ACCOUNT}).

        :param text: Text that may contain placeholders.
        :returns: Text with placeholders replaced.
        :raises ValueError: If ACCOUNT placeholder cannot be resolved.
        """
        if "${ACCOUNT}" not in text:
            return text

        account = self.config.ACCOUNT
        if account is None:
            raise ValueError(
                "ACCOUNT is None - cannot replace ${ACCOUNT} placeholder. Ensure AWS credentials are configured."
            )

        replaced = text.replace("${ACCOUNT}", account)
        if replaced != text and not self._account_placeholder_logged:
            self.logger.info(f"🔄 Replaced ${{ACCOUNT}} placeholder with: {account}")
            self._account_placeholder_logged = True
        return replaced

    def run_test_suite(
        self, test_cases: List[Dict[str, Any]], timeout_minutes: int = 30, delay_between_tests: int = 5
    ) -> Dict[str, Any]:
        """
        Run a suite of integration tests.

        :param test_cases: List of test case dictionaries.
        :param timeout_minutes: Maximum time to wait for each test.
        :param delay_between_tests: Delay in seconds between tests.
        :returns: Dictionary with test results summary.
        """
        # Replace placeholders in test cases
        for test_case in test_cases:
            if "image_uri" in test_case:
                test_case["image_uri"] = self._replace_placeholders(test_case["image_uri"])
            if "expected_output" in test_case:
                test_case["expected_output"] = self._replace_placeholders(test_case.get("expected_output", ""))

        self.logger.info(f"\n🧪 Starting test suite: {len(test_cases)} test(s)")
        self.logger.info("=" * 60 + "\n")

        results = {"total_tests": len(test_cases), "passed": 0, "failed": 0, "test_results": []}

        for i, test_case in enumerate(test_cases, 1):
            self.logger.info(f"\n[{i}/{len(test_cases)}] {test_case.get('name', 'Unnamed test')}")

            if i > 1:
                time.sleep(delay_between_tests)

            image_request = self._create_image_request_from_test_case(test_case)
            success, test_result = self.run_test(
                image_request=image_request,
                expected_output_path=test_case.get("expected_output"),
                timeout_minutes=test_case.get("timeout_minutes", timeout_minutes),
            )

            test_result["test_name"] = test_case.get("name", f"Test {i}")
            test_result["success"] = success
            results["test_results"].append(test_result)

            if success:
                results["passed"] += 1
            else:
                results["failed"] += 1

        self.logger.info("\n" + "=" * 60)
        self.logger.info(f"Test suite completed: {results['passed']} passed, {results['failed']} failed")
        self.logger.info("=" * 60 + "\n")
        return results


def main() -> None:
    """
    Main entry point for the integration test runner.

    Provides command-line interface for running single tests or test suites.
    """
    parser = argparse.ArgumentParser(
        description="OSML Integration Test Runner",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
            Examples:
            # Run a single test
            python integ_runner.py s3://mr-test-imagery-975050113711/small.tif centerpoint

            # Run a test with expected output validation
            python integ_runner.py s3://mr-test-imagery-975050113711/small.tif centerpoint expected.json

            # Run a test with HTTP endpoint
            python integ_runner.py s3://my-bucket/image.tif my-model expected.json --http

            # Run a test suite
            python integ_runner.py --suite centerpoint_tests.json

            # Run with custom timeout and delay
            python integ_runner.py --suite centerpoint_tests.json --timeout 1 --delay 10
        """,
    )

    # Positional arguments for single test (backward compatibility)
    parser.add_argument("image_uri", nargs="?", help="S3 URI to the test image")
    parser.add_argument("model_name", nargs="?", help="Name of the model to test")
    parser.add_argument("expected_output", nargs="?", help="Path to expected output file (optional)")

    # Test suite option
    parser.add_argument("--suite", help="Path to test suite JSON file")

    # Optional arguments
    parser.add_argument("--http", action="store_true", help="Use HTTP endpoint instead of SageMaker")
    parser.add_argument("--timeout", type=int, default=30, help="Timeout in minutes (default: 30)")
    parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    parser.add_argument("--output", help="Output file for test results (JSON format)")
    parser.add_argument("--delay", type=int, default=5, help="Delay in seconds between tests (default: 5)")
    parser.add_argument("--model-variant", help="SageMaker model variant (e.g., 'AllTraffic', 'flood-50')")
    parser.add_argument("--target-container", help="Target container hostname for multi-container endpoints")

    args = parser.parse_args()

    # Validate arguments
    if args.suite:
        if args.image_uri or args.model_name:
            parser.error("Cannot specify both --suite and individual test parameters")
    else:
        if not args.image_uri or not args.model_name:
            parser.error("Either --suite or both image_uri and model_name must be provided")

    # Initialize test runner
    runner = IntegRunner(verbose=args.verbose)

    # Run tests
    if args.suite:
        # Load test suite - resolve path relative to script directory
        resolved_suite_path = os.path.abspath(args.suite)
        with open(resolved_suite_path, "r") as f:
            test_cases = json.load(f)

        results = runner.run_test_suite(test_cases, args.timeout, args.delay)
    else:
        # Run single test
        model_invoke_mode = ModelInvokeMode.HTTP_ENDPOINT if args.http else ModelInvokeMode.SM_ENDPOINT

        image_request = runner._create_image_request(
            image_url=args.image_uri,
            model_name=args.model_name,
            model_invoke_mode=model_invoke_mode,
            model_variant=args.model_variant,
            target_container=args.target_container,
        )

        success, test_result = runner.run_test(
            image_request=image_request,
            expected_output_path=args.expected_output,
            timeout_minutes=args.timeout,
        )

        results = {
            "total_tests": 1,
            "passed": 1 if success else 0,
            "failed": 0 if success else 1,
            "test_results": [test_result],
        }

    # Output results
    if args.output:
        with open(args.output, "w") as f:
            json.dump(results, f, indent=2)
        print(f"Results saved to {args.output}")
    else:
        print("\n=== Test Results ===")
        print(json.dumps(results, indent=2))

    # Exit with appropriate code
    if results["failed"] > 0:
        print(f"\n❌ {results['failed']} test(s) FAILED")
        sys.exit(1)
    else:
        print(f"\n✅ All {results['passed']} test(s) PASSED")
        sys.exit(0)


if __name__ == "__main__":
    main()
