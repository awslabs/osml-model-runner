#  Copyright 2025-2026 Amazon.com, Inc. or its affiliates.

import time
import unittest
from typing import List, Optional
from unittest.mock import Mock, patch

import boto3
from moto import mock_aws

from aws.osml.model_runner.api import ImageRequest
from aws.osml.model_runner.database import ImageRequestStatusRecord
from aws.osml.model_runner.scheduler.endpoint_load_image_scheduler import (
    EndpointLoadImageScheduler,
    EndpointUtilizationSummary,
)


@mock_aws
class TestEndpointLoadImageScheduler(unittest.TestCase):
    """Test cases for EndpointLoadImageScheduler"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        # Set up mock AWS resources
        self.sagemaker = boto3.client("sagemaker")

        # Create mock endpoints in SageMaker
        self.endpoints = {
            "endpoint1": {"InstanceCount": 2},
            "endpoint2": {"InstanceCount": 1},
            "endpoint3": {"InstanceCount": 3},
        }

        for endpoint_id, config in self.endpoints.items():
            self.sagemaker.create_model(
                ModelName=f"{endpoint_id}-model", PrimaryContainer={"Image": "test-model-container-image"}
            )
            self.sagemaker.create_endpoint_config(
                EndpointConfigName=f"{endpoint_id}-config",
                ProductionVariants=[
                    {
                        "InstanceType": "ml.m5.xlarge",
                        "InitialInstanceCount": config["InstanceCount"],
                        "VariantName": "AllTraffic",
                        "ModelName": f"{endpoint_id}-model",
                    }
                ],
            )

            self.sagemaker.create_endpoint(EndpointName=f"{endpoint_id}-model", EndpointConfigName=f"{endpoint_id}-config")

        # Create mock BufferedImageRequestQueue
        self.mock_queue = Mock()
        self.mock_queue.retry_time = 600

        # Create scheduler
        self.scheduler = EndpointLoadImageScheduler(image_request_queue=self.mock_queue)

    def create_sample_image_request(self, job_name: str = "test-job", model_name: str = "endpoint1-model") -> ImageRequest:
        """Helper method to create a sample ImageRequest"""
        return ImageRequest.from_external_message(
            {
                "jobName": job_name,
                "jobId": f"{job_name}-id",
                "imageUrls": ["s3://test-bucket/test.nitf"],
                "outputs": [
                    {"type": "S3", "bucket": "test-bucket", "prefix": "results"},
                    {"type": "Kinesis", "stream": "test-stream", "batchSize": 1000},
                ],
                "imageProcessor": {"name": model_name, "type": "SM_ENDPOINT"},
                "imageProcessorTileSize": 2048,
                "imageProcessorTileOverlap": 50,
            }
        )

    def create_status_record(
        self,
        job_name: str,
        model_name: str,
        request_time: Optional[int] = None,
        last_attempt: Optional[int] = None,
        num_attempts: Optional[int] = None,
        regions_complete: Optional[List[str]] = None,
        region_count: Optional[int] = None,
    ) -> ImageRequestStatusRecord:
        """Helper method to create a status record"""
        image_request = self.create_sample_image_request(job_name, model_name)
        image_status_record = ImageRequestStatusRecord.new_from_request(image_request)
        if request_time is not None:
            image_status_record.request_time = request_time
        if last_attempt is not None:
            image_status_record.last_attempt = last_attempt
        if num_attempts is not None:
            image_status_record.num_attempts = num_attempts
        if regions_complete is not None:
            image_status_record.regions_complete = regions_complete
        if region_count is not None:
            image_status_record.region_count = region_count
        return image_status_record

    def test_get_next_scheduled_request_no_requests(self):
        """Test scheduling when there are no requests"""
        self.mock_queue.get_outstanding_requests.return_value = []
        result = self.scheduler.get_next_scheduled_request()
        self.assertIsNone(result)

    def test_get_next_scheduled_request_single_endpoint(self):
        """Test scheduling with requests for a single endpoint"""
        time_in_past = int(time.time() - 5)
        status_records = [
            self.create_status_record("job1", "endpoint1-model", request_time=time_in_past),
            self.create_status_record("job2", "endpoint1-model", request_time=time_in_past + 1),
        ]

        self.mock_queue.get_outstanding_requests.return_value = status_records
        self.mock_queue.requested_jobs_table.start_next_attempt.return_value = True

        result = self.scheduler.get_next_scheduled_request()
        self.assertIsNotNone(result)
        self.assertEqual(result.job_id, "job1-id")

    def test_get_next_scheduled_request_multiple_endpoints(self):
        """Test scheduling with requests across multiple endpoints"""
        time_in_past = int(time.time() - 10)
        status_records = [
            self.create_status_record("job1", "endpoint1-model", request_time=time_in_past + 1),
            self.create_status_record("job2", "endpoint2-model", request_time=time_in_past),
            self.create_status_record("job3", "endpoint3-model", request_time=time_in_past + 2),
        ]

        self.mock_queue.get_outstanding_requests.return_value = status_records
        self.mock_queue.requested_jobs_table.start_next_attempt.return_value = True

        result = self.scheduler.get_next_scheduled_request()
        self.assertIsNotNone(result)
        # Should choose job2 because all 3 endpoints have no load and it was submitted first
        self.assertEqual(result.job_id, "job2-id")

    def test_get_next_scheduled_request_with_existing_load(self):
        """Test scheduling considering existing endpoint load"""
        status_records = [
            # endpoint1 (2 instances) has 3 running jobs
            self.create_status_record("job1", "endpoint1-model", region_count=1),
            self.create_status_record("job2", "endpoint1-model", region_count=1),
            self.create_status_record("job3", "endpoint1-model", region_count=1),
            # endpoint2 (1 instance) has 1 running job
            self.create_status_record("job4", "endpoint2-model", region_count=1),
            # endpoint3 (3 instances) has no running jobs
            self.create_status_record("job5", "endpoint3-model"),
        ]

        self.mock_queue.get_outstanding_requests.return_value = status_records
        self.mock_queue.requested_jobs_table.start_next_attempt.return_value = True

        result = self.scheduler.get_next_scheduled_request()
        self.assertIsNotNone(result)
        # Should choose endpoint3 as it has lowest load factor (0/3)
        self.assertEqual(result.job_id, "job5-id")

    def test_get_next_scheduled_request_start_attempt_failure(self):
        """Test scheduling when start_next_attempt fails"""
        status_records = [self.create_status_record("job1", "endpoint1-model")]

        self.mock_queue.get_outstanding_requests.return_value = status_records
        self.mock_queue.requested_jobs_table.start_next_attempt.return_value = False

        result = self.scheduler.get_next_scheduled_request()
        self.assertIsNone(result)

    def test_get_next_scheduled_request_sagemaker_error(self):
        """Test handling of SageMaker API errors"""
        status_records = [
            self.create_status_record("job1", "nonexistent-endpoint", region_count=1),
            self.create_status_record("job2", "endpoint3-model", region_count=2),
        ]

        self.mock_queue.get_outstanding_requests.return_value = status_records
        self.mock_queue.requested_jobs_table.start_next_attempt.return_value = True

        result = self.scheduler.get_next_scheduled_request()
        self.assertIsNotNone(result)
        # Should choose endpoint2 as it has lowest load factor (2/3) assuming the unknown endpoint
        # defaulted to 1 instance
        self.assertEqual(result.job_id, "job2-id")

    def test_endpoint_utilization_summary(self):
        """Test EndpointUtilizationSummary calculations"""
        summary = EndpointUtilizationSummary(endpoint_id="test-endpoint", instance_count=2, current_load=4, requests=[])
        self.assertEqual(summary.load_factor, 2)

    def test_estimate_image_load_with_region_count(self):
        """Test _estimate_image_load with region_count=10 and TILE_WORKERS=4 returns 40"""
        # Create a status record with region_count=10
        status_record = self.create_status_record("job1", "endpoint1-model", region_count=10)

        # Call _estimate_image_load
        estimated_load = self.scheduler._estimate_image_load(status_record)

        # With default TILE_WORKERS_PER_INSTANCE=4, expected load is 10 * 4 = 40
        self.assertEqual(estimated_load, 40)

    def test_estimate_image_load_without_region_count(self):
        """Test _estimate_image_load with region_count=None returns default (20 × TILE_WORKERS)"""
        # Create a status record without region_count (None)
        status_record = self.create_status_record("job1", "endpoint1-model", region_count=None)

        # Call _estimate_image_load
        estimated_load = self.scheduler._estimate_image_load(status_record)

        # With default TILE_WORKERS_PER_INSTANCE=4 and default region count of 20,
        # expected load is 20 * 4 = 80
        self.assertEqual(estimated_load, 80)

    def test_check_capacity_available_sufficient_capacity(self):
        """Test _check_capacity_available returns True when sufficient capacity is available"""
        # Create a request with region_count=5 (load = 5 * 4 = 20)
        request = self.create_status_record("job1", "endpoint1-model", region_count=5)

        # Available capacity is 50, which is greater than required load of 20
        available_capacity = 50
        outstanding_requests = [request]

        # Should return True because available_capacity (50) >= image_load (20)
        result = self.scheduler._check_capacity_available(request, available_capacity, outstanding_requests)
        self.assertTrue(result)

    def test_check_capacity_available_insufficient_capacity(self):
        """Test _check_capacity_available returns False when insufficient capacity and other jobs running"""
        # Create requests with region_count
        current_time = int(time.time())
        request1 = self.create_status_record("job1", "endpoint1-model", region_count=10, last_attempt=0)
        request2 = self.create_status_record("job2", "endpoint1-model", region_count=5, last_attempt=current_time - 10)

        # Available capacity is 10, but request1 needs 40 (10 * 4)
        available_capacity = 10
        outstanding_requests = [request1, request2]

        # Should return False because available_capacity (10) < image_load (40)
        # and there are other jobs running (request2 with last_attempt set)
        result = self.scheduler._check_capacity_available(request1, available_capacity, outstanding_requests)
        self.assertFalse(result)

    def test_check_capacity_available_single_image_exception(self):
        """Test _check_capacity_available returns True for single image exception (prevents deadlock)"""
        # Create a request with large region_count=20 (load = 20 * 4 = 80)
        request = self.create_status_record("job1", "endpoint1-model", region_count=20)

        # Available capacity is only 30, which is less than required load of 80
        available_capacity = 30
        outstanding_requests = [request]  # Only this job for this endpoint

        # Should return True due to single image exception
        # This prevents deadlock when a single image exceeds total endpoint capacity
        result = self.scheduler._check_capacity_available(request, available_capacity, outstanding_requests)
        self.assertTrue(result)

    def test_check_capacity_available_single_image_exception_with_variant(self):
        """Test single image exception considers variant when checking for other jobs"""
        # Create requests with different variants
        request1 = self.create_status_record("job1", "endpoint1-model", region_count=20)
        # Initialize model_endpoint_parameters if None
        if request1.request_payload.model_endpoint_parameters is None:
            request1.request_payload.model_endpoint_parameters = {}
        request1.request_payload.model_endpoint_parameters["TargetVariant"] = "variant-1"

        request2 = self.create_status_record("job2", "endpoint1-model", region_count=5)
        # Initialize model_endpoint_parameters if None
        if request2.request_payload.model_endpoint_parameters is None:
            request2.request_payload.model_endpoint_parameters = {}
        request2.request_payload.model_endpoint_parameters["TargetVariant"] = "variant-2"

        # Available capacity is 30, request1 needs 80 (20 * 4)
        available_capacity = 30
        outstanding_requests = [request1, request2]

        # Should return True because request2 is on a different variant
        # So request1 is the only job for variant-1
        result = self.scheduler._check_capacity_available(request1, available_capacity, outstanding_requests)
        self.assertTrue(result)

    def test_check_capacity_available_no_single_image_exception_with_same_variant(self):
        """Test single image exception does NOT apply when other jobs on same variant"""
        # Create requests with same variant
        current_time = int(time.time())
        request1 = self.create_status_record("job1", "endpoint1-model", region_count=20, last_attempt=0)
        # Initialize model_endpoint_parameters if None
        if request1.request_payload.model_endpoint_parameters is None:
            request1.request_payload.model_endpoint_parameters = {}
        request1.request_payload.model_endpoint_parameters["TargetVariant"] = "variant-1"

        request2 = self.create_status_record("job2", "endpoint1-model", region_count=5, last_attempt=current_time - 10)
        # Initialize model_endpoint_parameters if None
        if request2.request_payload.model_endpoint_parameters is None:
            request2.request_payload.model_endpoint_parameters = {}
        request2.request_payload.model_endpoint_parameters["TargetVariant"] = "variant-1"

        # Available capacity is 30, request1 needs 80 (20 * 4)
        available_capacity = 30
        outstanding_requests = [request1, request2]

        # Should return False because request2 is also on variant-1 and is running (last_attempt set)
        # So request1 is NOT the only job for this endpoint/variant
        result = self.scheduler._check_capacity_available(request1, available_capacity, outstanding_requests)
        self.assertFalse(result)

    def test_calculate_available_capacity_with_target_80_percent(self):
        """Test _calculate_available_capacity with max_capacity=100, target=0.8, current_load=50 returns 30"""
        from aws.osml.model_runner.scheduler.endpoint_capacity_estimator import EndpointCapacityEstimator

        # Create a mock capacity estimator that returns max_capacity=100
        mock_capacity_estimator = Mock(spec=EndpointCapacityEstimator)
        mock_capacity_estimator.estimate_capacity.return_value = 100

        # Create scheduler with capacity_target_percentage=0.8
        scheduler = EndpointLoadImageScheduler(
            image_request_queue=self.mock_queue,
            capacity_estimator=mock_capacity_estimator,
            throttling_enabled=True,
            capacity_target_percentage=0.8,
        )

        # Create outstanding requests with total load close to 50
        # 10*4 = 40, 2*4 = 8, total = 48 (close enough)
        # Set last_attempt to recent time so they are considered "currently running"
        current_time = int(time.time())
        request1 = self.create_status_record("job1", "endpoint1-model", region_count=10, last_attempt=current_time - 10)
        request2 = self.create_status_record("job2", "endpoint1-model", region_count=2, last_attempt=current_time - 20)

        # Initialize model_endpoint_parameters to None for both requests
        if request1.request_payload.model_endpoint_parameters is None:
            request1.request_payload.model_endpoint_parameters = {}
        if request2.request_payload.model_endpoint_parameters is None:
            request2.request_payload.model_endpoint_parameters = {}

        outstanding_requests = [request1, request2]

        # Calculate available capacity
        # max_capacity = 100, target = 0.8, so target_capacity = 80
        # current_load = 10*4 + 2*4 = 40 + 8 = 48
        # available = 80 - 48 = 32
        available_capacity, max_capacity, current_utilization = scheduler._calculate_available_capacity(
            "endpoint1-model", None, outstanding_requests
        )

        self.assertEqual(available_capacity, 32)
        self.assertEqual(max_capacity, 100)
        self.assertEqual(current_utilization, 48)

    def test_calculate_available_capacity_with_target_100_percent(self):
        """Test _calculate_available_capacity with max_capacity=50, target=1.0, current_load=30 returns 20"""
        from aws.osml.model_runner.scheduler.endpoint_capacity_estimator import EndpointCapacityEstimator

        # Create a mock capacity estimator that returns max_capacity=50
        mock_capacity_estimator = Mock(spec=EndpointCapacityEstimator)
        mock_capacity_estimator.estimate_capacity.return_value = 50

        # Create scheduler with capacity_target_percentage=1.0 (default)
        scheduler = EndpointLoadImageScheduler(
            image_request_queue=self.mock_queue,
            capacity_estimator=mock_capacity_estimator,
            throttling_enabled=True,
            capacity_target_percentage=1.0,
        )

        # Create outstanding requests with total load of 28 (close to 30)
        # To get load=30 with TILE_WORKERS=4: need 30/4 = 7.5 regions
        # Use 7 regions (28 load) which is close to 30
        # Set last_attempt to recent time so it's considered "currently running"
        current_time = int(time.time())
        request1 = self.create_status_record("job1", "endpoint1-model", region_count=7, last_attempt=current_time - 10)

        # Initialize model_endpoint_parameters
        if request1.request_payload.model_endpoint_parameters is None:
            request1.request_payload.model_endpoint_parameters = {}

        outstanding_requests = [request1]

        # Calculate available capacity
        # max_capacity = 50, target = 1.0, so target_capacity = 50
        # current_load = 7*4 = 28
        # available = 50 - 28 = 22
        available_capacity, max_capacity, current_utilization = scheduler._calculate_available_capacity(
            "endpoint1-model", None, outstanding_requests
        )

        self.assertEqual(available_capacity, 22)
        self.assertEqual(max_capacity, 50)
        self.assertEqual(current_utilization, 28)

    def test_calculate_available_capacity_with_target_120_percent(self):
        """Test _calculate_available_capacity with max_capacity=200, target=1.2, current_load=100 returns 140"""
        from aws.osml.model_runner.scheduler.endpoint_capacity_estimator import EndpointCapacityEstimator

        # Create a mock capacity estimator that returns max_capacity=200
        mock_capacity_estimator = Mock(spec=EndpointCapacityEstimator)
        mock_capacity_estimator.estimate_capacity.return_value = 200

        # Create scheduler with capacity_target_percentage=1.2
        scheduler = EndpointLoadImageScheduler(
            image_request_queue=self.mock_queue,
            capacity_estimator=mock_capacity_estimator,
            throttling_enabled=True,
            capacity_target_percentage=1.2,
        )

        # Create outstanding requests with total load of 100
        # To get load=100 with TILE_WORKERS=4: need 100/4 = 25 regions
        # Set last_attempt to recent time so it's considered "currently running"
        current_time = int(time.time())
        request1 = self.create_status_record("job1", "endpoint1-model", region_count=25, last_attempt=current_time - 10)

        # Initialize model_endpoint_parameters
        if request1.request_payload.model_endpoint_parameters is None:
            request1.request_payload.model_endpoint_parameters = {}

        outstanding_requests = [request1]

        # Calculate available capacity
        # max_capacity = 200, target = 1.2, so target_capacity = 240
        # current_load = 25*4 = 100
        # available = 240 - 100 = 140
        available_capacity, max_capacity, current_utilization = scheduler._calculate_available_capacity(
            "endpoint1-model", None, outstanding_requests
        )

        self.assertEqual(available_capacity, 140)
        self.assertEqual(max_capacity, 200)
        self.assertEqual(current_utilization, 100)

    def test_calculate_available_capacity_filters_by_endpoint_and_variant(self):
        """Test _calculate_available_capacity filters requests by endpoint and variant correctly"""
        from aws.osml.model_runner.scheduler.endpoint_capacity_estimator import EndpointCapacityEstimator

        # Create a mock capacity estimator that returns max_capacity=100
        mock_capacity_estimator = Mock(spec=EndpointCapacityEstimator)
        mock_capacity_estimator.estimate_capacity.return_value = 100

        # Create scheduler with default settings
        scheduler = EndpointLoadImageScheduler(
            image_request_queue=self.mock_queue,
            capacity_estimator=mock_capacity_estimator,
            throttling_enabled=True,
            capacity_target_percentage=1.0,
        )

        # Create requests for different endpoints and variants
        # Set last_attempt to recent time so they are considered "currently running"
        current_time = int(time.time())

        # Request 1: endpoint1-model, variant-1, region_count=10 (load=40)
        request1 = self.create_status_record("job1", "endpoint1-model", region_count=10, last_attempt=current_time - 10)
        if request1.request_payload.model_endpoint_parameters is None:
            request1.request_payload.model_endpoint_parameters = {}
        request1.request_payload.model_endpoint_parameters["TargetVariant"] = "variant-1"

        # Request 2: endpoint1-model, variant-2, region_count=5 (load=20)
        request2 = self.create_status_record("job2", "endpoint1-model", region_count=5, last_attempt=current_time - 20)
        if request2.request_payload.model_endpoint_parameters is None:
            request2.request_payload.model_endpoint_parameters = {}
        request2.request_payload.model_endpoint_parameters["TargetVariant"] = "variant-2"

        # Request 3: endpoint2-model, variant-1, region_count=8 (load=32)
        request3 = self.create_status_record("job3", "endpoint2-model", region_count=8, last_attempt=current_time - 30)
        if request3.request_payload.model_endpoint_parameters is None:
            request3.request_payload.model_endpoint_parameters = {}
        request3.request_payload.model_endpoint_parameters["TargetVariant"] = "variant-1"

        # Request 4: endpoint1-model, variant-1, region_count=3 (load=12)
        request4 = self.create_status_record("job4", "endpoint1-model", region_count=3, last_attempt=current_time - 40)
        if request4.request_payload.model_endpoint_parameters is None:
            request4.request_payload.model_endpoint_parameters = {}
        request4.request_payload.model_endpoint_parameters["TargetVariant"] = "variant-1"

        outstanding_requests = [request1, request2, request3, request4]

        # Calculate available capacity for endpoint1-model, variant-1
        # Should only count request1 (40) and request4 (12) = 52 total load
        # max_capacity = 100, target = 1.0, so target_capacity = 100
        # available = 100 - 52 = 48
        available_capacity, max_capacity, current_utilization = scheduler._calculate_available_capacity(
            "endpoint1-model", "variant-1", outstanding_requests
        )
        self.assertEqual(available_capacity, 48)
        self.assertEqual(max_capacity, 100)
        self.assertEqual(current_utilization, 52)

        # Calculate available capacity for endpoint1-model, variant-2
        # Should only count request2 (20) = 20 total load
        # available = 100 - 20 = 80
        available_capacity, max_capacity, current_utilization = scheduler._calculate_available_capacity(
            "endpoint1-model", "variant-2", outstanding_requests
        )
        self.assertEqual(available_capacity, 80)
        self.assertEqual(max_capacity, 100)
        self.assertEqual(current_utilization, 20)

        # Calculate available capacity for endpoint2-model, variant-1
        # Should only count request3 (32) = 32 total load
        # available = 100 - 32 = 68
        available_capacity, max_capacity, current_utilization = scheduler._calculate_available_capacity(
            "endpoint2-model", "variant-1", outstanding_requests
        )
        self.assertEqual(available_capacity, 68)
        self.assertEqual(max_capacity, 100)
        self.assertEqual(current_utilization, 32)

    def test_get_next_scheduled_request_throttling_disabled(self):
        """Test throttling_enabled=False schedules without capacity checks"""
        from aws.osml.model_runner.scheduler.endpoint_capacity_estimator import EndpointCapacityEstimator

        # Create a mock capacity estimator that would return insufficient capacity
        mock_capacity_estimator = Mock(spec=EndpointCapacityEstimator)
        mock_capacity_estimator.estimate_capacity.return_value = 10  # Very low capacity

        # Create scheduler with throttling disabled
        scheduler = EndpointLoadImageScheduler(
            image_request_queue=self.mock_queue,
            capacity_estimator=mock_capacity_estimator,
            throttling_enabled=False,  # Throttling disabled
            capacity_target_percentage=1.0,
        )

        # Create a request with high load that would exceed capacity
        time_in_past = int(time.time() - 5)
        request = self.create_status_record("job1", "endpoint1-model", request_time=time_in_past, region_count=100)

        self.mock_queue.get_outstanding_requests.return_value = [request]
        self.mock_queue.requested_jobs_table.start_next_attempt.return_value = True

        # Should schedule the request even though capacity is insufficient
        result = scheduler.get_next_scheduled_request()
        self.assertIsNotNone(result)
        self.assertEqual(result.job_id, "job1-id")

        # Verify capacity estimator was NOT called (no capacity checks)
        mock_capacity_estimator.estimate_capacity.assert_not_called()

    def test_get_next_scheduled_request_throttling_enabled_checks_capacity(self):
        """Test throttling_enabled=True checks capacity before scheduling"""
        from aws.osml.model_runner.scheduler.endpoint_capacity_estimator import EndpointCapacityEstimator

        # Create a mock capacity estimator with sufficient capacity
        mock_capacity_estimator = Mock(spec=EndpointCapacityEstimator)
        mock_capacity_estimator.estimate_capacity.return_value = 200  # High capacity

        # Create scheduler with throttling enabled
        scheduler = EndpointLoadImageScheduler(
            image_request_queue=self.mock_queue,
            capacity_estimator=mock_capacity_estimator,
            throttling_enabled=True,  # Throttling enabled
            capacity_target_percentage=1.0,
        )

        # Create a request with moderate load
        time_in_past = int(time.time() - 5)
        request = self.create_status_record("job1", "endpoint1-model", request_time=time_in_past, region_count=10)

        self.mock_queue.get_outstanding_requests.return_value = [request]
        self.mock_queue.requested_jobs_table.start_next_attempt.return_value = True

        # Should schedule the request because capacity is sufficient
        result = scheduler.get_next_scheduled_request()
        self.assertIsNotNone(result)
        self.assertEqual(result.job_id, "job1-id")

        # Verify capacity estimator WAS called (capacity checks performed)
        mock_capacity_estimator.estimate_capacity.assert_called_once_with("endpoint1-model", None)

    def test_get_next_scheduled_request_throttling_blocks_insufficient_capacity(self):
        """Test throttling blocks scheduling when capacity is insufficient"""
        from aws.osml.model_runner.scheduler.endpoint_capacity_estimator import EndpointCapacityEstimator

        # Create a mock capacity estimator with low capacity
        mock_capacity_estimator = Mock(spec=EndpointCapacityEstimator)
        mock_capacity_estimator.estimate_capacity.return_value = 20  # Low capacity

        # Create scheduler with throttling enabled
        scheduler = EndpointLoadImageScheduler(
            image_request_queue=self.mock_queue,
            capacity_estimator=mock_capacity_estimator,
            throttling_enabled=True,
            capacity_target_percentage=1.0,
        )

        # Create two requests: one already running, one waiting
        time_in_past = int(time.time() - 10)
        running_request = self.create_status_record(
            "job1", "endpoint1-model", request_time=time_in_past, region_count=4, last_attempt=time_in_past
        )
        waiting_request = self.create_status_record(
            "job2", "endpoint1-model", request_time=time_in_past + 1, region_count=10
        )

        self.mock_queue.get_outstanding_requests.return_value = [running_request, waiting_request]
        self.mock_queue.requested_jobs_table.start_next_attempt.return_value = True

        # Should NOT schedule waiting_request because capacity is insufficient
        # running_request load = 4 * 4 = 16
        # waiting_request load = 10 * 4 = 40
        # available capacity = 20 - 16 = 4 (less than 40 needed)
        result = scheduler.get_next_scheduled_request()
        self.assertIsNone(result)

        # Verify capacity estimator was called
        mock_capacity_estimator.estimate_capacity.assert_called()

    def test_get_next_scheduled_request_uses_target_variant_from_request(self):
        """Test capacity calculation uses TargetVariant from request (already set by queue)"""
        from aws.osml.model_runner.scheduler.endpoint_capacity_estimator import EndpointCapacityEstimator

        # Create a mock capacity estimator
        mock_capacity_estimator = Mock(spec=EndpointCapacityEstimator)
        mock_capacity_estimator.estimate_capacity.return_value = 100

        # Create scheduler with throttling enabled
        scheduler = EndpointLoadImageScheduler(
            image_request_queue=self.mock_queue,
            capacity_estimator=mock_capacity_estimator,
            throttling_enabled=True,
            capacity_target_percentage=1.0,
        )

        # Create a request with explicit TargetVariant
        time_in_past = int(time.time() - 5)
        request = self.create_status_record("job1", "endpoint1-model", request_time=time_in_past, region_count=5)
        if request.request_payload.model_endpoint_parameters is None:
            request.request_payload.model_endpoint_parameters = {}
        request.request_payload.model_endpoint_parameters["TargetVariant"] = "variant-1"

        self.mock_queue.get_outstanding_requests.return_value = [request]
        self.mock_queue.requested_jobs_table.start_next_attempt.return_value = True

        # Schedule the request
        result = scheduler.get_next_scheduled_request()
        self.assertIsNotNone(result)

        # Verify capacity estimator was called with the specific variant
        mock_capacity_estimator.estimate_capacity.assert_called_once_with("endpoint1-model", "variant-1")

    def test_get_next_scheduled_request_capacity_for_specific_variant(self):
        """Test capacity calculation for specific variant (not all variants)"""
        from aws.osml.model_runner.scheduler.endpoint_capacity_estimator import EndpointCapacityEstimator

        # Create a mock capacity estimator that returns different capacities per variant
        mock_capacity_estimator = Mock(spec=EndpointCapacityEstimator)

        def estimate_capacity_side_effect(endpoint_name, variant_name):
            if variant_name == "variant-1":
                return 50  # variant-1 has capacity 50
            elif variant_name == "variant-2":
                return 100  # variant-2 has capacity 100
            else:
                return 150  # all variants combined

        mock_capacity_estimator.estimate_capacity.side_effect = estimate_capacity_side_effect

        # Create scheduler with throttling enabled
        scheduler = EndpointLoadImageScheduler(
            image_request_queue=self.mock_queue,
            capacity_estimator=mock_capacity_estimator,
            throttling_enabled=True,
            capacity_target_percentage=1.0,
        )

        # Create requests for different variants
        time_in_past = int(time.time() - 10)

        # Request 1: variant-1, region_count=10 (load=40)
        request1 = self.create_status_record("job1", "endpoint1-model", request_time=time_in_past, region_count=10)
        if request1.request_payload.model_endpoint_parameters is None:
            request1.request_payload.model_endpoint_parameters = {}
        request1.request_payload.model_endpoint_parameters["TargetVariant"] = "variant-1"

        # Request 2: variant-2, region_count=20 (load=80)
        request2 = self.create_status_record("job2", "endpoint1-model", request_time=time_in_past + 1, region_count=20)
        if request2.request_payload.model_endpoint_parameters is None:
            request2.request_payload.model_endpoint_parameters = {}
        request2.request_payload.model_endpoint_parameters["TargetVariant"] = "variant-2"

        self.mock_queue.get_outstanding_requests.return_value = [request1, request2]
        self.mock_queue.requested_jobs_table.start_next_attempt.return_value = True

        # Schedule request1 (variant-1)
        # variant-1 capacity = 50, request1 load = 40, available = 50 - 0 = 50 (sufficient)
        result = scheduler.get_next_scheduled_request()
        self.assertIsNotNone(result)
        self.assertEqual(result.job_id, "job1-id")

        # Verify capacity was calculated for variant-1 specifically
        mock_capacity_estimator.estimate_capacity.assert_called_with("endpoint1-model", "variant-1")

    def test_get_next_scheduled_request_no_capacity_estimator_uses_existing_logic(self):
        """Test no capacity_estimator provided uses existing logic (no capacity checks)"""
        # Create scheduler without capacity estimator
        scheduler = EndpointLoadImageScheduler(
            image_request_queue=self.mock_queue,
            capacity_estimator=None,  # No capacity estimator
            throttling_enabled=True,  # Throttling enabled but no estimator
            capacity_target_percentage=1.0,
        )

        # Create a request
        time_in_past = int(time.time() - 5)
        request = self.create_status_record("job1", "endpoint1-model", request_time=time_in_past, region_count=100)

        self.mock_queue.get_outstanding_requests.return_value = [request]
        self.mock_queue.requested_jobs_table.start_next_attempt.return_value = True

        # Should schedule the request without capacity checks
        result = scheduler.get_next_scheduled_request()
        self.assertIsNotNone(result)
        self.assertEqual(result.job_id, "job1-id")

    def test_get_next_scheduled_request_logs_throttling_decisions(self):
        """Test logging of throttling decisions"""
        from aws.osml.model_runner.scheduler.endpoint_capacity_estimator import EndpointCapacityEstimator

        # Create a mock capacity estimator with low capacity
        mock_capacity_estimator = Mock(spec=EndpointCapacityEstimator)
        mock_capacity_estimator.estimate_capacity.return_value = 20

        # Create scheduler with throttling enabled
        scheduler = EndpointLoadImageScheduler(
            image_request_queue=self.mock_queue,
            capacity_estimator=mock_capacity_estimator,
            throttling_enabled=True,
            capacity_target_percentage=0.8,
        )

        # Create two requests: one running, one waiting
        time_in_past = int(time.time() - 10)
        running_request = self.create_status_record(
            "job1", "endpoint1-model", request_time=time_in_past, region_count=3, last_attempt=time_in_past
        )
        waiting_request = self.create_status_record(
            "job2", "endpoint1-model", request_time=time_in_past + 1, region_count=10
        )

        self.mock_queue.get_outstanding_requests.return_value = [running_request, waiting_request]
        self.mock_queue.requested_jobs_table.start_next_attempt.return_value = True

        # Capture log output
        with self.assertLogs(logger="aws.osml.model_runner.scheduler.endpoint_load_image_scheduler", level="INFO") as cm:
            result = scheduler.get_next_scheduled_request()
            self.assertIsNone(result)

            # Verify info log was emitted for throttling (changed from WARNING to INFO level)
            self.assertTrue(any("Throttling job job2-id due to insufficient capacity" in message for message in cm.output))
            self.assertTrue(any("Required load:" in message for message in cm.output))
            self.assertTrue(any("Available capacity:" in message for message in cm.output))

    def test_get_next_scheduled_request_logs_successful_scheduling(self):
        """Test logging of successful scheduling with capacity details"""
        from aws.osml.model_runner.scheduler.endpoint_capacity_estimator import EndpointCapacityEstimator

        # Create a mock capacity estimator with sufficient capacity
        mock_capacity_estimator = Mock(spec=EndpointCapacityEstimator)
        mock_capacity_estimator.estimate_capacity.return_value = 200

        # Create scheduler with throttling enabled
        scheduler = EndpointLoadImageScheduler(
            image_request_queue=self.mock_queue,
            capacity_estimator=mock_capacity_estimator,
            throttling_enabled=True,
            capacity_target_percentage=0.9,
        )

        # Create a request
        time_in_past = int(time.time() - 5)
        request = self.create_status_record("job1", "endpoint1-model", request_time=time_in_past, region_count=10)

        self.mock_queue.get_outstanding_requests.return_value = [request]
        self.mock_queue.requested_jobs_table.start_next_attempt.return_value = True

        # Capture log output
        with self.assertLogs(logger="aws.osml.model_runner.scheduler.endpoint_load_image_scheduler", level="INFO") as cm:
            result = scheduler.get_next_scheduled_request()
            self.assertIsNotNone(result)

            # Verify info log was emitted with capacity details
            self.assertTrue(any("Scheduling job job1-id with sufficient capacity" in message for message in cm.output))
            self.assertTrue(any("Required load:" in message for message in cm.output))
            self.assertTrue(any("Available capacity:" in message for message in cm.output))
            self.assertTrue(any("Target percentage: 90.0%" in message for message in cm.output))

    def test_check_capacity_available_single_image_exception_ignores_not_running_jobs(self):
        """Test single image exception only considers currently running jobs (bug fix)"""
        # Create multiple new requests (last_attempt=0) that exceed endpoint capacity
        request1 = self.create_status_record("job1", "endpoint1-model", region_count=20, last_attempt=0)
        request2 = self.create_status_record("job2", "endpoint1-model", region_count=20, last_attempt=0)

        # Available capacity is only 30, which is less than required load of 80
        available_capacity = 30
        outstanding_requests = [request1, request2]

        # Should return True due to single image exception
        # Even though request2 exists, it's not running (last_attempt=0)
        # So request1 is free to start even though it exceeds available capacity
        result = self.scheduler._check_capacity_available(request1, available_capacity, outstanding_requests)
        self.assertTrue(result)

    def test_check_capacity_available_single_image_exception_considers_running_jobs(self):
        """Test single image exception correctly identifies running jobs"""
        # Create requests where one is running
        current_time = int(time.time())

        # Request 1: Large image that needs 80 tiles (20 * 4)
        request1 = self.create_status_record("job1", "endpoint1-model", region_count=20, last_attempt=0)

        # Request 2: Currently running (last_attempt is recent)
        request2 = self.create_status_record("job2", "endpoint1-model", region_count=5, last_attempt=current_time - 10)

        # Available capacity is only 30, which is less than required load of 80
        available_capacity = 30
        outstanding_requests = [request1, request2]

        # Should return False because request2 is running
        # So request1 is NOT the only job for this endpoint
        result = self.scheduler._check_capacity_available(request1, available_capacity, outstanding_requests)
        self.assertFalse(result)

    def test_get_running_jobs_for_endpoint_variant_filters_correctly(self):
        """Test _get_running_jobs_for_endpoint_variant filters by running status"""
        current_time = int(time.time())

        # Create various requests with different states
        # Running: last_attempt is recent
        running_request = self.create_status_record(
            "job1", "endpoint1-model", region_count=5, last_attempt=current_time - 10
        )

        # Not started: last_attempt=0
        not_started_request = self.create_status_record("job2", "endpoint1-model", region_count=5, last_attempt=0)

        # Timed out: last_attempt is too old (beyond retry_time)
        timed_out_request = self.create_status_record(
            "job3", "endpoint1-model", region_count=5, last_attempt=current_time - 700
        )

        # Different endpoint: should be excluded
        different_endpoint_request = self.create_status_record(
            "job4", "endpoint2-model", region_count=5, last_attempt=current_time - 10
        )

        outstanding_requests = [running_request, not_started_request, timed_out_request, different_endpoint_request]

        # Get running jobs for endpoint1-model
        running_jobs = self.scheduler._get_running_jobs_for_endpoint_variant("endpoint1-model", None, outstanding_requests)

        # Should only include running_request
        self.assertEqual(len(running_jobs), 1)
        self.assertEqual(running_jobs[0].job_id, "job1-id")

    def test_get_running_jobs_for_endpoint_variant_excludes_job_id(self):
        """Test _get_running_jobs_for_endpoint_variant excludes specified job_id"""
        current_time = int(time.time())

        # Create running requests
        request1 = self.create_status_record("job1", "endpoint1-model", region_count=5, last_attempt=current_time - 10)
        request2 = self.create_status_record("job2", "endpoint1-model", region_count=5, last_attempt=current_time - 20)

        outstanding_requests = [request1, request2]

        # Get running jobs excluding job1
        running_jobs = self.scheduler._get_running_jobs_for_endpoint_variant(
            "endpoint1-model", None, outstanding_requests, exclude_job_id="job1-id"
        )

        # Should only include request2
        self.assertEqual(len(running_jobs), 1)
        self.assertEqual(running_jobs[0].job_id, "job2-id")

    def test_get_running_jobs_for_endpoint_variant_filters_by_variant(self):
        """Test _get_running_jobs_for_endpoint_variant filters by variant correctly"""
        current_time = int(time.time())

        # Create running requests with different variants
        request1 = self.create_status_record("job1", "endpoint1-model", region_count=5, last_attempt=current_time - 10)
        if request1.request_payload.model_endpoint_parameters is None:
            request1.request_payload.model_endpoint_parameters = {}
        request1.request_payload.model_endpoint_parameters["TargetVariant"] = "variant-1"

        request2 = self.create_status_record("job2", "endpoint1-model", region_count=5, last_attempt=current_time - 20)
        if request2.request_payload.model_endpoint_parameters is None:
            request2.request_payload.model_endpoint_parameters = {}
        request2.request_payload.model_endpoint_parameters["TargetVariant"] = "variant-2"

        outstanding_requests = [request1, request2]

        # Get running jobs for variant-1
        running_jobs = self.scheduler._get_running_jobs_for_endpoint_variant(
            "endpoint1-model", "variant-1", outstanding_requests
        )

        # Should only include request1
        self.assertEqual(len(running_jobs), 1)
        self.assertEqual(running_jobs[0].job_id, "job1-id")


if __name__ == "__main__":
    unittest.main()


@mock_aws
class TestEndpointLoadImageSchedulerMetricsEmission(unittest.TestCase):
    """Test cases for metrics emission in EndpointLoadImageScheduler"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.sagemaker = boto3.client("sagemaker", region_name="us-west-2")

        # Create mock endpoints in SageMaker
        self.endpoints = {
            "endpoint1": {"InstanceCount": 2},
            "endpoint2": {"InstanceCount": 1},
        }

        for endpoint_id, config in self.endpoints.items():
            self.sagemaker.create_model(
                ModelName=f"{endpoint_id}-model", PrimaryContainer={"Image": "test-model-container-image"}
            )
            self.sagemaker.create_endpoint_config(
                EndpointConfigName=f"{endpoint_id}-config",
                ProductionVariants=[
                    {
                        "InstanceType": "ml.m5.xlarge",
                        "InitialInstanceCount": config["InstanceCount"],
                        "VariantName": "AllTraffic",
                        "ModelName": f"{endpoint_id}-model",
                    }
                ],
            )
            self.sagemaker.create_endpoint(EndpointName=f"{endpoint_id}-model", EndpointConfigName=f"{endpoint_id}-config")

        # Create mock BufferedImageRequestQueue
        self.mock_queue = Mock()
        self.mock_queue.retry_time = 600

    def create_mock_metrics_logger(self):
        """Create a mock MetricsLogger that passes isinstance checks"""
        from unittest.mock import MagicMock

        from aws_embedded_metrics.logger.metrics_logger import MetricsLogger

        mock_metrics = MagicMock(spec=MetricsLogger)
        mock_metrics.put_dimensions = Mock()
        mock_metrics.put_metric = Mock()
        return mock_metrics

    def create_sample_image_request(self, job_name: str = "test-job", model_name: str = "endpoint1-model") -> ImageRequest:
        """Helper method to create a sample ImageRequest"""
        return ImageRequest.from_external_message(
            {
                "jobName": job_name,
                "jobId": f"{job_name}-id",
                "imageUrls": ["s3://test-bucket/test.nitf"],
                "outputs": [
                    {"type": "S3", "bucket": "test-bucket", "prefix": "results"},
                    {"type": "Kinesis", "stream": "test-stream", "batchSize": 1000},
                ],
                "imageProcessor": {"name": model_name, "type": "SM_ENDPOINT"},
                "imageProcessorTileSize": 2048,
                "imageProcessorTileOverlap": 50,
            }
        )

    def create_status_record(
        self,
        job_name: str,
        model_name: str,
        request_time: Optional[int] = None,
        last_attempt: Optional[int] = None,
        num_attempts: Optional[int] = None,
        regions_complete: Optional[List[str]] = None,
        region_count: Optional[int] = None,
    ) -> ImageRequestStatusRecord:
        """Helper method to create a status record"""
        image_request = self.create_sample_image_request(job_name, model_name)
        image_status_record = ImageRequestStatusRecord.new_from_request(image_request)
        if request_time is not None:
            image_status_record.request_time = request_time
        if last_attempt is not None:
            image_status_record.last_attempt = last_attempt
        if num_attempts is not None:
            image_status_record.num_attempts = num_attempts
        if regions_complete is not None:
            image_status_record.regions_complete = regions_complete
        if region_count is not None:
            image_status_record.region_count = region_count
        return image_status_record

    def test_throttles_metric_increments_when_throttling_occurs(self):
        """Test Throttles metric (Operation=Scheduling, ModelName=<endpoint>) increments when throttling occurs"""
        from aws_embedded_metrics.unit import Unit

        from aws.osml.model_runner.app_config import MetricLabels

        scheduler = EndpointLoadImageScheduler(
            image_request_queue=self.mock_queue,
            throttling_enabled=True,
            capacity_target_percentage=1.0,
        )

        mock_metrics = self.create_mock_metrics_logger()

        # Call the underlying method without the decorator
        scheduler._emit_throttle_metric.__wrapped__(scheduler, "endpoint1-model", metrics=mock_metrics)

        mock_metrics.put_dimensions.assert_called_once_with(
            {
                MetricLabels.OPERATION_DIMENSION: MetricLabels.SCHEDULING_OPERATION,
                MetricLabels.MODEL_NAME_DIMENSION: "endpoint1-model",
            }
        )
        mock_metrics.put_metric.assert_called_once_with(MetricLabels.THROTTLES, 1, str(Unit.COUNT.value))

    def test_utilization_metric_shows_correct_percentage(self):
        """Test Utilization metric (Operation=Scheduling, ModelName=<endpoint>) shows correct percentage (0-100%)"""
        from aws_embedded_metrics.unit import Unit

        from aws.osml.model_runner.app_config import MetricLabels

        scheduler = EndpointLoadImageScheduler(
            image_request_queue=self.mock_queue,
            throttling_enabled=True,
            capacity_target_percentage=1.0,
        )

        mock_metrics = self.create_mock_metrics_logger()

        # Test 50% utilization
        scheduler._emit_utilization_metric.__wrapped__(
            scheduler, "endpoint1-model", max_capacity=100, current_utilization=50, metrics=mock_metrics
        )
        mock_metrics.put_metric.assert_called_with(MetricLabels.UTILIZATION, 50.0, str(Unit.PERCENT.value))

        mock_metrics.reset_mock()

        # Test 100% utilization
        scheduler._emit_utilization_metric.__wrapped__(
            scheduler, "endpoint1-model", max_capacity=100, current_utilization=100, metrics=mock_metrics
        )
        mock_metrics.put_metric.assert_called_with(MetricLabels.UTILIZATION, 100.0, str(Unit.PERCENT.value))

    def test_utilization_metric_clamps_to_valid_range(self):
        """Test Utilization metric clamps values to 0-100% range"""
        from aws_embedded_metrics.unit import Unit

        from aws.osml.model_runner.app_config import MetricLabels

        scheduler = EndpointLoadImageScheduler(
            image_request_queue=self.mock_queue,
            throttling_enabled=True,
            capacity_target_percentage=1.0,
        )

        mock_metrics = self.create_mock_metrics_logger()

        # Over 100% should be clamped to 100%
        scheduler._emit_utilization_metric.__wrapped__(
            scheduler, "endpoint1-model", max_capacity=100, current_utilization=150, metrics=mock_metrics
        )
        mock_metrics.put_metric.assert_called_with(MetricLabels.UTILIZATION, 100.0, str(Unit.PERCENT.value))

    def test_utilization_metric_handles_zero_max_capacity(self):
        """Test Utilization metric handles zero max_capacity gracefully"""
        scheduler = EndpointLoadImageScheduler(
            image_request_queue=self.mock_queue,
            throttling_enabled=True,
            capacity_target_percentage=1.0,
        )

        mock_metrics = self.create_mock_metrics_logger()

        # Zero max_capacity should not emit metric
        scheduler._emit_utilization_metric.__wrapped__(
            scheduler, "endpoint1-model", max_capacity=0, current_utilization=50, metrics=mock_metrics
        )
        mock_metrics.put_metric.assert_not_called()

    def test_duration_metric_records_scheduling_latency(self):
        """Test Duration metric (Operation=Scheduling) records scheduling decision latency"""
        from aws_embedded_metrics.unit import Unit

        from aws.osml.model_runner.app_config import MetricLabels
        from aws.osml.model_runner.scheduler.endpoint_capacity_estimator import EndpointCapacityEstimator

        mock_capacity_estimator = Mock(spec=EndpointCapacityEstimator)
        mock_capacity_estimator.estimate_capacity.return_value = 200

        scheduler = EndpointLoadImageScheduler(
            image_request_queue=self.mock_queue,
            capacity_estimator=mock_capacity_estimator,
            throttling_enabled=True,
            capacity_target_percentage=1.0,
        )

        time_in_past = int(time.time() - 5)
        request = self.create_status_record("job1", "endpoint1-model", request_time=time_in_past, region_count=5)

        self.mock_queue.get_outstanding_requests.return_value = [request]
        self.mock_queue.requested_jobs_table.start_next_attempt.return_value = True

        mock_metrics = self.create_mock_metrics_logger()

        # Call the helper method directly to test metric emission
        scheduler._emit_scheduling_metrics.__wrapped__(scheduler, duration_ms=100.0, metrics=mock_metrics)

        duration_calls = [
            call
            for call in mock_metrics.put_metric.call_args_list
            if call[0][0] == MetricLabels.DURATION and call[0][2] == str(Unit.MILLISECONDS.value)
        ]
        self.assertEqual(len(duration_calls), 1)
        self.assertGreaterEqual(duration_calls[0][0][1], 0)

    def test_invocations_metric_increments_when_evaluating_images(self):
        """Test Invocations metric (Operation=Scheduling) increments when evaluating images"""
        from aws_embedded_metrics.unit import Unit

        from aws.osml.model_runner.app_config import MetricLabels
        from aws.osml.model_runner.scheduler.endpoint_capacity_estimator import EndpointCapacityEstimator

        mock_capacity_estimator = Mock(spec=EndpointCapacityEstimator)
        mock_capacity_estimator.estimate_capacity.return_value = 200

        scheduler = EndpointLoadImageScheduler(
            image_request_queue=self.mock_queue,
            capacity_estimator=mock_capacity_estimator,
            throttling_enabled=True,
            capacity_target_percentage=1.0,
        )

        time_in_past = int(time.time() - 5)
        request = self.create_status_record("job1", "endpoint1-model", request_time=time_in_past, region_count=5)

        self.mock_queue.get_outstanding_requests.return_value = [request]
        self.mock_queue.requested_jobs_table.start_next_attempt.return_value = True

        mock_metrics = self.create_mock_metrics_logger()

        # Call the helper method directly to test metric emission
        scheduler._emit_scheduling_metrics.__wrapped__(scheduler, duration_ms=100.0, metrics=mock_metrics)

        invocations_calls = [
            call
            for call in mock_metrics.put_metric.call_args_list
            if call[0][0] == MetricLabels.INVOCATIONS and call[0][2] == str(Unit.COUNT.value)
        ]
        self.assertEqual(len(invocations_calls), 1)
        self.assertEqual(invocations_calls[0][0][1], 1)

    def test_invocations_metric_not_emitted_when_no_requests(self):
        """Test Invocations metric is not emitted when there are no outstanding requests"""
        scheduler = EndpointLoadImageScheduler(
            image_request_queue=self.mock_queue,
            throttling_enabled=True,
            capacity_target_percentage=1.0,
        )

        self.mock_queue.get_outstanding_requests.return_value = []

        # Mock _emit_scheduling_metrics to verify it's not called when no requests
        with patch.object(scheduler, "_emit_scheduling_metrics") as mock_emit:
            scheduler.get_next_scheduled_request()
            mock_emit.assert_not_called()

    def test_metrics_follow_standard_modelrunner_pattern(self):
        """Test metrics follow standard ModelRunner pattern with correct namespace and dimensions"""
        from aws_embedded_metrics.unit import Unit

        from aws.osml.model_runner.app_config import MetricLabels

        scheduler = EndpointLoadImageScheduler(
            image_request_queue=self.mock_queue,
            throttling_enabled=True,
            capacity_target_percentage=1.0,
        )

        mock_metrics = self.create_mock_metrics_logger()

        scheduler._emit_throttle_metric.__wrapped__(scheduler, "test-endpoint", metrics=mock_metrics)

        mock_metrics.put_dimensions.assert_called_with(
            {
                MetricLabels.OPERATION_DIMENSION: MetricLabels.SCHEDULING_OPERATION,
                MetricLabels.MODEL_NAME_DIMENSION: "test-endpoint",
            }
        )
        mock_metrics.put_metric.assert_called_with(MetricLabels.THROTTLES, 1, str(Unit.COUNT.value))

    def test_throttle_metric_handles_none_metrics_logger(self):
        """Test _emit_throttle_metric handles None metrics logger gracefully"""
        scheduler = EndpointLoadImageScheduler(
            image_request_queue=self.mock_queue,
            throttling_enabled=True,
            capacity_target_percentage=1.0,
        )

        # Should not raise exception when metrics is None
        scheduler._emit_throttle_metric.__wrapped__(scheduler, "test-endpoint", metrics=None)

    def test_utilization_metric_handles_none_metrics_logger(self):
        """Test _emit_utilization_metric handles None metrics logger gracefully"""
        scheduler = EndpointLoadImageScheduler(
            image_request_queue=self.mock_queue,
            throttling_enabled=True,
            capacity_target_percentage=1.0,
        )

        # Should not raise exception when metrics is None
        scheduler._emit_utilization_metric.__wrapped__(
            scheduler, "test-endpoint", max_capacity=100, current_utilization=50, metrics=None
        )
