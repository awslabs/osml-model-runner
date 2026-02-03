#  Copyright 2025-2026 Amazon.com, Inc. or its affiliates.

import time
from typing import List, Optional
from unittest.mock import MagicMock, Mock

import boto3
import pytest
from moto import mock_aws

from aws.osml.model_runner.api import ImageRequest
from aws.osml.model_runner.database import ImageRequestStatusRecord
from aws.osml.model_runner.scheduler.endpoint_load_image_scheduler import (
    EndpointLoadImageScheduler,
    EndpointUtilizationSummary,
)


def create_sample_image_request(job_name: str = "test-job", model_name: str = "endpoint1-model") -> ImageRequest:
    """Helper function to create a sample ImageRequest"""
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
    job_name: str,
    model_name: str,
    request_time: Optional[int] = None,
    last_attempt: Optional[int] = None,
    num_attempts: Optional[int] = None,
    regions_complete: Optional[List[str]] = None,
    region_count: Optional[int] = None,
) -> ImageRequestStatusRecord:
    """Helper function to create a status record"""
    image_request = create_sample_image_request(job_name, model_name)
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


def create_mock_metrics_logger():
    """Create a mock MetricsLogger that passes isinstance checks"""
    from aws_embedded_metrics.logger.metrics_logger import MetricsLogger

    mock_metrics = MagicMock(spec=MetricsLogger)
    mock_metrics.put_dimensions = Mock()
    mock_metrics.put_metric = Mock()
    return mock_metrics


@pytest.fixture
def scheduler_setup():
    """Set up test fixtures for EndpointLoadImageScheduler tests."""
    with mock_aws():
        # Set up mock AWS resources
        sagemaker = boto3.client("sagemaker")

        # Create mock endpoints in SageMaker
        endpoints = {
            "endpoint1": {"InstanceCount": 2},
            "endpoint2": {"InstanceCount": 1},
            "endpoint3": {"InstanceCount": 3},
        }

        for endpoint_id, config in endpoints.items():
            sagemaker.create_model(
                ModelName=f"{endpoint_id}-model", PrimaryContainer={"Image": "test-model-container-image"}
            )
            sagemaker.create_endpoint_config(
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

            sagemaker.create_endpoint(EndpointName=f"{endpoint_id}-model", EndpointConfigName=f"{endpoint_id}-config")

        # Create mock BufferedImageRequestQueue
        mock_queue = Mock()
        mock_queue.retry_time = 600

        # Create scheduler
        scheduler = EndpointLoadImageScheduler(image_request_queue=mock_queue)

        yield scheduler, mock_queue, sagemaker, endpoints


@pytest.fixture
def scheduler_metrics_setup():
    """Set up test fixtures for metrics emission tests."""
    with mock_aws():
        sagemaker = boto3.client("sagemaker", region_name="us-west-2")

        # Create mock endpoints in SageMaker
        endpoints = {
            "endpoint1": {"InstanceCount": 2},
            "endpoint2": {"InstanceCount": 1},
        }

        for endpoint_id, config in endpoints.items():
            sagemaker.create_model(
                ModelName=f"{endpoint_id}-model", PrimaryContainer={"Image": "test-model-container-image"}
            )
            sagemaker.create_endpoint_config(
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
            sagemaker.create_endpoint(EndpointName=f"{endpoint_id}-model", EndpointConfigName=f"{endpoint_id}-config")

        # Create mock BufferedImageRequestQueue
        mock_queue = Mock()
        mock_queue.retry_time = 600

        yield sagemaker, mock_queue, endpoints


def test_get_next_scheduled_request_no_requests(scheduler_setup):
    """Test scheduling when there are no requests"""
    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup
    mock_queue.get_outstanding_requests.return_value = []
    result = scheduler.get_next_scheduled_request()
    assert result is None


def test_get_next_scheduled_request_single_endpoint(scheduler_setup):
    """Test scheduling with requests for a single endpoint"""
    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup
    time_in_past = int(time.time() - 5)
    status_records = [
        create_status_record("job1", "endpoint1-model", request_time=time_in_past),
        create_status_record("job2", "endpoint1-model", request_time=time_in_past + 1),
    ]

    mock_queue.get_outstanding_requests.return_value = status_records
    mock_queue.requested_jobs_table.start_next_attempt.return_value = True

    result = scheduler.get_next_scheduled_request()
    assert result is not None
    assert result.job_id == "job1-id"


def test_get_next_scheduled_request_multiple_endpoints(scheduler_setup):
    """Test scheduling with requests across multiple endpoints"""
    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup
    time_in_past = int(time.time() - 10)
    status_records = [
        create_status_record("job1", "endpoint1-model", request_time=time_in_past + 1),
        create_status_record("job2", "endpoint2-model", request_time=time_in_past),
        create_status_record("job3", "endpoint3-model", request_time=time_in_past + 2),
    ]

    mock_queue.get_outstanding_requests.return_value = status_records
    mock_queue.requested_jobs_table.start_next_attempt.return_value = True

    result = scheduler.get_next_scheduled_request()
    assert result is not None
    # Should choose job2 because all 3 endpoints have no load and it was submitted first
    assert result.job_id == "job2-id"


def test_get_next_scheduled_request_with_existing_load(scheduler_setup):
    """Test scheduling considering existing endpoint load"""
    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup
    status_records = [
        # endpoint1 (2 instances) has 3 running jobs
        create_status_record("job1", "endpoint1-model", region_count=1),
        create_status_record("job2", "endpoint1-model", region_count=1),
        create_status_record("job3", "endpoint1-model", region_count=1),
        # endpoint2 (1 instance) has 1 running job
        create_status_record("job4", "endpoint2-model", region_count=1),
        # endpoint3 (3 instances) has no running jobs
        create_status_record("job5", "endpoint3-model"),
    ]

    mock_queue.get_outstanding_requests.return_value = status_records
    mock_queue.requested_jobs_table.start_next_attempt.return_value = True

    result = scheduler.get_next_scheduled_request()
    assert result is not None
    # Should choose endpoint3 as it has lowest load factor (0/3)
    assert result.job_id == "job5-id"


def test_get_next_scheduled_request_start_attempt_failure(scheduler_setup):
    """Test scheduling when start_next_attempt fails"""
    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup
    status_records = [create_status_record("job1", "endpoint1-model")]

    mock_queue.get_outstanding_requests.return_value = status_records
    mock_queue.requested_jobs_table.start_next_attempt.return_value = False

    result = scheduler.get_next_scheduled_request()
    assert result is None


def test_get_next_scheduled_request_sagemaker_error(scheduler_setup):
    """Test handling of SageMaker API errors"""
    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup
    status_records = [
        create_status_record("job1", "nonexistent-endpoint", region_count=1),
        create_status_record("job2", "endpoint3-model", region_count=2),
    ]

    mock_queue.get_outstanding_requests.return_value = status_records
    mock_queue.requested_jobs_table.start_next_attempt.return_value = True

    result = scheduler.get_next_scheduled_request()
    assert result is not None
    # Should choose endpoint2 as it has lowest load factor (2/3) assuming the unknown endpoint
    # defaulted to 1 instance
    assert result.job_id == "job2-id"


def test_endpoint_utilization_summary():
    """Test EndpointUtilizationSummary calculations"""
    summary = EndpointUtilizationSummary(endpoint_id="test-endpoint", instance_count=2, current_load=4, requests=[])
    assert summary.load_factor == 2


def test_estimate_image_load_with_region_count(scheduler_setup):
    """Test _estimate_image_load with region_count=10 and TILE_WORKERS=4 returns 40"""
    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup
    # Create a status record with region_count=10
    status_record = create_status_record("job1", "endpoint1-model", region_count=10)

    # Call _estimate_image_load
    estimated_load = scheduler._estimate_image_load(status_record)

    # With default TILE_WORKERS_PER_INSTANCE=4, expected load is 10 * 4 = 40
    assert estimated_load == 40


def test_estimate_image_load_without_region_count(scheduler_setup):
    """Test _estimate_image_load with region_count=None returns default (20 × TILE_WORKERS)"""
    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup
    # Create a status record without region_count (None)
    status_record = create_status_record("job1", "endpoint1-model", region_count=None)

    # Call _estimate_image_load
    estimated_load = scheduler._estimate_image_load(status_record)

    # With default TILE_WORKERS_PER_INSTANCE=4 and default region count of 20,
    # expected load is 20 * 4 = 80
    assert estimated_load == 80


def test_check_capacity_available_sufficient_capacity(scheduler_setup):
    """Test _check_capacity_available returns True when sufficient capacity is available"""
    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup
    # Create a request with region_count=5 (load = 5 * 4 = 20)
    request = create_status_record("job1", "endpoint1-model", region_count=5)

    # Available capacity is 50, which is greater than required load of 20
    available_capacity = 50
    outstanding_requests = [request]

    # Should return True because available_capacity (50) >= image_load (20)
    result = scheduler._check_capacity_available(request, available_capacity, outstanding_requests)
    assert result is True


def test_check_capacity_available_insufficient_capacity(scheduler_setup):
    """Test _check_capacity_available returns False when insufficient capacity and other jobs running"""
    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup
    # Create requests with region_count
    current_time = int(time.time())
    request1 = create_status_record("job1", "endpoint1-model", region_count=10, last_attempt=0)
    request2 = create_status_record("job2", "endpoint1-model", region_count=5, last_attempt=current_time - 10)

    # Available capacity is 10, but request1 needs 40 (10 * 4)
    available_capacity = 10
    outstanding_requests = [request1, request2]

    # Should return False because available_capacity (10) < image_load (40)
    # and there are other jobs running (request2 with last_attempt set)
    result = scheduler._check_capacity_available(request1, available_capacity, outstanding_requests)
    assert result is False


def test_check_capacity_available_single_image_exception(scheduler_setup):
    """Test _check_capacity_available returns True for single image exception (prevents deadlock)"""
    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup
    # Create a request with large region_count=20 (load = 20 * 4 = 80)
    request = create_status_record("job1", "endpoint1-model", region_count=20)

    # Available capacity is only 30, which is less than required load of 80
    available_capacity = 30
    outstanding_requests = [request]  # Only this job for this endpoint

    # Should return True due to single image exception
    # This prevents deadlock when a single image exceeds total endpoint capacity
    result = scheduler._check_capacity_available(request, available_capacity, outstanding_requests)
    assert result is True


def test_check_capacity_available_single_image_exception_with_variant(scheduler_setup):
    """Test single image exception considers variant when checking for other jobs"""
    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup
    # Create requests with different variants
    request1 = create_status_record("job1", "endpoint1-model", region_count=20)
    # Initialize model_endpoint_parameters if None
    if request1.request_payload.model_endpoint_parameters is None:
        request1.request_payload.model_endpoint_parameters = {}
    request1.request_payload.model_endpoint_parameters["TargetVariant"] = "variant-1"

    request2 = create_status_record("job2", "endpoint1-model", region_count=5)
    # Initialize model_endpoint_parameters if None
    if request2.request_payload.model_endpoint_parameters is None:
        request2.request_payload.model_endpoint_parameters = {}
    request2.request_payload.model_endpoint_parameters["TargetVariant"] = "variant-2"

    # Available capacity is 30, request1 needs 80 (20 * 4)
    available_capacity = 30
    outstanding_requests = [request1, request2]

    # Should return True because request2 is on a different variant
    # So request1 is the only job for variant-1
    result = scheduler._check_capacity_available(request1, available_capacity, outstanding_requests)
    assert result is True


def test_check_capacity_available_no_single_image_exception_with_same_variant(scheduler_setup):
    """Test single image exception does NOT apply when other jobs on same variant"""
    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup
    # Create requests with same variant
    current_time = int(time.time())
    request1 = create_status_record("job1", "endpoint1-model", region_count=20, last_attempt=0)
    # Initialize model_endpoint_parameters if None
    if request1.request_payload.model_endpoint_parameters is None:
        request1.request_payload.model_endpoint_parameters = {}
    request1.request_payload.model_endpoint_parameters["TargetVariant"] = "variant-1"

    request2 = create_status_record("job2", "endpoint1-model", region_count=5, last_attempt=current_time - 10)
    # Initialize model_endpoint_parameters if None
    if request2.request_payload.model_endpoint_parameters is None:
        request2.request_payload.model_endpoint_parameters = {}
    request2.request_payload.model_endpoint_parameters["TargetVariant"] = "variant-1"

    # Available capacity is 30, request1 needs 80 (20 * 4)
    available_capacity = 30
    outstanding_requests = [request1, request2]

    # Should return False because request2 is also on variant-1 and is running (last_attempt set)
    # So request1 is NOT the only job for this endpoint/variant
    result = scheduler._check_capacity_available(request1, available_capacity, outstanding_requests)
    assert result is False


def test_calculate_available_capacity_with_target_80_percent(scheduler_setup):
    """Test _calculate_available_capacity with max_capacity=100, target=0.8, current_load=50 returns 30"""
    from aws.osml.model_runner.scheduler.endpoint_capacity_estimator import EndpointCapacityEstimator

    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup

    # Create a mock capacity estimator that returns max_capacity=100
    mock_capacity_estimator = Mock(spec=EndpointCapacityEstimator)
    mock_capacity_estimator.estimate_capacity.return_value = 100

    # Create scheduler with capacity_target_percentage=0.8
    scheduler = EndpointLoadImageScheduler(
        image_request_queue=mock_queue,
        capacity_estimator=mock_capacity_estimator,
        throttling_enabled=True,
        capacity_target_percentage=0.8,
    )

    # Create outstanding requests with total load close to 50
    # 10*4 = 40, 2*4 = 8, total = 48 (close enough)
    # Set last_attempt to recent time so they are considered "currently running"
    current_time = int(time.time())
    request1 = create_status_record("job1", "endpoint1-model", region_count=10, last_attempt=current_time - 10)
    request2 = create_status_record("job2", "endpoint1-model", region_count=2, last_attempt=current_time - 20)

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

    assert available_capacity == 32
    assert max_capacity == 100
    assert current_utilization == 48


def test_calculate_available_capacity_with_target_100_percent(scheduler_setup):
    """Test _calculate_available_capacity with max_capacity=50, target=1.0, current_load=30 returns 20"""
    from aws.osml.model_runner.scheduler.endpoint_capacity_estimator import EndpointCapacityEstimator

    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup

    # Create a mock capacity estimator that returns max_capacity=50
    mock_capacity_estimator = Mock(spec=EndpointCapacityEstimator)
    mock_capacity_estimator.estimate_capacity.return_value = 50

    # Create scheduler with capacity_target_percentage=1.0 (default)
    scheduler = EndpointLoadImageScheduler(
        image_request_queue=mock_queue,
        capacity_estimator=mock_capacity_estimator,
        throttling_enabled=True,
        capacity_target_percentage=1.0,
    )

    # Create outstanding requests with total load of 28 (close to 30)
    # To get load=30 with TILE_WORKERS=4: need 30/4 = 7.5 regions
    # Use 7 regions (28 load) which is close to 30
    # Set last_attempt to recent time so it's considered "currently running"
    current_time = int(time.time())
    request1 = create_status_record("job1", "endpoint1-model", region_count=7, last_attempt=current_time - 10)

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

    assert available_capacity == 22
    assert max_capacity == 50
    assert current_utilization == 28


def test_calculate_available_capacity_with_target_120_percent(scheduler_setup):
    """Test _calculate_available_capacity with max_capacity=200, target=1.2, current_load=100 returns 140"""
    from aws.osml.model_runner.scheduler.endpoint_capacity_estimator import EndpointCapacityEstimator

    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup

    # Create a mock capacity estimator that returns max_capacity=200
    mock_capacity_estimator = Mock(spec=EndpointCapacityEstimator)
    mock_capacity_estimator.estimate_capacity.return_value = 200

    # Create scheduler with capacity_target_percentage=1.2
    scheduler = EndpointLoadImageScheduler(
        image_request_queue=mock_queue,
        capacity_estimator=mock_capacity_estimator,
        throttling_enabled=True,
        capacity_target_percentage=1.2,
    )

    # Create outstanding requests with total load of 100
    # To get load=100 with TILE_WORKERS=4: need 100/4 = 25 regions
    # Set last_attempt to recent time so it's considered "currently running"
    current_time = int(time.time())
    request1 = create_status_record("job1", "endpoint1-model", region_count=25, last_attempt=current_time - 10)

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

    assert available_capacity == 140
    assert max_capacity == 200
    assert current_utilization == 100


def test_calculate_available_capacity_filters_by_endpoint_and_variant(scheduler_setup):
    """Test _calculate_available_capacity filters requests by endpoint and variant correctly"""
    from aws.osml.model_runner.scheduler.endpoint_capacity_estimator import EndpointCapacityEstimator

    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup

    # Create a mock capacity estimator that returns max_capacity=100
    mock_capacity_estimator = Mock(spec=EndpointCapacityEstimator)
    mock_capacity_estimator.estimate_capacity.return_value = 100

    # Create scheduler with default settings
    scheduler = EndpointLoadImageScheduler(
        image_request_queue=mock_queue,
        capacity_estimator=mock_capacity_estimator,
        throttling_enabled=True,
        capacity_target_percentage=1.0,
    )

    # Create requests for different endpoints and variants
    # Set last_attempt to recent time so they are considered "currently running"
    current_time = int(time.time())

    # Request 1: endpoint1-model, variant-1, region_count=10 (load=40)
    request1 = create_status_record("job1", "endpoint1-model", region_count=10, last_attempt=current_time - 10)
    if request1.request_payload.model_endpoint_parameters is None:
        request1.request_payload.model_endpoint_parameters = {}
    request1.request_payload.model_endpoint_parameters["TargetVariant"] = "variant-1"

    # Request 2: endpoint1-model, variant-2, region_count=5 (load=20)
    request2 = create_status_record("job2", "endpoint1-model", region_count=5, last_attempt=current_time - 20)
    if request2.request_payload.model_endpoint_parameters is None:
        request2.request_payload.model_endpoint_parameters = {}
    request2.request_payload.model_endpoint_parameters["TargetVariant"] = "variant-2"

    # Request 3: endpoint2-model, variant-1, region_count=8 (load=32)
    request3 = create_status_record("job3", "endpoint2-model", region_count=8, last_attempt=current_time - 30)
    if request3.request_payload.model_endpoint_parameters is None:
        request3.request_payload.model_endpoint_parameters = {}
    request3.request_payload.model_endpoint_parameters["TargetVariant"] = "variant-1"

    # Request 4: endpoint1-model, variant-1, region_count=3 (load=12)
    request4 = create_status_record("job4", "endpoint1-model", region_count=3, last_attempt=current_time - 40)
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
    assert available_capacity == 48
    assert max_capacity == 100
    assert current_utilization == 52

    # Calculate available capacity for endpoint1-model, variant-2
    # Should only count request2 (20) = 20 total load
    # available = 100 - 20 = 80
    available_capacity, max_capacity, current_utilization = scheduler._calculate_available_capacity(
        "endpoint1-model", "variant-2", outstanding_requests
    )
    assert available_capacity == 80
    assert max_capacity == 100
    assert current_utilization == 20

    # Calculate available capacity for endpoint2-model, variant-1
    # Should only count request3 (32) = 32 total load
    # available = 100 - 32 = 68
    available_capacity, max_capacity, current_utilization = scheduler._calculate_available_capacity(
        "endpoint2-model", "variant-1", outstanding_requests
    )
    assert available_capacity == 68
    assert max_capacity == 100
    assert current_utilization == 32


def test_get_next_scheduled_request_throttling_disabled(scheduler_setup):
    """Test throttling_enabled=False schedules without capacity checks"""
    from aws.osml.model_runner.scheduler.endpoint_capacity_estimator import EndpointCapacityEstimator

    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup

    # Create a mock capacity estimator that would return insufficient capacity
    mock_capacity_estimator = Mock(spec=EndpointCapacityEstimator)
    mock_capacity_estimator.estimate_capacity.return_value = 10  # Very low capacity

    # Create scheduler with throttling disabled
    scheduler = EndpointLoadImageScheduler(
        image_request_queue=mock_queue,
        capacity_estimator=mock_capacity_estimator,
        throttling_enabled=False,  # Throttling disabled
        capacity_target_percentage=1.0,
    )

    # Create a request with high load that would exceed capacity
    time_in_past = int(time.time() - 5)
    request = create_status_record("job1", "endpoint1-model", request_time=time_in_past, region_count=100)

    mock_queue.get_outstanding_requests.return_value = [request]
    mock_queue.requested_jobs_table.start_next_attempt.return_value = True

    # Should schedule the request even though capacity is insufficient
    result = scheduler.get_next_scheduled_request()
    assert result is not None
    assert result.job_id == "job1-id"

    # Verify capacity estimator was NOT called (no capacity checks)
    mock_capacity_estimator.estimate_capacity.assert_not_called()


def test_get_next_scheduled_request_throttling_enabled_checks_capacity(scheduler_setup):
    """Test throttling_enabled=True checks capacity before scheduling"""
    from aws.osml.model_runner.scheduler.endpoint_capacity_estimator import EndpointCapacityEstimator

    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup

    # Create a mock capacity estimator with sufficient capacity
    mock_capacity_estimator = Mock(spec=EndpointCapacityEstimator)
    mock_capacity_estimator.estimate_capacity.return_value = 200  # High capacity

    # Create scheduler with throttling enabled
    scheduler = EndpointLoadImageScheduler(
        image_request_queue=mock_queue,
        capacity_estimator=mock_capacity_estimator,
        throttling_enabled=True,  # Throttling enabled
        capacity_target_percentage=1.0,
    )

    # Create a request with moderate load
    time_in_past = int(time.time() - 5)
    request = create_status_record("job1", "endpoint1-model", request_time=time_in_past, region_count=10)

    mock_queue.get_outstanding_requests.return_value = [request]
    mock_queue.requested_jobs_table.start_next_attempt.return_value = True

    # Should schedule the request because capacity is sufficient
    result = scheduler.get_next_scheduled_request()
    assert result is not None
    assert result.job_id == "job1-id"

    # Verify capacity estimator WAS called (capacity checks performed)
    mock_capacity_estimator.estimate_capacity.assert_called_once_with("endpoint1-model", None)


def test_get_next_scheduled_request_throttling_blocks_insufficient_capacity(scheduler_setup):
    """Test throttling blocks scheduling when capacity is insufficient"""
    from aws.osml.model_runner.scheduler.endpoint_capacity_estimator import EndpointCapacityEstimator

    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup

    # Create a mock capacity estimator with low capacity
    mock_capacity_estimator = Mock(spec=EndpointCapacityEstimator)
    mock_capacity_estimator.estimate_capacity.return_value = 20  # Low capacity

    # Create scheduler with throttling enabled
    scheduler = EndpointLoadImageScheduler(
        image_request_queue=mock_queue,
        capacity_estimator=mock_capacity_estimator,
        throttling_enabled=True,
        capacity_target_percentage=1.0,
    )

    # Create two requests: one already running, one waiting
    time_in_past = int(time.time() - 10)
    running_request = create_status_record(
        "job1", "endpoint1-model", request_time=time_in_past, region_count=4, last_attempt=time_in_past
    )
    waiting_request = create_status_record("job2", "endpoint1-model", request_time=time_in_past + 1, region_count=10)

    mock_queue.get_outstanding_requests.return_value = [running_request, waiting_request]
    mock_queue.requested_jobs_table.start_next_attempt.return_value = True

    # Should NOT schedule waiting_request because capacity is insufficient
    # running_request load = 4 * 4 = 16
    # waiting_request load = 10 * 4 = 40
    # available capacity = 20 - 16 = 4 (less than 40 needed)
    result = scheduler.get_next_scheduled_request()
    assert result is None

    # Verify capacity estimator was called
    mock_capacity_estimator.estimate_capacity.assert_called()


def test_get_next_scheduled_request_uses_target_variant_from_request(scheduler_setup):
    """Test capacity calculation uses TargetVariant from request (already set by queue)"""
    from aws.osml.model_runner.scheduler.endpoint_capacity_estimator import EndpointCapacityEstimator

    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup

    # Create a mock capacity estimator
    mock_capacity_estimator = Mock(spec=EndpointCapacityEstimator)
    mock_capacity_estimator.estimate_capacity.return_value = 100

    # Create scheduler with throttling enabled
    scheduler = EndpointLoadImageScheduler(
        image_request_queue=mock_queue,
        capacity_estimator=mock_capacity_estimator,
        throttling_enabled=True,
        capacity_target_percentage=1.0,
    )

    # Create a request with explicit TargetVariant
    time_in_past = int(time.time() - 5)
    request = create_status_record("job1", "endpoint1-model", request_time=time_in_past, region_count=5)
    if request.request_payload.model_endpoint_parameters is None:
        request.request_payload.model_endpoint_parameters = {}
    request.request_payload.model_endpoint_parameters["TargetVariant"] = "variant-1"

    mock_queue.get_outstanding_requests.return_value = [request]
    mock_queue.requested_jobs_table.start_next_attempt.return_value = True

    # Schedule the request
    result = scheduler.get_next_scheduled_request()
    assert result is not None

    # Verify capacity estimator was called with the specific variant
    mock_capacity_estimator.estimate_capacity.assert_called_once_with("endpoint1-model", "variant-1")


def test_get_next_scheduled_request_capacity_for_specific_variant(scheduler_setup):
    """Test capacity calculation for specific variant (not all variants)"""
    from aws.osml.model_runner.scheduler.endpoint_capacity_estimator import EndpointCapacityEstimator

    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup

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
        image_request_queue=mock_queue,
        capacity_estimator=mock_capacity_estimator,
        throttling_enabled=True,
        capacity_target_percentage=1.0,
    )

    # Create requests for different variants
    time_in_past = int(time.time() - 10)

    # Request 1: variant-1, region_count=10 (load=40)
    request1 = create_status_record("job1", "endpoint1-model", request_time=time_in_past, region_count=10)
    if request1.request_payload.model_endpoint_parameters is None:
        request1.request_payload.model_endpoint_parameters = {}
    request1.request_payload.model_endpoint_parameters["TargetVariant"] = "variant-1"

    # Request 2: variant-2, region_count=20 (load=80)
    request2 = create_status_record("job2", "endpoint1-model", request_time=time_in_past + 1, region_count=20)
    if request2.request_payload.model_endpoint_parameters is None:
        request2.request_payload.model_endpoint_parameters = {}
    request2.request_payload.model_endpoint_parameters["TargetVariant"] = "variant-2"

    mock_queue.get_outstanding_requests.return_value = [request1, request2]
    mock_queue.requested_jobs_table.start_next_attempt.return_value = True

    # Schedule request1 (variant-1)
    # variant-1 capacity = 50, request1 load = 40, available = 50 - 0 = 50 (sufficient)
    result = scheduler.get_next_scheduled_request()
    assert result is not None
    assert result.job_id == "job1-id"

    # Verify capacity was calculated for variant-1 specifically
    mock_capacity_estimator.estimate_capacity.assert_called_with("endpoint1-model", "variant-1")


def test_get_next_scheduled_request_no_capacity_estimator_uses_existing_logic(scheduler_setup):
    """Test no capacity_estimator provided uses existing logic (no capacity checks)"""
    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup
    # Create scheduler without capacity estimator
    scheduler = EndpointLoadImageScheduler(
        image_request_queue=mock_queue,
        capacity_estimator=None,  # No capacity estimator
        throttling_enabled=True,  # Throttling enabled but no estimator
        capacity_target_percentage=1.0,
    )

    # Create a request
    time_in_past = int(time.time() - 5)
    request = create_status_record("job1", "endpoint1-model", request_time=time_in_past, region_count=100)

    mock_queue.get_outstanding_requests.return_value = [request]
    mock_queue.requested_jobs_table.start_next_attempt.return_value = True

    # Should schedule the request without capacity checks
    result = scheduler.get_next_scheduled_request()
    assert result is not None
    assert result.job_id == "job1-id"


def test_get_next_scheduled_request_logs_throttling_decisions(scheduler_setup, caplog):
    """Test logging of throttling decisions"""
    import logging

    from aws.osml.model_runner.scheduler.endpoint_capacity_estimator import EndpointCapacityEstimator

    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup

    # Create a mock capacity estimator with low capacity
    mock_capacity_estimator = Mock(spec=EndpointCapacityEstimator)
    mock_capacity_estimator.estimate_capacity.return_value = 20

    # Create scheduler with throttling enabled
    scheduler = EndpointLoadImageScheduler(
        image_request_queue=mock_queue,
        capacity_estimator=mock_capacity_estimator,
        throttling_enabled=True,
        capacity_target_percentage=0.8,
    )

    # Create two requests: one running, one waiting
    time_in_past = int(time.time() - 10)
    running_request = create_status_record(
        "job1", "endpoint1-model", request_time=time_in_past, region_count=3, last_attempt=time_in_past
    )
    waiting_request = create_status_record("job2", "endpoint1-model", request_time=time_in_past + 1, region_count=10)

    mock_queue.get_outstanding_requests.return_value = [running_request, waiting_request]
    mock_queue.requested_jobs_table.start_next_attempt.return_value = True

    # Capture log output
    with caplog.at_level(logging.INFO, logger="aws.osml.model_runner.scheduler.endpoint_load_image_scheduler"):
        result = scheduler.get_next_scheduled_request()
        assert result is None

        # Verify info log was emitted for throttling (changed from WARNING to INFO level)
        assert any("Throttling job job2-id due to insufficient capacity" in message for message in caplog.text.split("\n"))
        assert any("Required load:" in message for message in caplog.text.split("\n"))
        assert any("Available capacity:" in message for message in caplog.text.split("\n"))


def test_get_next_scheduled_request_logs_successful_scheduling(scheduler_setup, caplog):
    """Test logging of successful scheduling with capacity details"""
    import logging

    from aws.osml.model_runner.scheduler.endpoint_capacity_estimator import EndpointCapacityEstimator

    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup

    # Create a mock capacity estimator with sufficient capacity
    mock_capacity_estimator = Mock(spec=EndpointCapacityEstimator)
    mock_capacity_estimator.estimate_capacity.return_value = 200

    # Create scheduler with throttling enabled
    scheduler = EndpointLoadImageScheduler(
        image_request_queue=mock_queue,
        capacity_estimator=mock_capacity_estimator,
        throttling_enabled=True,
        capacity_target_percentage=0.9,
    )

    # Create a request
    time_in_past = int(time.time() - 5)
    request = create_status_record("job1", "endpoint1-model", request_time=time_in_past, region_count=10)

    mock_queue.get_outstanding_requests.return_value = [request]
    mock_queue.requested_jobs_table.start_next_attempt.return_value = True

    # Capture log output
    with caplog.at_level(logging.INFO, logger="aws.osml.model_runner.scheduler.endpoint_load_image_scheduler"):
        result = scheduler.get_next_scheduled_request()
        assert result is not None

        # Verify info log was emitted with capacity details
        assert any("Scheduling job job1-id with sufficient capacity" in message for message in caplog.text.split("\n"))
        assert any("Required load:" in message for message in caplog.text.split("\n"))
        assert any("Available capacity:" in message for message in caplog.text.split("\n"))
        assert any("Target percentage: 90.0%" in message for message in caplog.text.split("\n"))


def test_check_capacity_available_single_image_exception_ignores_not_running_jobs(scheduler_setup):
    """Test single image exception only considers currently running jobs (bug fix)"""
    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup
    # Create multiple new requests (last_attempt=0) that exceed endpoint capacity
    request1 = create_status_record("job1", "endpoint1-model", region_count=20, last_attempt=0)
    request2 = create_status_record("job2", "endpoint1-model", region_count=20, last_attempt=0)

    # Available capacity is only 30, which is less than required load of 80
    available_capacity = 30
    outstanding_requests = [request1, request2]

    # Should return True due to single image exception
    # Even though request2 exists, it's not running (last_attempt=0)
    # So request1 is free to start even though it exceeds available capacity
    result = scheduler._check_capacity_available(request1, available_capacity, outstanding_requests)
    assert result is True


def test_check_capacity_available_single_image_exception_considers_running_jobs(scheduler_setup):
    """Test single image exception correctly identifies running jobs"""
    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup
    # Create requests where one is running
    current_time = int(time.time())

    # Request 1: Large image that needs 80 tiles (20 * 4)
    request1 = create_status_record("job1", "endpoint1-model", region_count=20, last_attempt=0)

    # Request 2: Currently running (last_attempt is recent)
    request2 = create_status_record("job2", "endpoint1-model", region_count=5, last_attempt=current_time - 10)

    # Available capacity is only 30, which is less than required load of 80
    available_capacity = 30
    outstanding_requests = [request1, request2]

    # Should return False because request2 is running
    # So request1 is NOT the only job for this endpoint
    result = scheduler._check_capacity_available(request1, available_capacity, outstanding_requests)
    assert result is False


def test_get_running_jobs_for_endpoint_variant_filters_correctly(scheduler_setup):
    """Test _get_running_jobs_for_endpoint_variant filters by running status"""
    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup
    current_time = int(time.time())

    # Create various requests with different states
    # Running: last_attempt is recent
    running_request = create_status_record("job1", "endpoint1-model", region_count=5, last_attempt=current_time - 10)

    # Not started: last_attempt=0
    not_started_request = create_status_record("job2", "endpoint1-model", region_count=5, last_attempt=0)

    # Timed out: last_attempt is too old (beyond retry_time)
    timed_out_request = create_status_record("job3", "endpoint1-model", region_count=5, last_attempt=current_time - 700)

    # Different endpoint: should be excluded
    different_endpoint_request = create_status_record(
        "job4", "endpoint2-model", region_count=5, last_attempt=current_time - 10
    )

    outstanding_requests = [running_request, not_started_request, timed_out_request, different_endpoint_request]

    # Get running jobs for endpoint1-model
    running_jobs = scheduler._get_running_jobs_for_endpoint_variant("endpoint1-model", None, outstanding_requests)

    # Should only include running_request
    assert len(running_jobs) == 1
    assert running_jobs[0].job_id == "job1-id"


def test_get_running_jobs_for_endpoint_variant_excludes_job_id(scheduler_setup):
    """Test _get_running_jobs_for_endpoint_variant excludes specified job_id"""
    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup
    current_time = int(time.time())

    # Create running requests
    request1 = create_status_record("job1", "endpoint1-model", region_count=5, last_attempt=current_time - 10)
    request2 = create_status_record("job2", "endpoint1-model", region_count=5, last_attempt=current_time - 20)

    outstanding_requests = [request1, request2]

    # Get running jobs excluding job1
    running_jobs = scheduler._get_running_jobs_for_endpoint_variant(
        "endpoint1-model", None, outstanding_requests, exclude_job_id="job1-id"
    )

    # Should only include request2
    assert len(running_jobs) == 1
    assert running_jobs[0].job_id == "job2-id"


def test_get_running_jobs_for_endpoint_variant_filters_by_variant(scheduler_setup):
    """Test _get_running_jobs_for_endpoint_variant filters by variant correctly"""
    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup
    current_time = int(time.time())

    # Create running requests with different variants
    request1 = create_status_record("job1", "endpoint1-model", region_count=5, last_attempt=current_time - 10)
    if request1.request_payload.model_endpoint_parameters is None:
        request1.request_payload.model_endpoint_parameters = {}
    request1.request_payload.model_endpoint_parameters["TargetVariant"] = "variant-1"

    request2 = create_status_record("job2", "endpoint1-model", region_count=5, last_attempt=current_time - 20)
    if request2.request_payload.model_endpoint_parameters is None:
        request2.request_payload.model_endpoint_parameters = {}
    request2.request_payload.model_endpoint_parameters["TargetVariant"] = "variant-2"

    outstanding_requests = [request1, request2]

    # Get running jobs for variant-1
    running_jobs = scheduler._get_running_jobs_for_endpoint_variant("endpoint1-model", "variant-1", outstanding_requests)

    # Should only include request1
    assert len(running_jobs) == 1
    assert running_jobs[0].job_id == "job1-id"


def test_schedule_next_image_request_no_eligible_requests_returns_none(scheduler_setup):
    """Test schedule_next_image_request returns None when no requests are eligible"""
    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup
    # Arrange - Mock to return no eligible requests
    mock_queue.get_outstanding_requests.return_value = []

    # Act
    result = scheduler.get_next_scheduled_request()

    # Assert
    assert result is None


def test_schedule_next_image_request_handles_exception_in_scheduling(scheduler_setup, mocker):
    """Test get_next_scheduled_request handles exception and logs error"""
    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup

    mock_logger = mocker.patch("aws.osml.model_runner.scheduler.endpoint_load_image_scheduler.logger")

    # Arrange - Mock to raise exception
    mock_queue.get_outstanding_requests.side_effect = Exception("Test error")

    # Act
    result = scheduler.get_next_scheduled_request()

    # Assert - returns None on error
    assert result is None
    # Verify error logged
    mock_logger.error.assert_called_once()
    error_args = str(mock_logger.error.call_args)
    assert "Error getting next scheduled request" in error_args


def test_calculate_available_capacity_no_estimator_returns_zeros(scheduler_setup):
    """Test _calculate_available_capacity returns zeros when estimator not configured"""
    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup
    # Arrange - Create scheduler without capacity estimator
    scheduler = EndpointLoadImageScheduler(
        image_request_queue=mock_queue,
        throttling_enabled=True,
    )
    # Ensure capacity_estimator is None
    scheduler.capacity_estimator = None

    # Act
    available, max_cap, current = scheduler._calculate_available_capacity("endpoint1", None, [])

    # Assert - returns (0, 0, 0)
    assert available == 0
    assert max_cap == 0
    assert current == 0


def test_calculate_available_capacity_exception_returns_zeros(scheduler_setup, mocker):
    """Test _calculate_available_capacity handles exception and returns zeros"""
    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup

    mock_logger = mocker.patch("aws.osml.model_runner.scheduler.endpoint_load_image_scheduler.logger")

    # Arrange - Mock capacity estimator to raise exception
    scheduler = EndpointLoadImageScheduler(
        image_request_queue=mock_queue,
        throttling_enabled=True,
    )
    mock_estimator = Mock()
    mock_estimator.estimate_capacity.side_effect = Exception("Estimator error")
    scheduler.capacity_estimator = mock_estimator

    # Act
    available, max_cap, current = scheduler._calculate_available_capacity("endpoint1", None, [])

    # Assert - returns (0, 0, 0) on error
    assert available == 0
    assert max_cap == 0
    assert current == 0
    # Verify error logged
    mock_logger.error.assert_called()
    error_args = str(mock_logger.error.call_args)
    assert "Error calculating available capacity" in error_args


def test_get_endpoint_instance_count_http_endpoint_returns_one(scheduler_setup):
    """Test _get_endpoint_instance_count returns 1 for HTTP endpoints"""
    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup
    # Arrange
    http_endpoint = "http://example.com/model"

    # Act
    count = scheduler._get_endpoint_instance_count(http_endpoint)

    # Assert - HTTP endpoints default to 1 instance
    assert count == 1


def test_calculate_endpoint_utilization_handles_request_with_started_attempt_no_region_count(scheduler_setup):
    """Test _calculate_endpoint_utilization handles request with last_attempt > 0 but no region_count (line 563)"""
    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup
    # Arrange - Create request with last_attempt > 0 but region_count = None
    request = create_status_record("job1", "endpoint1-model", last_attempt=int(time.time()) - 100, region_count=None)
    grouped = {"endpoint1-model": [request]}

    # Act
    utilization = scheduler._calculate_endpoint_utilization(grouped)

    # Assert - assumes load of 1 for started request without region count
    assert len(utilization) == 1
    assert utilization[0].current_load == 1


def test_emit_utilization_metric_handles_exception_gracefully(scheduler_setup):
    """Test _emit_utilization_metric handles exception without propagating"""
    from aws_embedded_metrics.logger.metrics_logger import MetricsLogger

    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup

    scheduler = EndpointLoadImageScheduler(
        image_request_queue=mock_queue,
        throttling_enabled=True,
    )

    # Create mock metrics that raises exception
    mock_metrics = Mock(spec=MetricsLogger)
    mock_metrics.put_metric.side_effect = Exception("Metrics error")

    # Act - should not propagate exception
    scheduler._emit_utilization_metric.__wrapped__(scheduler, "test-endpoint", 100, 50, metrics=mock_metrics)

    # Assert - exception handled, doesn't propagate (test passes if no exception raised)


def test_emit_throttle_metric_handles_exception_gracefully(scheduler_setup):
    """Test _emit_throttle_metric handles exception without propagating"""
    from aws_embedded_metrics.logger.metrics_logger import MetricsLogger

    scheduler, mock_queue, sagemaker, endpoints = scheduler_setup

    scheduler = EndpointLoadImageScheduler(
        image_request_queue=mock_queue,
        throttling_enabled=True,
    )

    # Create mock metrics that raises exception
    mock_metrics = Mock(spec=MetricsLogger)
    mock_metrics.put_metric.side_effect = Exception("Metrics error")

    # Act - should not propagate exception
    scheduler._emit_throttle_metric.__wrapped__(scheduler, "test-endpoint", metrics=mock_metrics)

    # Assert - exception handled, doesn't propagate (test passes if no exception raised)


def test_throttles_metric_increments_when_throttling_occurs(scheduler_metrics_setup):
    """Test Throttles metric (Operation=Scheduling, ModelName=<endpoint>) increments when throttling occurs"""
    from aws_embedded_metrics.unit import Unit

    from aws.osml.model_runner.app_config import MetricLabels

    sagemaker, mock_queue, endpoints = scheduler_metrics_setup

    scheduler = EndpointLoadImageScheduler(
        image_request_queue=mock_queue,
        throttling_enabled=True,
        capacity_target_percentage=1.0,
    )

    mock_metrics = create_mock_metrics_logger()

    # Call the underlying method without the decorator
    scheduler._emit_throttle_metric.__wrapped__(scheduler, "endpoint1-model", metrics=mock_metrics)

    mock_metrics.put_dimensions.assert_called_once_with(
        {
            MetricLabels.OPERATION_DIMENSION: MetricLabels.SCHEDULING_OPERATION,
            MetricLabels.MODEL_NAME_DIMENSION: "endpoint1-model",
        }
    )
    mock_metrics.put_metric.assert_called_once_with(MetricLabels.THROTTLES, 1, str(Unit.COUNT.value))


def test_utilization_metric_shows_correct_percentage(scheduler_metrics_setup):
    """Test Utilization metric (Operation=Scheduling, ModelName=<endpoint>) shows correct percentage (0-100%)"""
    from aws_embedded_metrics.unit import Unit

    from aws.osml.model_runner.app_config import MetricLabels

    sagemaker, mock_queue, endpoints = scheduler_metrics_setup

    scheduler = EndpointLoadImageScheduler(
        image_request_queue=mock_queue,
        throttling_enabled=True,
        capacity_target_percentage=1.0,
    )

    mock_metrics = create_mock_metrics_logger()

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


def test_utilization_metric_clamps_to_valid_range(scheduler_metrics_setup):
    """Test Utilization metric clamps values to 0-100% range"""
    from aws_embedded_metrics.unit import Unit

    from aws.osml.model_runner.app_config import MetricLabels

    sagemaker, mock_queue, endpoints = scheduler_metrics_setup

    scheduler = EndpointLoadImageScheduler(
        image_request_queue=mock_queue,
        throttling_enabled=True,
        capacity_target_percentage=1.0,
    )

    mock_metrics = create_mock_metrics_logger()

    # Over 100% should be clamped to 100%
    scheduler._emit_utilization_metric.__wrapped__(
        scheduler, "endpoint1-model", max_capacity=100, current_utilization=150, metrics=mock_metrics
    )
    mock_metrics.put_metric.assert_called_with(MetricLabels.UTILIZATION, 100.0, str(Unit.PERCENT.value))


def test_utilization_metric_handles_zero_max_capacity(scheduler_metrics_setup):
    """Test Utilization metric handles zero max_capacity gracefully"""
    sagemaker, mock_queue, endpoints = scheduler_metrics_setup

    scheduler = EndpointLoadImageScheduler(
        image_request_queue=mock_queue,
        throttling_enabled=True,
        capacity_target_percentage=1.0,
    )

    mock_metrics = create_mock_metrics_logger()

    # Zero max_capacity should not emit metric
    scheduler._emit_utilization_metric.__wrapped__(
        scheduler, "endpoint1-model", max_capacity=0, current_utilization=50, metrics=mock_metrics
    )
    mock_metrics.put_metric.assert_not_called()


def test_duration_metric_records_scheduling_latency(scheduler_metrics_setup):
    """Test Duration metric (Operation=Scheduling) records scheduling decision latency"""
    from aws_embedded_metrics.unit import Unit

    from aws.osml.model_runner.app_config import MetricLabels
    from aws.osml.model_runner.scheduler.endpoint_capacity_estimator import EndpointCapacityEstimator

    sagemaker, mock_queue, endpoints = scheduler_metrics_setup

    mock_capacity_estimator = Mock(spec=EndpointCapacityEstimator)
    mock_capacity_estimator.estimate_capacity.return_value = 200

    scheduler = EndpointLoadImageScheduler(
        image_request_queue=mock_queue,
        capacity_estimator=mock_capacity_estimator,
        throttling_enabled=True,
        capacity_target_percentage=1.0,
    )

    time_in_past = int(time.time() - 5)
    request = create_status_record("job1", "endpoint1-model", request_time=time_in_past, region_count=5)

    mock_queue.get_outstanding_requests.return_value = [request]
    mock_queue.requested_jobs_table.start_next_attempt.return_value = True

    mock_metrics = create_mock_metrics_logger()

    # Call the helper method directly to test metric emission
    scheduler._emit_scheduling_metrics.__wrapped__(scheduler, duration_ms=100.0, metrics=mock_metrics)

    duration_calls = [
        call
        for call in mock_metrics.put_metric.call_args_list
        if call[0][0] == MetricLabels.DURATION and call[0][2] == str(Unit.MILLISECONDS.value)
    ]
    assert len(duration_calls) == 1
    assert duration_calls[0][0][1] >= 0


def test_invocations_metric_increments_when_evaluating_images(scheduler_metrics_setup):
    """Test Invocations metric (Operation=Scheduling) increments when evaluating images"""
    from aws_embedded_metrics.unit import Unit

    from aws.osml.model_runner.app_config import MetricLabels
    from aws.osml.model_runner.scheduler.endpoint_capacity_estimator import EndpointCapacityEstimator

    sagemaker, mock_queue, endpoints = scheduler_metrics_setup

    mock_capacity_estimator = Mock(spec=EndpointCapacityEstimator)
    mock_capacity_estimator.estimate_capacity.return_value = 200

    scheduler = EndpointLoadImageScheduler(
        image_request_queue=mock_queue,
        capacity_estimator=mock_capacity_estimator,
        throttling_enabled=True,
        capacity_target_percentage=1.0,
    )

    time_in_past = int(time.time() - 5)
    request = create_status_record("job1", "endpoint1-model", request_time=time_in_past, region_count=5)

    mock_queue.get_outstanding_requests.return_value = [request]
    mock_queue.requested_jobs_table.start_next_attempt.return_value = True

    mock_metrics = create_mock_metrics_logger()

    # Call the helper method directly to test metric emission
    scheduler._emit_scheduling_metrics.__wrapped__(scheduler, duration_ms=100.0, metrics=mock_metrics)

    invocations_calls = [
        call
        for call in mock_metrics.put_metric.call_args_list
        if call[0][0] == MetricLabels.INVOCATIONS and call[0][2] == str(Unit.COUNT.value)
    ]
    assert len(invocations_calls) == 1
    assert invocations_calls[0][0][1] == 1


def test_invocations_metric_not_emitted_when_no_requests(scheduler_metrics_setup, mocker):
    """Test Invocations metric is not emitted when there are no outstanding requests"""
    sagemaker, mock_queue, endpoints = scheduler_metrics_setup

    scheduler = EndpointLoadImageScheduler(
        image_request_queue=mock_queue,
        throttling_enabled=True,
        capacity_target_percentage=1.0,
    )

    mock_queue.get_outstanding_requests.return_value = []

    # Mock _emit_scheduling_metrics to verify it's not called when no requests
    mock_emit = mocker.patch.object(scheduler, "_emit_scheduling_metrics")
    scheduler.get_next_scheduled_request()
    mock_emit.assert_not_called()


def test_metrics_follow_standard_modelrunner_pattern(scheduler_metrics_setup):
    """Test metrics follow standard ModelRunner pattern with correct namespace and dimensions"""
    from aws_embedded_metrics.unit import Unit

    from aws.osml.model_runner.app_config import MetricLabels

    sagemaker, mock_queue, endpoints = scheduler_metrics_setup

    scheduler = EndpointLoadImageScheduler(
        image_request_queue=mock_queue,
        throttling_enabled=True,
        capacity_target_percentage=1.0,
    )

    mock_metrics = create_mock_metrics_logger()

    scheduler._emit_throttle_metric.__wrapped__(scheduler, "test-endpoint", metrics=mock_metrics)

    mock_metrics.put_dimensions.assert_called_with(
        {
            MetricLabels.OPERATION_DIMENSION: MetricLabels.SCHEDULING_OPERATION,
            MetricLabels.MODEL_NAME_DIMENSION: "test-endpoint",
        }
    )
    mock_metrics.put_metric.assert_called_with(MetricLabels.THROTTLES, 1, str(Unit.COUNT.value))


def test_throttle_metric_handles_none_metrics_logger(scheduler_metrics_setup):
    """Test _emit_throttle_metric handles None metrics logger gracefully"""
    sagemaker, mock_queue, endpoints = scheduler_metrics_setup

    scheduler = EndpointLoadImageScheduler(
        image_request_queue=mock_queue,
        throttling_enabled=True,
        capacity_target_percentage=1.0,
    )

    # Should not raise exception when metrics is None
    scheduler._emit_throttle_metric.__wrapped__(scheduler, "test-endpoint", metrics=None)


def test_utilization_metric_handles_none_metrics_logger(scheduler_metrics_setup):
    """Test _emit_utilization_metric handles None metrics logger gracefully"""
    sagemaker, mock_queue, endpoints = scheduler_metrics_setup

    scheduler = EndpointLoadImageScheduler(
        image_request_queue=mock_queue,
        throttling_enabled=True,
        capacity_target_percentage=1.0,
    )

    # Should not raise exception when metrics is None
    scheduler._emit_utilization_metric.__wrapped__(
        scheduler, "test-endpoint", max_capacity=100, current_utilization=50, metrics=None
    )
