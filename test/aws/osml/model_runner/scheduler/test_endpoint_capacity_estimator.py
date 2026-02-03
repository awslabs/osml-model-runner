#  Copyright 2025-2026 Amazon.com, Inc. or its affiliates.

import time

import boto3
import pytest
from moto import mock_aws

from aws.osml.model_runner.scheduler import EndpointCapacityEstimator


@pytest.fixture
def capacity_estimator_setup():
    """Set up test fixtures before each test method."""
    with mock_aws():
        sagemaker = boto3.client("sagemaker", region_name="us-west-2")
        estimator = EndpointCapacityEstimator(
            sm_client=sagemaker, default_instance_concurrency=2, default_http_concurrency=10, cache_ttl_seconds=300
        )
        yield estimator, sagemaker


def test_http_endpoint_returns_default_concurrency(capacity_estimator_setup):
    """Test that HTTP endpoints return DEFAULT_HTTP_ENDPOINT_CONCURRENCY"""
    estimator, sagemaker = capacity_estimator_setup

    # Test http://
    capacity = estimator.estimate_capacity("http://example.com/model")
    assert capacity == 10

    # Test https://
    capacity = estimator.estimate_capacity("https://api.example.com/inference")
    assert capacity == 10


def test_serverless_endpoint_returns_max_concurrency(capacity_estimator_setup):
    """Test that serverless endpoints return MaxConcurrency"""
    estimator, sagemaker = capacity_estimator_setup

    # Create a serverless endpoint
    endpoint_name = "test-serverless-endpoint"

    # Create model
    sagemaker.create_model(ModelName=f"{endpoint_name}-model", PrimaryContainer={"Image": "test-image"})

    # Create endpoint config with serverless
    sagemaker.create_endpoint_config(
        EndpointConfigName=f"{endpoint_name}-config",
        ProductionVariants=[
            {
                "VariantName": "AllTraffic",
                "ModelName": f"{endpoint_name}-model",
                "ServerlessConfig": {"MemorySizeInMB": 2048, "MaxConcurrency": 100},
            }
        ],
    )

    # Create endpoint
    sagemaker.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=f"{endpoint_name}-config")

    # Test capacity estimation
    capacity = estimator.estimate_capacity(endpoint_name)
    assert capacity == 100


def test_instance_backed_endpoint_without_tag(capacity_estimator_setup):
    """Test instance-backed endpoint without osml:instance-concurrency tag"""
    estimator, sagemaker = capacity_estimator_setup
    endpoint_name = "test-instance-endpoint"

    # Create model
    sagemaker.create_model(ModelName=f"{endpoint_name}-model", PrimaryContainer={"Image": "test-image"})

    # Create endpoint config
    sagemaker.create_endpoint_config(
        EndpointConfigName=f"{endpoint_name}-config",
        ProductionVariants=[
            {
                "VariantName": "AllTraffic",
                "ModelName": f"{endpoint_name}-model",
                "InstanceType": "ml.m5.xlarge",
                "InitialInstanceCount": 2,
            }
        ],
    )

    # Create endpoint
    sagemaker.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=f"{endpoint_name}-config")

    # Test capacity: 2 instances × 2 default concurrency = 4
    capacity = estimator.estimate_capacity(endpoint_name)
    assert capacity == 4


def test_instance_backed_endpoint_with_tag(capacity_estimator_setup):
    """Test instance-backed endpoint with osml:instance-concurrency tag"""
    estimator, sagemaker = capacity_estimator_setup
    endpoint_name = "test-tagged-endpoint"

    # Create model
    sagemaker.create_model(ModelName=f"{endpoint_name}-model", PrimaryContainer={"Image": "test-image"})

    # Create endpoint config
    sagemaker.create_endpoint_config(
        EndpointConfigName=f"{endpoint_name}-config",
        ProductionVariants=[
            {
                "VariantName": "AllTraffic",
                "ModelName": f"{endpoint_name}-model",
                "InstanceType": "ml.m5.xlarge",
                "InitialInstanceCount": 3,
            }
        ],
    )

    # Create endpoint
    sagemaker.create_endpoint(
        EndpointName=endpoint_name,
        EndpointConfigName=f"{endpoint_name}-config",
        Tags=[{"Key": "osml:instance-concurrency", "Value": "5"}],
    )

    # Test capacity: 3 instances × 5 concurrency = 15
    capacity = estimator.estimate_capacity(endpoint_name)
    assert capacity == 15


def test_multi_variant_endpoint_all_variants(capacity_estimator_setup):
    """Test multi-variant endpoint with variant_name=None returns sum of all variants"""
    estimator, sagemaker = capacity_estimator_setup
    endpoint_name = "test-multi-variant"

    # Create models
    sagemaker.create_model(ModelName=f"{endpoint_name}-model-1", PrimaryContainer={"Image": "test-image-1"})
    sagemaker.create_model(ModelName=f"{endpoint_name}-model-2", PrimaryContainer={"Image": "test-image-2"})

    # Create endpoint config with multiple variants
    sagemaker.create_endpoint_config(
        EndpointConfigName=f"{endpoint_name}-config",
        ProductionVariants=[
            {
                "VariantName": "variant-1",
                "ModelName": f"{endpoint_name}-model-1",
                "InstanceType": "ml.m5.xlarge",
                "InitialInstanceCount": 2,  # 2 × 2 = 4
            },
            {
                "VariantName": "variant-2",
                "ModelName": f"{endpoint_name}-model-2",
                "InstanceType": "ml.m5.xlarge",
                "InitialInstanceCount": 3,  # 3 × 2 = 6
            },
        ],
    )

    # Create endpoint
    sagemaker.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=f"{endpoint_name}-config")

    # Test capacity for all variants: 4 + 6 = 10
    capacity = estimator.estimate_capacity(endpoint_name, variant_name=None)
    assert capacity == 10


def test_multi_variant_endpoint_specific_variant(capacity_estimator_setup):
    """Test multi-variant endpoint with specific variant_name returns only that variant's capacity"""
    estimator, sagemaker = capacity_estimator_setup
    endpoint_name = "test-multi-variant-specific"

    # Create models
    sagemaker.create_model(ModelName=f"{endpoint_name}-model-1", PrimaryContainer={"Image": "test-image-1"})
    sagemaker.create_model(ModelName=f"{endpoint_name}-model-2", PrimaryContainer={"Image": "test-image-2"})

    # Create endpoint config with multiple variants
    sagemaker.create_endpoint_config(
        EndpointConfigName=f"{endpoint_name}-config",
        ProductionVariants=[
            {
                "VariantName": "variant-1",
                "ModelName": f"{endpoint_name}-model-1",
                "InstanceType": "ml.m5.xlarge",
                "InitialInstanceCount": 2,  # 2 × 2 = 4
            },
            {
                "VariantName": "variant-2",
                "ModelName": f"{endpoint_name}-model-2",
                "InstanceType": "ml.m5.xlarge",
                "InitialInstanceCount": 3,  # 3 × 2 = 6
            },
        ],
    )

    # Create endpoint
    sagemaker.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=f"{endpoint_name}-config")

    # Test capacity for variant-1 only: 2 × 2 = 4
    capacity = estimator.estimate_capacity(endpoint_name, variant_name="variant-1")
    assert capacity == 4

    # Test capacity for variant-2 only: 3 × 2 = 6
    capacity = estimator.estimate_capacity(endpoint_name, variant_name="variant-2")
    assert capacity == 6


def test_capacity_caching_reduces_api_calls(capacity_estimator_setup):
    """Test that capacity caching reduces SageMaker API calls"""
    estimator, sagemaker = capacity_estimator_setup
    endpoint_name = "test-cache-endpoint"

    # Create model
    sagemaker.create_model(ModelName=f"{endpoint_name}-model", PrimaryContainer={"Image": "test-image"})

    # Create endpoint config
    sagemaker.create_endpoint_config(
        EndpointConfigName=f"{endpoint_name}-config",
        ProductionVariants=[
            {
                "VariantName": "AllTraffic",
                "ModelName": f"{endpoint_name}-model",
                "InstanceType": "ml.m5.xlarge",
                "InitialInstanceCount": 2,
            }
        ],
    )

    # Create endpoint
    sagemaker.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=f"{endpoint_name}-config")

    # Mock the describe_endpoint call to count invocations
    original_describe = sagemaker.describe_endpoint
    call_count = {"count": 0}

    def counting_describe(*args, **kwargs):
        call_count["count"] += 1
        return original_describe(*args, **kwargs)

    sagemaker.describe_endpoint = counting_describe

    # First call - should hit API
    capacity1 = estimator.estimate_capacity(endpoint_name)
    assert call_count["count"] == 1

    # Second call - should use cache (TTLCache)
    capacity2 = estimator.estimate_capacity(endpoint_name)
    assert call_count["count"] == 1  # No additional API call

    # Capacities should be the same
    assert capacity1 == capacity2

    # Verify cache size is bounded
    assert len(estimator._endpoint_cache) <= 100  # max_size default


def test_cache_expiration_after_ttl(capacity_estimator_setup):
    """Test that cache expires after TTL using TTLCache"""
    estimator, sagemaker = capacity_estimator_setup

    # Create estimator with short TTL
    short_ttl_estimator = EndpointCapacityEstimator(
        sm_client=sagemaker,
        default_instance_concurrency=2,
        default_http_concurrency=10,
        cache_ttl_seconds=1,  # 1 second TTL
    )

    endpoint_name = "test-ttl-endpoint"

    # Create model
    sagemaker.create_model(ModelName=f"{endpoint_name}-model", PrimaryContainer={"Image": "test-image"})

    # Create endpoint config
    sagemaker.create_endpoint_config(
        EndpointConfigName=f"{endpoint_name}-config",
        ProductionVariants=[
            {
                "VariantName": "AllTraffic",
                "ModelName": f"{endpoint_name}-model",
                "InstanceType": "ml.m5.xlarge",
                "InitialInstanceCount": 2,
            }
        ],
    )

    # Create endpoint
    sagemaker.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=f"{endpoint_name}-config")

    # Mock the describe_endpoint call to count invocations
    original_describe = sagemaker.describe_endpoint
    call_count = {"count": 0}

    def counting_describe(*args, **kwargs):
        call_count["count"] += 1
        return original_describe(*args, **kwargs)

    sagemaker.describe_endpoint = counting_describe

    # First call - should hit API
    short_ttl_estimator.estimate_capacity(endpoint_name)
    assert call_count["count"] == 1

    # Wait for cache to expire (TTLCache automatically removes expired items)
    time.sleep(1.1)

    # Second call after TTL - should hit API again
    short_ttl_estimator.estimate_capacity(endpoint_name)
    assert call_count["count"] == 2


def test_cache_bounded_size(capacity_estimator_setup):
    """Test that cache size is bounded and uses LRU eviction"""
    estimator, sagemaker = capacity_estimator_setup

    # Create estimator with small cache size
    small_cache_estimator = EndpointCapacityEstimator(
        sm_client=sagemaker,
        default_instance_concurrency=2,
        default_http_concurrency=10,
        cache_ttl_seconds=300,
        cache_max_size=3,  # Only cache 3 endpoints
    )

    # Create 5 endpoints
    for i in range(5):
        endpoint_name = f"test-endpoint-{i}"
        sagemaker.create_model(ModelName=f"{endpoint_name}-model", PrimaryContainer={"Image": "test-image"})
        sagemaker.create_endpoint_config(
            EndpointConfigName=f"{endpoint_name}-config",
            ProductionVariants=[
                {
                    "VariantName": "AllTraffic",
                    "ModelName": f"{endpoint_name}-model",
                    "InstanceType": "ml.m5.xlarge",
                    "InitialInstanceCount": 2,
                }
            ],
        )
        sagemaker.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=f"{endpoint_name}-config")

    # Query all 5 endpoints
    for i in range(5):
        small_cache_estimator.estimate_capacity(f"test-endpoint-{i}")

    # Cache should only contain 3 items (most recent due to LRU)
    assert len(small_cache_estimator._endpoint_cache) == 3

    # The most recent 3 should be cached (endpoints 2, 3, 4)
    assert "test-endpoint-2" in small_cache_estimator._endpoint_cache
    assert "test-endpoint-3" in small_cache_estimator._endpoint_cache
    assert "test-endpoint-4" in small_cache_estimator._endpoint_cache

    # The oldest 2 should have been evicted (endpoints 0, 1)
    assert "test-endpoint-0" not in small_cache_estimator._endpoint_cache
    assert "test-endpoint-1" not in small_cache_estimator._endpoint_cache


def test_estimate_capacity_describe_endpoint_fails_returns_default(capacity_estimator_setup, mocker):
    """Test that API failures return default_instance_concurrency when no cache available"""
    estimator, sagemaker = capacity_estimator_setup

    # Arrange - Mock describe_endpoint to raise exception
    original_describe = sagemaker.describe_endpoint
    sagemaker.describe_endpoint = mocker.Mock(side_effect=Exception("API Error"))

    # Act
    capacity = estimator.estimate_capacity("nonexistent-endpoint")

    # Assert - Should return default_instance_concurrency
    assert capacity == 2  # default_instance_concurrency=2

    # Cleanup
    sagemaker.describe_endpoint = original_describe


def test_estimate_capacity_api_fails_uses_cached_fallback(capacity_estimator_setup, mocker):
    """Test that API failures use cached data when available"""
    estimator, sagemaker = capacity_estimator_setup
    endpoint_name = "test-cache-fallback"

    # Create endpoint to populate cache
    sagemaker.create_model(ModelName=f"{endpoint_name}-model", PrimaryContainer={"Image": "test-image"})
    sagemaker.create_endpoint_config(
        EndpointConfigName=f"{endpoint_name}-config",
        ProductionVariants=[
            {
                "VariantName": "AllTraffic",
                "ModelName": f"{endpoint_name}-model",
                "InstanceType": "ml.m5.xlarge",
                "InitialInstanceCount": 3,
            }
        ],
    )
    sagemaker.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=f"{endpoint_name}-config")

    # First call - populate cache
    first_capacity = estimator.estimate_capacity(endpoint_name)
    assert first_capacity == 6  # 3 instances × 2 default concurrency

    # Mock API to fail
    original_describe = sagemaker.describe_endpoint
    sagemaker.describe_endpoint = mocker.Mock(side_effect=Exception("API temporarily unavailable"))

    # Second call - should use cached data
    second_capacity = estimator.estimate_capacity(endpoint_name)
    assert second_capacity == 6  # Same as cached value

    # Cleanup
    sagemaker.describe_endpoint = original_describe


def test_list_tags_failure_returns_empty_dict(capacity_estimator_setup, mocker):
    """Test that ListTags API failure returns empty dict when no cache"""
    estimator, sagemaker = capacity_estimator_setup
    endpoint_name = "test-tags-failure"

    # Create endpoint
    sagemaker.create_model(ModelName=f"{endpoint_name}-model", PrimaryContainer={"Image": "test-image"})
    sagemaker.create_endpoint_config(
        EndpointConfigName=f"{endpoint_name}-config",
        ProductionVariants=[
            {
                "VariantName": "AllTraffic",
                "ModelName": f"{endpoint_name}-model",
                "InstanceType": "ml.m5.xlarge",
                "InitialInstanceCount": 2,
            }
        ],
    )
    sagemaker.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=f"{endpoint_name}-config")

    # Mock list_tags to fail
    original_list_tags = sagemaker.list_tags
    sagemaker.list_tags = mocker.Mock(side_effect=Exception("ListTags failed"))

    # Act - Should proceed with default concurrency despite tag failure
    capacity = estimator.estimate_capacity(endpoint_name)

    # Assert - Should return default capacity (2 instances × 2 default concurrency = 4)
    assert capacity == 4

    # Cleanup
    sagemaker.list_tags = original_list_tags


def test_list_tags_failure_uses_cached_fallback(capacity_estimator_setup, mocker):
    """Test that ListTags API failure uses cached tags when available"""
    estimator, sagemaker = capacity_estimator_setup
    endpoint_name = "test-tags-cache-fallback"

    # Create endpoint with tag
    sagemaker.create_model(ModelName=f"{endpoint_name}-model", PrimaryContainer={"Image": "test-image"})
    sagemaker.create_endpoint_config(
        EndpointConfigName=f"{endpoint_name}-config",
        ProductionVariants=[
            {
                "VariantName": "AllTraffic",
                "ModelName": f"{endpoint_name}-model",
                "InstanceType": "ml.m5.xlarge",
                "InitialInstanceCount": 2,
            }
        ],
    )
    sagemaker.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=f"{endpoint_name}-config")

    # Tag the endpoint with custom concurrency
    endpoint_arn = sagemaker.describe_endpoint(EndpointName=endpoint_name)["EndpointArn"]
    sagemaker.add_tags(ResourceArn=endpoint_arn, Tags=[{"Key": "osml:instance-concurrency", "Value": "5"}])

    # First call - populate cache with tags
    first_capacity = estimator.estimate_capacity(endpoint_name)
    assert first_capacity == 10  # 2 instances × 5 tagged concurrency

    # Mock list_tags to fail
    original_list_tags = sagemaker.list_tags
    sagemaker.list_tags = mocker.Mock(side_effect=Exception("ListTags API unavailable"))

    # Clear endpoint metadata cache but keep tags cache
    # This simulates a scenario where tags cache is still valid but endpoint cache expired
    estimator._endpoint_cache.clear()

    # Second call - should use cached tags despite API failure
    second_capacity = estimator.estimate_capacity(endpoint_name)
    assert second_capacity == 10  # Same as before, using cached tags

    # Cleanup
    sagemaker.list_tags = original_list_tags


def test_get_variant_capacity_with_zero_instances(capacity_estimator_setup):
    """Test that endpoint with 0 instances returns 0 capacity"""
    estimator, sagemaker = capacity_estimator_setup
    endpoint_name = "test-zero-instances"

    # Create endpoint with 0 instances (scaled down)
    sagemaker.create_model(ModelName=f"{endpoint_name}-model", PrimaryContainer={"Image": "test-image"})
    sagemaker.create_endpoint_config(
        EndpointConfigName=f"{endpoint_name}-config",
        ProductionVariants=[
            {
                "VariantName": "AllTraffic",
                "ModelName": f"{endpoint_name}-model",
                "InstanceType": "ml.m5.xlarge",
                "InitialInstanceCount": 1,  # Will be manually set to 0
            }
        ],
    )
    sagemaker.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=f"{endpoint_name}-config")

    # Manually modify the endpoint to have 0 instances (simulating scaled down state)
    # Get endpoint metadata and modify it
    original_describe = sagemaker.describe_endpoint

    def mock_describe(*args, **kwargs):
        result = original_describe(*args, **kwargs)
        # Set CurrentInstanceCount to 0
        for variant in result.get("ProductionVariants", []):
            variant["CurrentInstanceCount"] = 0
        return result

    sagemaker.describe_endpoint = mock_describe

    # Act
    capacity = estimator.estimate_capacity(endpoint_name)

    # Assert - Should return 0
    assert capacity == 0

    # Cleanup
    sagemaker.describe_endpoint = original_describe


def test_get_variant_capacity_serverless_with_zero_max_concurrency(capacity_estimator_setup):
    """Test that serverless endpoint with MaxConcurrency=0 returns 0 capacity"""
    estimator, sagemaker = capacity_estimator_setup
    endpoint_name = "test-serverless-zero"

    # Create serverless endpoint
    sagemaker.create_model(ModelName=f"{endpoint_name}-model", PrimaryContainer={"Image": "test-image"})
    sagemaker.create_endpoint_config(
        EndpointConfigName=f"{endpoint_name}-config",
        ProductionVariants=[
            {
                "VariantName": "AllTraffic",
                "ModelName": f"{endpoint_name}-model",
                "ServerlessConfig": {"MemorySizeInMB": 2048, "MaxConcurrency": 10},
            }
        ],
    )
    sagemaker.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=f"{endpoint_name}-config")

    # Mock describe_endpoint to return MaxConcurrency=0
    original_describe = sagemaker.describe_endpoint

    def mock_describe(*args, **kwargs):
        result = original_describe(*args, **kwargs)
        # Set MaxConcurrency to 0
        for variant in result.get("ProductionVariants", []):
            if "CurrentServerlessConfig" in variant:
                variant["CurrentServerlessConfig"]["MaxConcurrency"] = 0
        return result

    sagemaker.describe_endpoint = mock_describe

    # Act
    capacity = estimator.estimate_capacity(endpoint_name)

    # Assert - Should return 0
    assert capacity == 0

    # Cleanup
    sagemaker.describe_endpoint = original_describe


def test_get_variant_capacity_with_invalid_tag_value_uses_default(capacity_estimator_setup):
    """Test that invalid tag value falls back to default concurrency"""
    estimator, sagemaker = capacity_estimator_setup
    endpoint_name = "test-invalid-tag"

    # Create endpoint
    sagemaker.create_model(ModelName=f"{endpoint_name}-model", PrimaryContainer={"Image": "test-image"})
    sagemaker.create_endpoint_config(
        EndpointConfigName=f"{endpoint_name}-config",
        ProductionVariants=[
            {
                "VariantName": "AllTraffic",
                "ModelName": f"{endpoint_name}-model",
                "InstanceType": "ml.m5.xlarge",
                "InitialInstanceCount": 2,
            }
        ],
    )
    sagemaker.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=f"{endpoint_name}-config")

    # Tag with invalid (non-numeric) value
    endpoint_arn = sagemaker.describe_endpoint(EndpointName=endpoint_name)["EndpointArn"]
    sagemaker.add_tags(ResourceArn=endpoint_arn, Tags=[{"Key": "osml:instance-concurrency", "Value": "not-a-number"}])

    # Act
    capacity = estimator.estimate_capacity(endpoint_name)

    # Assert - Should fall back to default: 2 instances × 2 default concurrency = 4
    assert capacity == 4


def test_calculate_capacity_variant_not_found_returns_zero(capacity_estimator_setup):
    """Test that requesting non-existent variant returns 0 capacity"""
    estimator, sagemaker = capacity_estimator_setup
    endpoint_name = "test-variant-not-found"

    # Create endpoint with one variant
    sagemaker.create_model(ModelName=f"{endpoint_name}-model", PrimaryContainer={"Image": "test-image"})
    sagemaker.create_endpoint_config(
        EndpointConfigName=f"{endpoint_name}-config",
        ProductionVariants=[
            {
                "VariantName": "VariantA",
                "ModelName": f"{endpoint_name}-model",
                "InstanceType": "ml.m5.xlarge",
                "InitialInstanceCount": 2,
            }
        ],
    )
    sagemaker.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=f"{endpoint_name}-config")

    # Act - Request non-existent variant
    capacity = estimator.estimate_capacity(endpoint_name, variant_name="NonExistentVariant")

    # Assert - Should return 0
    assert capacity == 0


def test_calculate_capacity_with_empty_variants_list(capacity_estimator_setup):
    """Test that empty variants list returns 0 capacity"""
    estimator, sagemaker = capacity_estimator_setup

    # Test the internal method directly with empty variants
    capacity_all = estimator._calculate_capacity_from_variants([], None, "arn:test")
    capacity_specific = estimator._calculate_capacity_from_variants([], "variant-1", "arn:test")

    # Assert - Both should return 0
    assert capacity_all == 0
    assert capacity_specific == 0


def test_get_endpoint_tags_with_null_arn_returns_empty_dict(capacity_estimator_setup):
    """Test that _get_endpoint_tags handles None or empty ARN gracefully"""
    estimator, sagemaker = capacity_estimator_setup

    # Act
    tags_none = estimator._get_endpoint_tags(None)
    tags_empty = estimator._get_endpoint_tags("")

    # Assert - Both should return empty dict
    assert tags_none == {}
    assert tags_empty == {}


def test_emit_error_metric_handles_exception_gracefully(capacity_estimator_setup, mocker):
    """Test that _emit_error_metric handles exceptions during metric emission"""
    from aws_embedded_metrics.logger.metrics_logger import MetricsLogger

    estimator, sagemaker = capacity_estimator_setup

    # Arrange - Mock metrics that raises exception
    mock_metrics = mocker.MagicMock(spec=MetricsLogger)
    mock_metrics.put_metric.side_effect = RuntimeError("Metrics service unavailable")

    # Act - Should not raise exception
    try:
        estimator._emit_error_metric.__wrapped__(estimator, "test-endpoint", metrics=mock_metrics)
        exception_raised = False
    except RuntimeError:
        exception_raised = True

    # Assert - Exception should be caught and logged, not propagated
    assert not exception_raised, "Exception should be handled gracefully"


# Metrics Emission Tests


@pytest.fixture
def metrics_estimator_setup():
    """Set up test fixtures for metrics emission tests."""
    with mock_aws():
        sagemaker = boto3.client("sagemaker", region_name="us-west-2")
        estimator = EndpointCapacityEstimator(
            sm_client=sagemaker, default_instance_concurrency=2, default_http_concurrency=10, cache_ttl_seconds=300
        )
        yield estimator, sagemaker


def create_mock_metrics_logger(mocker):
    """Create a mock MetricsLogger that passes isinstance checks"""
    from aws_embedded_metrics.logger.metrics_logger import MetricsLogger

    mock_metrics = mocker.MagicMock(spec=MetricsLogger)
    mock_metrics.put_dimensions = mocker.Mock()
    mock_metrics.put_metric = mocker.Mock()
    return mock_metrics


def test_errors_metric_increments_on_api_failures(metrics_estimator_setup, mocker):
    """Test Errors metric (Operation=Scheduling, ModelName=<endpoint>) increments on API failures"""
    from aws_embedded_metrics.unit import Unit

    from aws.osml.model_runner.app_config import MetricLabels

    estimator, sagemaker = metrics_estimator_setup
    mock_metrics = create_mock_metrics_logger(mocker)

    # Call the error metric emission method directly (bypassing decorator)
    estimator._emit_error_metric.__wrapped__(estimator, "nonexistent-endpoint", metrics=mock_metrics)

    mock_metrics.put_dimensions.assert_called_once_with(
        {
            MetricLabels.OPERATION_DIMENSION: MetricLabels.SCHEDULING_OPERATION,
            MetricLabels.MODEL_NAME_DIMENSION: "nonexistent-endpoint",
        }
    )
    mock_metrics.put_metric.assert_called_once_with(MetricLabels.ERRORS, 1, str(Unit.COUNT.value))


def test_errors_metric_not_emitted_for_http_endpoints(metrics_estimator_setup):
    """Test Errors metric is not emitted for HTTP endpoints (no API call needed)"""
    estimator, sagemaker = metrics_estimator_setup

    # HTTP endpoints don't make SageMaker API calls, so no errors can occur
    # Just verify the method works correctly without any metric emission path
    capacity = estimator.estimate_capacity("http://example.com/model")
    assert capacity == 10
