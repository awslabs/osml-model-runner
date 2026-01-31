#  Copyright 2025-2026 Amazon.com, Inc. or its affiliates.

import time
import unittest

import boto3
from moto import mock_aws

from aws.osml.model_runner.scheduler import EndpointCapacityEstimator


@mock_aws
class TestEndpointCapacityEstimator(unittest.TestCase):
    """Test cases for EndpointCapacityEstimator"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.sagemaker = boto3.client("sagemaker", region_name="us-west-2")
        self.estimator = EndpointCapacityEstimator(
            sm_client=self.sagemaker, default_instance_concurrency=2, default_http_concurrency=10, cache_ttl_seconds=300
        )

    def test_http_endpoint_returns_default_concurrency(self):
        """Test that HTTP endpoints return DEFAULT_HTTP_ENDPOINT_CONCURRENCY"""
        # Test http://
        capacity = self.estimator.estimate_capacity("http://example.com/model")
        self.assertEqual(capacity, 10)

        # Test https://
        capacity = self.estimator.estimate_capacity("https://api.example.com/inference")
        self.assertEqual(capacity, 10)

    def test_serverless_endpoint_returns_max_concurrency(self):
        """Test that serverless endpoints return MaxConcurrency"""
        # Create a serverless endpoint
        endpoint_name = "test-serverless-endpoint"

        # Create model
        self.sagemaker.create_model(ModelName=f"{endpoint_name}-model", PrimaryContainer={"Image": "test-image"})

        # Create endpoint config with serverless
        self.sagemaker.create_endpoint_config(
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
        self.sagemaker.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=f"{endpoint_name}-config")

        # Test capacity estimation
        capacity = self.estimator.estimate_capacity(endpoint_name)
        self.assertEqual(capacity, 100)

    def test_instance_backed_endpoint_without_tag(self):
        """Test instance-backed endpoint without osml:instance-concurrency tag"""
        endpoint_name = "test-instance-endpoint"

        # Create model
        self.sagemaker.create_model(ModelName=f"{endpoint_name}-model", PrimaryContainer={"Image": "test-image"})

        # Create endpoint config
        self.sagemaker.create_endpoint_config(
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
        self.sagemaker.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=f"{endpoint_name}-config")

        # Test capacity: 2 instances × 2 default concurrency = 4
        capacity = self.estimator.estimate_capacity(endpoint_name)
        self.assertEqual(capacity, 4)

    def test_instance_backed_endpoint_with_tag(self):
        """Test instance-backed endpoint with osml:instance-concurrency tag"""
        endpoint_name = "test-tagged-endpoint"

        # Create model
        self.sagemaker.create_model(ModelName=f"{endpoint_name}-model", PrimaryContainer={"Image": "test-image"})

        # Create endpoint config
        self.sagemaker.create_endpoint_config(
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
        self.sagemaker.create_endpoint(
            EndpointName=endpoint_name,
            EndpointConfigName=f"{endpoint_name}-config",
            Tags=[{"Key": "osml:instance-concurrency", "Value": "5"}],
        )

        # Test capacity: 3 instances × 5 concurrency = 15
        capacity = self.estimator.estimate_capacity(endpoint_name)
        self.assertEqual(capacity, 15)

    def test_multi_variant_endpoint_all_variants(self):
        """Test multi-variant endpoint with variant_name=None returns sum of all variants"""
        endpoint_name = "test-multi-variant"

        # Create models
        self.sagemaker.create_model(ModelName=f"{endpoint_name}-model-1", PrimaryContainer={"Image": "test-image-1"})
        self.sagemaker.create_model(ModelName=f"{endpoint_name}-model-2", PrimaryContainer={"Image": "test-image-2"})

        # Create endpoint config with multiple variants
        self.sagemaker.create_endpoint_config(
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
        self.sagemaker.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=f"{endpoint_name}-config")

        # Test capacity for all variants: 4 + 6 = 10
        capacity = self.estimator.estimate_capacity(endpoint_name, variant_name=None)
        self.assertEqual(capacity, 10)

    def test_multi_variant_endpoint_specific_variant(self):
        """Test multi-variant endpoint with specific variant_name returns only that variant's capacity"""
        endpoint_name = "test-multi-variant-specific"

        # Create models
        self.sagemaker.create_model(ModelName=f"{endpoint_name}-model-1", PrimaryContainer={"Image": "test-image-1"})
        self.sagemaker.create_model(ModelName=f"{endpoint_name}-model-2", PrimaryContainer={"Image": "test-image-2"})

        # Create endpoint config with multiple variants
        self.sagemaker.create_endpoint_config(
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
        self.sagemaker.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=f"{endpoint_name}-config")

        # Test capacity for variant-1 only: 2 × 2 = 4
        capacity = self.estimator.estimate_capacity(endpoint_name, variant_name="variant-1")
        self.assertEqual(capacity, 4)

        # Test capacity for variant-2 only: 3 × 2 = 6
        capacity = self.estimator.estimate_capacity(endpoint_name, variant_name="variant-2")
        self.assertEqual(capacity, 6)

    def test_capacity_caching_reduces_api_calls(self):
        """Test that capacity caching reduces SageMaker API calls"""
        endpoint_name = "test-cache-endpoint"

        # Create model
        self.sagemaker.create_model(ModelName=f"{endpoint_name}-model", PrimaryContainer={"Image": "test-image"})

        # Create endpoint config
        self.sagemaker.create_endpoint_config(
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
        self.sagemaker.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=f"{endpoint_name}-config")

        # Mock the describe_endpoint call to count invocations
        original_describe = self.sagemaker.describe_endpoint
        call_count = {"count": 0}

        def counting_describe(*args, **kwargs):
            call_count["count"] += 1
            return original_describe(*args, **kwargs)

        self.sagemaker.describe_endpoint = counting_describe

        # First call - should hit API
        capacity1 = self.estimator.estimate_capacity(endpoint_name)
        self.assertEqual(call_count["count"], 1)

        # Second call - should use cache (TTLCache)
        capacity2 = self.estimator.estimate_capacity(endpoint_name)
        self.assertEqual(call_count["count"], 1)  # No additional API call

        # Capacities should be the same
        self.assertEqual(capacity1, capacity2)

        # Verify cache size is bounded
        self.assertLessEqual(len(self.estimator._endpoint_cache), 100)  # max_size default

    def test_cache_expiration_after_ttl(self):
        """Test that cache expires after TTL using TTLCache"""
        # Create estimator with short TTL
        short_ttl_estimator = EndpointCapacityEstimator(
            sm_client=self.sagemaker,
            default_instance_concurrency=2,
            default_http_concurrency=10,
            cache_ttl_seconds=1,  # 1 second TTL
        )

        endpoint_name = "test-ttl-endpoint"

        # Create model
        self.sagemaker.create_model(ModelName=f"{endpoint_name}-model", PrimaryContainer={"Image": "test-image"})

        # Create endpoint config
        self.sagemaker.create_endpoint_config(
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
        self.sagemaker.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=f"{endpoint_name}-config")

        # Mock the describe_endpoint call to count invocations
        original_describe = self.sagemaker.describe_endpoint
        call_count = {"count": 0}

        def counting_describe(*args, **kwargs):
            call_count["count"] += 1
            return original_describe(*args, **kwargs)

        self.sagemaker.describe_endpoint = counting_describe

        # First call - should hit API
        short_ttl_estimator.estimate_capacity(endpoint_name)
        self.assertEqual(call_count["count"], 1)

        # Wait for cache to expire (TTLCache automatically removes expired items)
        time.sleep(1.1)

        # Second call after TTL - should hit API again
        short_ttl_estimator.estimate_capacity(endpoint_name)
        self.assertEqual(call_count["count"], 2)

    def test_cache_bounded_size(self):
        """Test that cache size is bounded and uses LRU eviction"""
        # Create estimator with small cache size
        small_cache_estimator = EndpointCapacityEstimator(
            sm_client=self.sagemaker,
            default_instance_concurrency=2,
            default_http_concurrency=10,
            cache_ttl_seconds=300,
            cache_max_size=3,  # Only cache 3 endpoints
        )

        # Create 5 endpoints
        for i in range(5):
            endpoint_name = f"test-endpoint-{i}"
            self.sagemaker.create_model(ModelName=f"{endpoint_name}-model", PrimaryContainer={"Image": "test-image"})
            self.sagemaker.create_endpoint_config(
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
            self.sagemaker.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=f"{endpoint_name}-config")

        # Query all 5 endpoints
        for i in range(5):
            small_cache_estimator.estimate_capacity(f"test-endpoint-{i}")

        # Cache should only contain 3 items (most recent due to LRU)
        self.assertEqual(len(small_cache_estimator._endpoint_cache), 3)

        # The most recent 3 should be cached (endpoints 2, 3, 4)
        self.assertIn("test-endpoint-2", small_cache_estimator._endpoint_cache)
        self.assertIn("test-endpoint-3", small_cache_estimator._endpoint_cache)
        self.assertIn("test-endpoint-4", small_cache_estimator._endpoint_cache)

        # The oldest 2 should have been evicted (endpoints 0, 1)
        self.assertNotIn("test-endpoint-0", small_cache_estimator._endpoint_cache)
        self.assertNotIn("test-endpoint-1", small_cache_estimator._endpoint_cache)

    def test_estimate_capacity_describe_endpoint_fails_returns_default(self):
        """Test that API failures return default_instance_concurrency when no cache available"""
        from unittest.mock import Mock

        # Arrange - Mock describe_endpoint to raise exception
        original_describe = self.sagemaker.describe_endpoint
        self.sagemaker.describe_endpoint = Mock(side_effect=Exception("API Error"))

        # Act
        capacity = self.estimator.estimate_capacity("nonexistent-endpoint")

        # Assert - Should return default_instance_concurrency
        self.assertEqual(capacity, 2)  # default_instance_concurrency=2

        # Cleanup
        self.sagemaker.describe_endpoint = original_describe

    def test_estimate_capacity_api_fails_uses_cached_fallback(self):
        """Test that API failures use cached data when available"""
        from unittest.mock import Mock

        endpoint_name = "test-cache-fallback"

        # Create endpoint to populate cache
        self.sagemaker.create_model(ModelName=f"{endpoint_name}-model", PrimaryContainer={"Image": "test-image"})
        self.sagemaker.create_endpoint_config(
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
        self.sagemaker.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=f"{endpoint_name}-config")

        # First call - populate cache
        first_capacity = self.estimator.estimate_capacity(endpoint_name)
        self.assertEqual(first_capacity, 6)  # 3 instances × 2 default concurrency

        # Mock API to fail
        original_describe = self.sagemaker.describe_endpoint
        self.sagemaker.describe_endpoint = Mock(side_effect=Exception("API temporarily unavailable"))

        # Second call - should use cached data
        second_capacity = self.estimator.estimate_capacity(endpoint_name)
        self.assertEqual(second_capacity, 6)  # Same as cached value

        # Cleanup
        self.sagemaker.describe_endpoint = original_describe

    def test_list_tags_failure_returns_empty_dict(self):
        """Test that ListTags API failure returns empty dict when no cache"""
        from unittest.mock import Mock

        endpoint_name = "test-tags-failure"

        # Create endpoint
        self.sagemaker.create_model(ModelName=f"{endpoint_name}-model", PrimaryContainer={"Image": "test-image"})
        self.sagemaker.create_endpoint_config(
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
        self.sagemaker.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=f"{endpoint_name}-config")

        # Mock list_tags to fail
        original_list_tags = self.sagemaker.list_tags
        self.sagemaker.list_tags = Mock(side_effect=Exception("ListTags failed"))

        # Act - Should proceed with default concurrency despite tag failure
        capacity = self.estimator.estimate_capacity(endpoint_name)

        # Assert - Should return default capacity (2 instances × 2 default concurrency = 4)
        self.assertEqual(capacity, 4)

        # Cleanup
        self.sagemaker.list_tags = original_list_tags

    def test_list_tags_failure_uses_cached_fallback(self):
        """Test that ListTags API failure uses cached tags when available"""
        from unittest.mock import Mock

        endpoint_name = "test-tags-cache-fallback"

        # Create endpoint with tag
        self.sagemaker.create_model(ModelName=f"{endpoint_name}-model", PrimaryContainer={"Image": "test-image"})
        self.sagemaker.create_endpoint_config(
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
        self.sagemaker.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=f"{endpoint_name}-config")

        # Tag the endpoint with custom concurrency
        endpoint_arn = self.sagemaker.describe_endpoint(EndpointName=endpoint_name)["EndpointArn"]
        self.sagemaker.add_tags(ResourceArn=endpoint_arn, Tags=[{"Key": "osml:instance-concurrency", "Value": "5"}])

        # First call - populate cache with tags
        first_capacity = self.estimator.estimate_capacity(endpoint_name)
        self.assertEqual(first_capacity, 10)  # 2 instances × 5 tagged concurrency

        # Mock list_tags to fail
        original_list_tags = self.sagemaker.list_tags
        self.sagemaker.list_tags = Mock(side_effect=Exception("ListTags API unavailable"))

        # Clear endpoint metadata cache but keep tags cache
        # This simulates a scenario where tags cache is still valid but endpoint cache expired
        self.estimator._endpoint_cache.clear()

        # Second call - should use cached tags despite API failure
        second_capacity = self.estimator.estimate_capacity(endpoint_name)
        self.assertEqual(second_capacity, 10)  # Same as before, using cached tags

        # Cleanup
        self.sagemaker.list_tags = original_list_tags

    def test_get_variant_capacity_with_zero_instances(self):
        """Test that endpoint with 0 instances returns 0 capacity"""
        endpoint_name = "test-zero-instances"

        # Create endpoint with 0 instances (scaled down)
        self.sagemaker.create_model(ModelName=f"{endpoint_name}-model", PrimaryContainer={"Image": "test-image"})
        self.sagemaker.create_endpoint_config(
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
        self.sagemaker.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=f"{endpoint_name}-config")

        # Manually modify the endpoint to have 0 instances (simulating scaled down state)
        # Get endpoint metadata and modify it
        original_describe = self.sagemaker.describe_endpoint

        def mock_describe(*args, **kwargs):
            result = original_describe(*args, **kwargs)
            # Set CurrentInstanceCount to 0
            for variant in result.get("ProductionVariants", []):
                variant["CurrentInstanceCount"] = 0
            return result

        self.sagemaker.describe_endpoint = mock_describe

        # Act
        capacity = self.estimator.estimate_capacity(endpoint_name)

        # Assert - Should return 0
        self.assertEqual(capacity, 0)

        # Cleanup
        self.sagemaker.describe_endpoint = original_describe

    def test_get_variant_capacity_serverless_with_zero_max_concurrency(self):
        """Test that serverless endpoint with MaxConcurrency=0 returns 0 capacity"""
        endpoint_name = "test-serverless-zero"

        # Create serverless endpoint
        self.sagemaker.create_model(ModelName=f"{endpoint_name}-model", PrimaryContainer={"Image": "test-image"})
        self.sagemaker.create_endpoint_config(
            EndpointConfigName=f"{endpoint_name}-config",
            ProductionVariants=[
                {
                    "VariantName": "AllTraffic",
                    "ModelName": f"{endpoint_name}-model",
                    "ServerlessConfig": {"MemorySizeInMB": 2048, "MaxConcurrency": 10},
                }
            ],
        )
        self.sagemaker.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=f"{endpoint_name}-config")

        # Mock describe_endpoint to return MaxConcurrency=0
        original_describe = self.sagemaker.describe_endpoint

        def mock_describe(*args, **kwargs):
            result = original_describe(*args, **kwargs)
            # Set MaxConcurrency to 0
            for variant in result.get("ProductionVariants", []):
                if "CurrentServerlessConfig" in variant:
                    variant["CurrentServerlessConfig"]["MaxConcurrency"] = 0
            return result

        self.sagemaker.describe_endpoint = mock_describe

        # Act
        capacity = self.estimator.estimate_capacity(endpoint_name)

        # Assert - Should return 0
        self.assertEqual(capacity, 0)

        # Cleanup
        self.sagemaker.describe_endpoint = original_describe

    def test_get_variant_capacity_with_invalid_tag_value_uses_default(self):
        """Test that invalid tag value falls back to default concurrency"""
        endpoint_name = "test-invalid-tag"

        # Create endpoint
        self.sagemaker.create_model(ModelName=f"{endpoint_name}-model", PrimaryContainer={"Image": "test-image"})
        self.sagemaker.create_endpoint_config(
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
        self.sagemaker.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=f"{endpoint_name}-config")

        # Tag with invalid (non-numeric) value
        endpoint_arn = self.sagemaker.describe_endpoint(EndpointName=endpoint_name)["EndpointArn"]
        self.sagemaker.add_tags(
            ResourceArn=endpoint_arn, Tags=[{"Key": "osml:instance-concurrency", "Value": "not-a-number"}]
        )

        # Act
        capacity = self.estimator.estimate_capacity(endpoint_name)

        # Assert - Should fall back to default: 2 instances × 2 default concurrency = 4
        self.assertEqual(capacity, 4)

    def test_calculate_capacity_variant_not_found_returns_zero(self):
        """Test that requesting non-existent variant returns 0 capacity"""
        endpoint_name = "test-variant-not-found"

        # Create endpoint with one variant
        self.sagemaker.create_model(ModelName=f"{endpoint_name}-model", PrimaryContainer={"Image": "test-image"})
        self.sagemaker.create_endpoint_config(
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
        self.sagemaker.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=f"{endpoint_name}-config")

        # Act - Request non-existent variant
        capacity = self.estimator.estimate_capacity(endpoint_name, variant_name="NonExistentVariant")

        # Assert - Should return 0
        self.assertEqual(capacity, 0)

    def test_calculate_capacity_with_empty_variants_list(self):
        """Test that empty variants list returns 0 capacity"""
        # Test the internal method directly with empty variants
        capacity_all = self.estimator._calculate_capacity_from_variants([], None, "arn:test")
        capacity_specific = self.estimator._calculate_capacity_from_variants([], "variant-1", "arn:test")

        # Assert - Both should return 0
        self.assertEqual(capacity_all, 0)
        self.assertEqual(capacity_specific, 0)

    def test_get_endpoint_tags_with_null_arn_returns_empty_dict(self):
        """Test that _get_endpoint_tags handles None or empty ARN gracefully"""
        # Act
        tags_none = self.estimator._get_endpoint_tags(None)
        tags_empty = self.estimator._get_endpoint_tags("")

        # Assert - Both should return empty dict
        self.assertEqual(tags_none, {})
        self.assertEqual(tags_empty, {})

    def test_emit_error_metric_handles_exception_gracefully(self):
        """Test that _emit_error_metric handles exceptions during metric emission"""
        from unittest.mock import MagicMock

        from aws_embedded_metrics.logger.metrics_logger import MetricsLogger

        # Arrange - Mock metrics that raises exception
        mock_metrics = MagicMock(spec=MetricsLogger)
        mock_metrics.put_metric.side_effect = RuntimeError("Metrics service unavailable")

        # Act - Should not raise exception
        try:
            self.estimator._emit_error_metric.__wrapped__(self.estimator, "test-endpoint", metrics=mock_metrics)
            exception_raised = False
        except RuntimeError:
            exception_raised = True

        # Assert - Exception should be caught and logged, not propagated
        self.assertFalse(exception_raised, "Exception should be handled gracefully")


if __name__ == "__main__":
    unittest.main()


@mock_aws
class TestEndpointCapacityEstimatorMetricsEmission(unittest.TestCase):
    """Test cases for metrics emission in EndpointCapacityEstimator"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.sagemaker = boto3.client("sagemaker", region_name="us-west-2")
        self.estimator = EndpointCapacityEstimator(
            sm_client=self.sagemaker, default_instance_concurrency=2, default_http_concurrency=10, cache_ttl_seconds=300
        )

    def create_mock_metrics_logger(self):
        """Create a mock MetricsLogger that passes isinstance checks"""
        from unittest.mock import MagicMock, Mock

        from aws_embedded_metrics.logger.metrics_logger import MetricsLogger

        mock_metrics = MagicMock(spec=MetricsLogger)
        mock_metrics.put_dimensions = Mock()
        mock_metrics.put_metric = Mock()
        return mock_metrics

    def test_errors_metric_increments_on_api_failures(self):
        """Test Errors metric (Operation=Scheduling, ModelName=<endpoint>) increments on API failures"""
        from aws_embedded_metrics.unit import Unit

        from aws.osml.model_runner.app_config import MetricLabels

        mock_metrics = self.create_mock_metrics_logger()

        # Call the error metric emission method directly (bypassing decorator)
        self.estimator._emit_error_metric.__wrapped__(self.estimator, "nonexistent-endpoint", metrics=mock_metrics)

        mock_metrics.put_dimensions.assert_called_once_with(
            {
                MetricLabels.OPERATION_DIMENSION: MetricLabels.SCHEDULING_OPERATION,
                MetricLabels.MODEL_NAME_DIMENSION: "nonexistent-endpoint",
            }
        )
        mock_metrics.put_metric.assert_called_once_with(MetricLabels.ERRORS, 1, str(Unit.COUNT.value))

    def test_errors_metric_not_emitted_for_http_endpoints(self):
        """Test Errors metric is not emitted for HTTP endpoints (no API call needed)"""
        # HTTP endpoints don't make SageMaker API calls, so no errors can occur
        # Just verify the method works correctly without any metric emission path
        capacity = self.estimator.estimate_capacity("http://example.com/model")
        self.assertEqual(capacity, 10)
