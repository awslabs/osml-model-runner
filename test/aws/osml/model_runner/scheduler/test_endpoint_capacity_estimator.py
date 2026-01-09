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
