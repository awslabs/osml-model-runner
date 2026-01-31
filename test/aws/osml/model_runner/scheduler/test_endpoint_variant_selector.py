#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

import time
import unittest

import boto3
from moto import mock_aws

from aws.osml.model_runner.api import ImageRequest, ModelInvokeMode
from aws.osml.model_runner.scheduler import EndpointVariantSelector


@mock_aws
class TestEndpointVariantSelector(unittest.TestCase):
    """Test cases for EndpointVariantSelector"""

    def setUp(self):
        """Set up test fixtures before each test method."""
        self.sagemaker = boto3.client("sagemaker", region_name="us-west-2")
        self.selector = EndpointVariantSelector(sm_client=self.sagemaker, cache_ttl_seconds=300)

    def test_http_endpoint_returns_unchanged(self):
        """Test that HTTP endpoints return request unchanged (no variants)"""
        # Test with http://
        request = ImageRequest(
            job_id="test-job",
            image_id="test-image",
            image_url="s3://bucket/image.tif",
            model_name="http://example.com/model",
            model_invoke_mode=ModelInvokeMode.HTTP_ENDPOINT,
        )

        result = self.selector.select_variant(request)
        self.assertEqual(result, request)
        self.assertIsNone(result.model_endpoint_parameters or {}.get("TargetVariant"))

        # Test with https://
        request = ImageRequest(
            job_id="test-job",
            image_id="test-image",
            image_url="s3://bucket/image.tif",
            model_name="https://api.example.com/inference",
            model_invoke_mode=ModelInvokeMode.HTTP_ENDPOINT,
        )

        result = self.selector.select_variant(request)
        self.assertEqual(result, request)
        self.assertIsNone(result.model_endpoint_parameters or {}.get("TargetVariant"))

    def test_explicit_target_variant_is_honored(self):
        """Test that explicit TargetVariant is never overridden"""
        endpoint_name = "test-endpoint"

        # Create a multi-variant endpoint
        self._create_multi_variant_endpoint(endpoint_name)

        # Create request with explicit TargetVariant
        request = ImageRequest(
            job_id="test-job",
            image_id="test-image",
            image_url="s3://bucket/image.tif",
            model_name=endpoint_name,
            model_invoke_mode=ModelInvokeMode.SM_ENDPOINT,
            model_endpoint_parameters={"TargetVariant": "variant-explicit"},
        )

        result = self.selector.select_variant(request)
        self.assertEqual(result.model_endpoint_parameters["TargetVariant"], "variant-explicit")

    def test_single_variant_endpoint_returns_that_variant(self):
        """Test that single variant endpoint returns that variant"""
        endpoint_name = "test-single-variant"

        # Create model
        self.sagemaker.create_model(ModelName=f"{endpoint_name}-model", PrimaryContainer={"Image": "test-image"})

        # Create endpoint config with single variant
        self.sagemaker.create_endpoint_config(
            EndpointConfigName=f"{endpoint_name}-config",
            ProductionVariants=[
                {
                    "VariantName": "OnlyVariant",
                    "ModelName": f"{endpoint_name}-model",
                    "InstanceType": "ml.m5.xlarge",
                    "InitialInstanceCount": 1,
                    "InitialVariantWeight": 1.0,
                }
            ],
        )

        # Create endpoint
        self.sagemaker.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=f"{endpoint_name}-config")

        # Create request without TargetVariant
        request = ImageRequest(
            job_id="test-job",
            image_id="test-image",
            image_url="s3://bucket/image.tif",
            model_name=endpoint_name,
            model_invoke_mode=ModelInvokeMode.SM_ENDPOINT,
        )

        result = self.selector.select_variant(request)
        self.assertEqual(result.model_endpoint_parameters["TargetVariant"], "OnlyVariant")

    def test_multi_variant_with_equal_weights(self):
        """Test multi-variant endpoint with equal weights (50/50 split)"""
        endpoint_name = "test-equal-weights"

        # Create endpoint with two variants with equal weights
        self._create_multi_variant_endpoint(endpoint_name, weights=[1.0, 1.0])

        # Perform many selections to test distribution
        variant_counts = {"variant-1": 0, "variant-2": 0}
        num_selections = 1000

        for _ in range(num_selections):
            request = ImageRequest(
                job_id="test-job",
                image_id="test-image",
                image_url="s3://bucket/image.tif",
                model_name=endpoint_name,
                model_invoke_mode=ModelInvokeMode.SM_ENDPOINT,
            )

            result = self.selector.select_variant(request)
            selected_variant = result.model_endpoint_parameters["TargetVariant"]
            variant_counts[selected_variant] += 1

        # With equal weights, we expect roughly 50/50 distribution
        # Allow for statistical variance (40-60% range)
        self.assertGreater(variant_counts["variant-1"], 400)
        self.assertLess(variant_counts["variant-1"], 600)
        self.assertGreater(variant_counts["variant-2"], 400)
        self.assertLess(variant_counts["variant-2"], 600)

    def test_multi_variant_with_unequal_weights(self):
        """Test multi-variant endpoint with unequal weights (80/20 split)"""
        endpoint_name = "test-unequal-weights"

        # Create endpoint with two variants with unequal weights
        self._create_multi_variant_endpoint(endpoint_name, weights=[0.8, 0.2])

        # Perform many selections to test distribution
        variant_counts = {"variant-1": 0, "variant-2": 0}
        num_selections = 1000

        for _ in range(num_selections):
            request = ImageRequest(
                job_id="test-job",
                image_id="test-image",
                image_url="s3://bucket/image.tif",
                model_name=endpoint_name,
                model_invoke_mode=ModelInvokeMode.SM_ENDPOINT,
            )

            result = self.selector.select_variant(request)
            selected_variant = result.model_endpoint_parameters["TargetVariant"]
            variant_counts[selected_variant] += 1

        # With 80/20 weights, we expect roughly 80/20 distribution
        # Allow for statistical variance (70-90% for variant-1, 10-30% for variant-2)
        self.assertGreater(variant_counts["variant-1"], 700)
        self.assertLess(variant_counts["variant-1"], 900)
        self.assertGreater(variant_counts["variant-2"], 100)
        self.assertLess(variant_counts["variant-2"], 300)

    def test_variant_caching_reduces_api_calls(self):
        """Test that variant caching reduces SageMaker API calls"""
        endpoint_name = "test-caching"

        # Create endpoint
        self._create_multi_variant_endpoint(endpoint_name)

        # Mock the describe_endpoint call to count invocations
        original_describe = self.sagemaker.describe_endpoint
        call_count = [0]

        def counting_describe(*args, **kwargs):
            call_count[0] += 1
            return original_describe(*args, **kwargs)

        self.sagemaker.describe_endpoint = counting_describe

        # Make multiple selections - should only call API once
        for _ in range(5):
            request = ImageRequest(
                job_id="test-job",
                image_id="test-image",
                image_url="s3://bucket/image.tif",
                model_name=endpoint_name,
                model_invoke_mode=ModelInvokeMode.SM_ENDPOINT,
            )
            self.selector.select_variant(request)

        # Should have called API only once due to caching
        self.assertEqual(call_count[0], 1)

    def test_cache_expiration_after_ttl(self):
        """Test that cache expires after TTL"""
        endpoint_name = "test-cache-expiry"

        # Create selector with short TTL
        short_ttl_selector = EndpointVariantSelector(sm_client=self.sagemaker, cache_ttl_seconds=1)

        # Create endpoint
        self._create_multi_variant_endpoint(endpoint_name)

        # Mock the describe_endpoint call to count invocations
        original_describe = self.sagemaker.describe_endpoint
        call_count = [0]

        def counting_describe(*args, **kwargs):
            call_count[0] += 1
            return original_describe(*args, **kwargs)

        self.sagemaker.describe_endpoint = counting_describe

        # First selection
        request = ImageRequest(
            job_id="test-job",
            image_id="test-image",
            image_url="s3://bucket/image.tif",
            model_name=endpoint_name,
            model_invoke_mode=ModelInvokeMode.SM_ENDPOINT,
        )
        short_ttl_selector.select_variant(request)
        self.assertEqual(call_count[0], 1)

        # Wait for cache to expire
        time.sleep(1.1)

        # Second selection should trigger new API call
        request = ImageRequest(
            job_id="test-job",
            image_id="test-image",
            image_url="s3://bucket/image.tif",
            model_name=endpoint_name,
            model_invoke_mode=ModelInvokeMode.SM_ENDPOINT,
        )
        short_ttl_selector.select_variant(request)
        self.assertEqual(call_count[0], 2)

    def test_weighted_random_selection_uses_current_weight(self):
        """Test that weighted random selection uses CurrentWeight correctly"""
        endpoint_name = "test-weights"

        # Create endpoint with three variants with different weights
        self._create_multi_variant_endpoint(endpoint_name, weights=[0.5, 0.3, 0.2], num_variants=3)

        # Perform many selections to test distribution
        variant_counts = {"variant-1": 0, "variant-2": 0, "variant-3": 0}
        num_selections = 1000

        for _ in range(num_selections):
            request = ImageRequest(
                job_id="test-job",
                image_id="test-image",
                image_url="s3://bucket/image.tif",
                model_name=endpoint_name,
                model_invoke_mode=ModelInvokeMode.SM_ENDPOINT,
            )

            result = self.selector.select_variant(request)
            selected_variant = result.model_endpoint_parameters["TargetVariant"]
            variant_counts[selected_variant] += 1

        # Verify distribution approximates weights (with statistical tolerance)
        # variant-1: 50% ± 10% = 400-600
        # variant-2: 30% ± 10% = 200-400
        # variant-3: 20% ± 10% = 100-300
        self.assertGreater(variant_counts["variant-1"], 400)
        self.assertLess(variant_counts["variant-1"], 600)
        self.assertGreater(variant_counts["variant-2"], 200)
        self.assertLess(variant_counts["variant-2"], 400)
        self.assertGreater(variant_counts["variant-3"], 100)
        self.assertLess(variant_counts["variant-3"], 300)

    def test_empty_target_variant_string_triggers_selection(self):
        """Test that empty string TargetVariant triggers selection"""
        endpoint_name = "test-empty-variant"

        # Create endpoint
        self._create_multi_variant_endpoint(endpoint_name)

        # Create request with empty TargetVariant
        request = ImageRequest(
            job_id="test-job",
            image_id="test-image",
            image_url="s3://bucket/image.tif",
            model_name=endpoint_name,
            model_invoke_mode=ModelInvokeMode.SM_ENDPOINT,
            model_endpoint_parameters={"TargetVariant": ""},
        )

        result = self.selector.select_variant(request)
        # Should have selected a variant
        self.assertIn(result.model_endpoint_parameters["TargetVariant"], ["variant-1", "variant-2"])
        self.assertNotEqual(result.model_endpoint_parameters["TargetVariant"], "")

    def test_none_model_endpoint_parameters_creates_dict(self):
        """Test that None model_endpoint_parameters creates new dict"""
        endpoint_name = "test-none-params"

        # Create endpoint
        self._create_multi_variant_endpoint(endpoint_name)

        # Create request with None model_endpoint_parameters
        request = ImageRequest(
            job_id="test-job",
            image_id="test-image",
            image_url="s3://bucket/image.tif",
            model_name=endpoint_name,
            model_invoke_mode=ModelInvokeMode.SM_ENDPOINT,
            model_endpoint_parameters=None,
        )

        result = self.selector.select_variant(request)
        # Should have created dict and selected a variant
        self.assertIsNotNone(result.model_endpoint_parameters)
        self.assertIn(result.model_endpoint_parameters["TargetVariant"], ["variant-1", "variant-2"])

    def test_get_endpoint_variants_api_failure_uses_stale_cache(self):
        """Test that API failure falls back to stale cache."""
        from unittest.mock import MagicMock

        from cachetools import TTLCache

        endpoint_name = "test-cache-fallback"

        # Create new selector
        test_selector = EndpointVariantSelector(sm_client=self.sagemaker, cache_ttl_seconds=300)

        # Create a real TTLCache and populate it
        cached_variants = [{"VariantName": "cached-variant-1", "CurrentWeight": 1.0}]
        test_selector._endpoint_cache[endpoint_name] = cached_variants

        # Mock the cache's __contains__ to simulate edge case:
        # - First call (line 129) returns False (expired)
        # - Second call (line 149) returns True (still accessible for fallback)
        mock_cache = MagicMock(spec=TTLCache)
        mock_cache.__getitem__ = lambda self, key: cached_variants

        contains_calls = [0]

        def mock_contains(self_param, key):
            contains_calls[0] += 1
            if contains_calls[0] == 1:
                return False  # First check fails
            return True  # Fallback check succeeds

        mock_cache.__contains__ = mock_contains
        test_selector._endpoint_cache = mock_cache

        # Mock describe_endpoint to fail
        test_selector.sm_client.describe_endpoint = lambda **kwargs: (_ for _ in ()).throw(Exception("API fail"))

        # Act
        variants = test_selector._get_endpoint_variants(endpoint_name)

        # Assert - returned cached variants
        self.assertEqual(len(variants), 1)
        self.assertEqual(variants[0]["VariantName"], "cached-variant-1")
        # Verify __contains__ was called twice
        self.assertEqual(contains_calls[0], 2)

    def test_select_weighted_variant_empty_list_raises_value_error(self):
        """Test _select_weighted_variant with empty list raises ValueError."""
        # Act / Assert
        with self.assertRaises(ValueError) as context:
            self.selector._select_weighted_variant([])

        self.assertIn("empty", str(context.exception).lower())

    def test_select_variant_no_variants_logs_warning_returns_unchanged(self):
        """Test select_variant with no variants logs warning and returns request unchanged."""
        endpoint_name = "test-no-variants"

        # Create a selector and mock _get_endpoint_variants to return empty list
        test_selector = EndpointVariantSelector(sm_client=self.sagemaker, cache_ttl_seconds=300)

        def mock_get_variants(name):
            return []  # Return empty variants list

        test_selector._get_endpoint_variants = mock_get_variants

        # Create request
        request = ImageRequest(
            job_id="test-job",
            image_id="test-image",
            image_url="s3://bucket/image.tif",
            model_name=endpoint_name,
            model_invoke_mode=ModelInvokeMode.SM_ENDPOINT,
        )

        # Act - should log warning and return unchanged
        import logging

        with self.assertLogs(level=logging.WARNING) as log:
            result = test_selector.select_variant(request)

            # Assert - warning logged
            self.assertTrue(any("No variants" in message for message in log.output))

        # Assert - request returned unchanged (no TargetVariant set)
        self.assertEqual(result, request)
        # If model_endpoint_parameters was None, it should remain None
        # If it was dict, TargetVariant should not be set
        if result.model_endpoint_parameters is not None:
            self.assertNotIn("TargetVariant", result.model_endpoint_parameters)

    def _create_multi_variant_endpoint(self, endpoint_name: str, weights=None, num_variants: int = 2):
        """
        Helper method to create a multi-variant endpoint.

        :param endpoint_name: Name of the endpoint
        :param weights: List of weights for variants (default: equal weights)
        :param num_variants: Number of variants to create
        """
        if weights is None:
            weights = [1.0] * num_variants

        # Create models
        for i in range(1, num_variants + 1):
            self.sagemaker.create_model(
                ModelName=f"{endpoint_name}-model-{i}", PrimaryContainer={"Image": f"test-image-{i}"}
            )

        # Create endpoint config with multiple variants
        variants = []
        for i in range(1, num_variants + 1):
            variants.append(
                {
                    "VariantName": f"variant-{i}",
                    "ModelName": f"{endpoint_name}-model-{i}",
                    "InstanceType": "ml.m5.xlarge",
                    "InitialInstanceCount": 1,
                    "InitialVariantWeight": weights[i - 1],
                }
            )

        self.sagemaker.create_endpoint_config(EndpointConfigName=f"{endpoint_name}-config", ProductionVariants=variants)

        # Create endpoint
        self.sagemaker.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=f"{endpoint_name}-config")


if __name__ == "__main__":
    unittest.main()
