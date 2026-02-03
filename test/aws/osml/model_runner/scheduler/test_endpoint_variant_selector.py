#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

import time

import boto3
import pytest
from moto import mock_aws

from aws.osml.model_runner.api import ImageRequest, ModelInvokeMode
from aws.osml.model_runner.scheduler import EndpointVariantSelector


def create_multi_variant_endpoint(sagemaker_client, endpoint_name: str, weights=None, num_variants: int = 2):
    """
    Helper function to create a multi-variant endpoint.

    :param sagemaker_client: SageMaker client
    :param endpoint_name: Name of the endpoint
    :param weights: List of weights for variants (default: equal weights)
    :param num_variants: Number of variants to create
    """
    if weights is None:
        weights = [1.0] * num_variants

    # Create models
    for i in range(1, num_variants + 1):
        sagemaker_client.create_model(ModelName=f"{endpoint_name}-model-{i}", PrimaryContainer={"Image": f"test-image-{i}"})

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

    sagemaker_client.create_endpoint_config(EndpointConfigName=f"{endpoint_name}-config", ProductionVariants=variants)

    # Create endpoint
    sagemaker_client.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=f"{endpoint_name}-config")


@pytest.fixture
def variant_selector_setup():
    """Set up test fixtures before each test method."""
    with mock_aws():
        sagemaker = boto3.client("sagemaker", region_name="us-west-2")
        selector = EndpointVariantSelector(sm_client=sagemaker, cache_ttl_seconds=300)
        yield selector, sagemaker


def test_http_endpoint_returns_unchanged(variant_selector_setup):
    """Test that HTTP endpoints return request unchanged (no variants)"""
    selector, sagemaker = variant_selector_setup

    # Test with http://
    request = ImageRequest(
        job_id="test-job",
        image_id="test-image",
        image_url="s3://bucket/image.tif",
        model_name="http://example.com/model",
        model_invoke_mode=ModelInvokeMode.HTTP_ENDPOINT,
    )

    result = selector.select_variant(request)
    assert result == request
    assert (result.model_endpoint_parameters or {}).get("TargetVariant") is None

    # Test with https://
    request = ImageRequest(
        job_id="test-job",
        image_id="test-image",
        image_url="s3://bucket/image.tif",
        model_name="https://api.example.com/inference",
        model_invoke_mode=ModelInvokeMode.HTTP_ENDPOINT,
    )

    result = selector.select_variant(request)
    assert result == request
    assert (result.model_endpoint_parameters or {}).get("TargetVariant") is None


def test_explicit_target_variant_is_honored(variant_selector_setup):
    """Test that explicit TargetVariant is never overridden"""
    selector, sagemaker = variant_selector_setup
    endpoint_name = "test-endpoint"

    # Create a multi-variant endpoint
    create_multi_variant_endpoint(sagemaker, endpoint_name)

    # Create request with explicit TargetVariant
    request = ImageRequest(
        job_id="test-job",
        image_id="test-image",
        image_url="s3://bucket/image.tif",
        model_name=endpoint_name,
        model_invoke_mode=ModelInvokeMode.SM_ENDPOINT,
        model_endpoint_parameters={"TargetVariant": "variant-explicit"},
    )

    result = selector.select_variant(request)
    assert result.model_endpoint_parameters["TargetVariant"] == "variant-explicit"


def test_single_variant_endpoint_returns_that_variant(variant_selector_setup):
    """Test that single variant endpoint returns that variant"""
    selector, sagemaker = variant_selector_setup
    endpoint_name = "test-single-variant"

    # Create model
    sagemaker.create_model(ModelName=f"{endpoint_name}-model", PrimaryContainer={"Image": "test-image"})

    # Create endpoint config with single variant
    sagemaker.create_endpoint_config(
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
    sagemaker.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=f"{endpoint_name}-config")

    # Create request without TargetVariant
    request = ImageRequest(
        job_id="test-job",
        image_id="test-image",
        image_url="s3://bucket/image.tif",
        model_name=endpoint_name,
        model_invoke_mode=ModelInvokeMode.SM_ENDPOINT,
    )

    result = selector.select_variant(request)
    assert result.model_endpoint_parameters["TargetVariant"] == "OnlyVariant"


def test_multi_variant_with_equal_weights(variant_selector_setup):
    """Test multi-variant endpoint with equal weights (50/50 split)"""
    selector, sagemaker = variant_selector_setup
    endpoint_name = "test-equal-weights"

    # Create endpoint with two variants with equal weights
    create_multi_variant_endpoint(sagemaker, endpoint_name, weights=[1.0, 1.0])

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

        result = selector.select_variant(request)
        selected_variant = result.model_endpoint_parameters["TargetVariant"]
        variant_counts[selected_variant] += 1

    # With equal weights, we expect roughly 50/50 distribution
    # Allow for statistical variance (40-60% range)
    assert variant_counts["variant-1"] > 400
    assert variant_counts["variant-1"] < 600
    assert variant_counts["variant-2"] > 400
    assert variant_counts["variant-2"] < 600


def test_multi_variant_with_unequal_weights(variant_selector_setup):
    """Test multi-variant endpoint with unequal weights (80/20 split)"""
    selector, sagemaker = variant_selector_setup
    endpoint_name = "test-unequal-weights"

    # Create endpoint with two variants with unequal weights
    create_multi_variant_endpoint(sagemaker, endpoint_name, weights=[0.8, 0.2])

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

        result = selector.select_variant(request)
        selected_variant = result.model_endpoint_parameters["TargetVariant"]
        variant_counts[selected_variant] += 1

    # With 80/20 weights, we expect roughly 80/20 distribution
    # Allow for statistical variance (70-90% for variant-1, 10-30% for variant-2)
    assert variant_counts["variant-1"] > 700
    assert variant_counts["variant-1"] < 900
    assert variant_counts["variant-2"] > 100
    assert variant_counts["variant-2"] < 300


def test_variant_caching_reduces_api_calls(variant_selector_setup):
    """Test that variant caching reduces SageMaker API calls"""
    selector, sagemaker = variant_selector_setup
    endpoint_name = "test-caching"

    # Create endpoint
    create_multi_variant_endpoint(sagemaker, endpoint_name)

    # Mock the describe_endpoint call to count invocations
    original_describe = sagemaker.describe_endpoint
    call_count = [0]

    def counting_describe(*args, **kwargs):
        call_count[0] += 1
        return original_describe(*args, **kwargs)

    sagemaker.describe_endpoint = counting_describe

    # Make multiple selections - should only call API once
    for _ in range(5):
        request = ImageRequest(
            job_id="test-job",
            image_id="test-image",
            image_url="s3://bucket/image.tif",
            model_name=endpoint_name,
            model_invoke_mode=ModelInvokeMode.SM_ENDPOINT,
        )
        selector.select_variant(request)

    # Should have called API only once due to caching
    assert call_count[0] == 1


def test_cache_expiration_after_ttl(variant_selector_setup):
    """Test that cache expires after TTL"""
    selector, sagemaker = variant_selector_setup
    endpoint_name = "test-cache-expiry"

    # Create selector with short TTL
    short_ttl_selector = EndpointVariantSelector(sm_client=sagemaker, cache_ttl_seconds=1)

    # Create endpoint
    create_multi_variant_endpoint(sagemaker, endpoint_name)

    # Mock the describe_endpoint call to count invocations
    original_describe = sagemaker.describe_endpoint
    call_count = [0]

    def counting_describe(*args, **kwargs):
        call_count[0] += 1
        return original_describe(*args, **kwargs)

    sagemaker.describe_endpoint = counting_describe

    # First selection
    request = ImageRequest(
        job_id="test-job",
        image_id="test-image",
        image_url="s3://bucket/image.tif",
        model_name=endpoint_name,
        model_invoke_mode=ModelInvokeMode.SM_ENDPOINT,
    )
    short_ttl_selector.select_variant(request)
    assert call_count[0] == 1

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
    assert call_count[0] == 2


def test_weighted_random_selection_uses_current_weight(variant_selector_setup):
    """Test that weighted random selection uses CurrentWeight correctly"""
    selector, sagemaker = variant_selector_setup
    endpoint_name = "test-weights"

    # Create endpoint with three variants with different weights
    create_multi_variant_endpoint(sagemaker, endpoint_name, weights=[0.5, 0.3, 0.2], num_variants=3)

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

        result = selector.select_variant(request)
        selected_variant = result.model_endpoint_parameters["TargetVariant"]
        variant_counts[selected_variant] += 1

    # Verify distribution approximates weights (with statistical tolerance)
    # variant-1: 50% ± 10% = 400-600
    # variant-2: 30% ± 10% = 200-400
    # variant-3: 20% ± 10% = 100-300
    assert variant_counts["variant-1"] > 400
    assert variant_counts["variant-1"] < 600
    assert variant_counts["variant-2"] > 200
    assert variant_counts["variant-2"] < 400
    assert variant_counts["variant-3"] > 100
    assert variant_counts["variant-3"] < 300


def test_empty_target_variant_string_triggers_selection(variant_selector_setup):
    """Test that empty string TargetVariant triggers selection"""
    selector, sagemaker = variant_selector_setup
    endpoint_name = "test-empty-variant"

    # Create endpoint
    create_multi_variant_endpoint(sagemaker, endpoint_name)

    # Create request with empty TargetVariant
    request = ImageRequest(
        job_id="test-job",
        image_id="test-image",
        image_url="s3://bucket/image.tif",
        model_name=endpoint_name,
        model_invoke_mode=ModelInvokeMode.SM_ENDPOINT,
        model_endpoint_parameters={"TargetVariant": ""},
    )

    result = selector.select_variant(request)
    # Should have selected a variant
    assert result.model_endpoint_parameters["TargetVariant"] in ["variant-1", "variant-2"]
    assert result.model_endpoint_parameters["TargetVariant"] != ""


def test_none_model_endpoint_parameters_creates_dict(variant_selector_setup):
    """Test that None model_endpoint_parameters creates new dict"""
    selector, sagemaker = variant_selector_setup
    endpoint_name = "test-none-params"

    # Create endpoint
    create_multi_variant_endpoint(sagemaker, endpoint_name)

    # Create request with None model_endpoint_parameters
    request = ImageRequest(
        job_id="test-job",
        image_id="test-image",
        image_url="s3://bucket/image.tif",
        model_name=endpoint_name,
        model_invoke_mode=ModelInvokeMode.SM_ENDPOINT,
        model_endpoint_parameters=None,
    )

    result = selector.select_variant(request)
    # Should have created dict and selected a variant
    assert result.model_endpoint_parameters is not None
    assert result.model_endpoint_parameters["TargetVariant"] in ["variant-1", "variant-2"]


def test_get_endpoint_variants_api_failure_uses_stale_cache(variant_selector_setup, mocker):
    """Test that API failure falls back to stale cache."""
    from cachetools import TTLCache

    selector, sagemaker = variant_selector_setup
    endpoint_name = "test-cache-fallback"

    # Create new selector
    test_selector = EndpointVariantSelector(sm_client=sagemaker, cache_ttl_seconds=300)

    # Create a real TTLCache and populate it
    cached_variants = [{"VariantName": "cached-variant-1", "CurrentWeight": 1.0}]
    test_selector._endpoint_cache[endpoint_name] = cached_variants

    # Mock the cache's __contains__ to simulate edge case:
    # - First call (line 129) returns False (expired)
    # - Second call (line 149) returns True (still accessible for fallback)
    mock_cache = mocker.MagicMock(spec=TTLCache)
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
    assert len(variants) == 1
    assert variants[0]["VariantName"] == "cached-variant-1"
    # Verify __contains__ was called twice
    assert contains_calls[0] == 2


def test_select_weighted_variant_empty_list_raises_value_error(variant_selector_setup):
    """Test _select_weighted_variant with empty list raises ValueError."""
    selector, sagemaker = variant_selector_setup

    # Act / Assert
    with pytest.raises(ValueError) as context:
        selector._select_weighted_variant([])

    assert "empty" in str(context.value).lower()


def test_select_variant_no_variants_logs_warning_returns_unchanged(variant_selector_setup, caplog):
    """Test select_variant with no variants logs warning and returns request unchanged."""
    import logging

    selector, sagemaker = variant_selector_setup
    endpoint_name = "test-no-variants"

    # Create a selector and mock _get_endpoint_variants to return empty list
    test_selector = EndpointVariantSelector(sm_client=sagemaker, cache_ttl_seconds=300)

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
    with caplog.at_level(logging.WARNING):
        result = test_selector.select_variant(request)

        # Assert - warning logged
        assert any("No variants" in message for message in caplog.text.split("\n"))

    # Assert - request returned unchanged (no TargetVariant set)
    assert result == request
    # If model_endpoint_parameters was None, it should remain None
    # If it was dict, TargetVariant should not be set
    if result.model_endpoint_parameters is not None:
        assert "TargetVariant" not in result.model_endpoint_parameters
