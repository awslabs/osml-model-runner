#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
Endpoint variant selector for SageMaker endpoints.

This module provides functionality to select appropriate endpoint variants for SageMaker
endpoints using weighted random selection based on routing configuration.
"""

import logging
import random
from typing import Any, Dict, List

from cachetools import TTLCache

from aws.osml.model_runner.api import ImageRequest, ModelInvokeMode

logger = logging.getLogger(__name__)


class EndpointVariantSelector:
    """
    Selects endpoint variants for SageMaker endpoints using weighted random selection.

    This class encapsulates the logic for selecting which variant of a multi-variant
    SageMaker endpoint should be used for processing an image request. It uses weighted
    random selection based on the CurrentWeight configuration of each variant.

    The selector caches endpoint metadata to minimize SageMaker API calls and is
    thread-safe for concurrent use by multiple components.
    """

    def __init__(self, sm_client, cache_ttl_seconds: int = 300, cache_max_size: int = 100):
        """
        Initialize the endpoint variant selector.

        :param sm_client: Boto3 SageMaker client for querying endpoint metadata
        :param cache_ttl_seconds: Time-to-live for cached endpoint metadata in seconds
        :param cache_max_size: Maximum number of items to cache (prevents unbounded growth)
        """
        self.sm_client = sm_client
        self.cache_ttl_seconds = cache_ttl_seconds

        # TTLCache automatically evicts expired items and enforces max size with LRU eviction
        # This prevents unbounded cache growth
        self._endpoint_cache: TTLCache = TTLCache(maxsize=cache_max_size, ttl=cache_ttl_seconds)

    def select_variant(self, image_request: ImageRequest) -> ImageRequest:
        """
        Select an endpoint variant for the ImageRequest if not already specified.

        This method implements the following logic:
        1. If TargetVariant is already set → return request unchanged (always honor explicit variant)
        2. If TargetVariant is not set and this is a SageMaker endpoint:
           - Query endpoint configuration to get ProductionVariants
           - Use weighted random selection based on CurrentWeight
           - Set TargetVariant in model_endpoint_parameters
        3. For HTTP endpoints → return request unchanged (no variants)

        :param image_request: The image request to process
        :return: ImageRequest with TargetVariant set if selection was needed
        """
        # Check if this is a SageMaker endpoint that needs variant selection
        if not self._is_sagemaker_endpoint(image_request):
            return image_request

        if not self._needs_variant_selection(image_request):
            return image_request

        # Get endpoint variants and select one
        endpoint_name = image_request.model_name
        variants = self._get_endpoint_variants(endpoint_name)

        if not variants:
            logger.warning(f"No variants found for endpoint {endpoint_name}, proceeding without variant selection")
            return image_request

        # Select variant using weighted random selection
        selected_variant = self._select_weighted_variant(variants)

        # Update the request with the selected variant
        if image_request.model_endpoint_parameters is None:
            image_request.model_endpoint_parameters = {}

        image_request.model_endpoint_parameters["TargetVariant"] = selected_variant

        logger.debug(f"Selected variant '{selected_variant}' for endpoint {endpoint_name}")

        return image_request

    def _is_sagemaker_endpoint(self, image_request: ImageRequest) -> bool:
        """
        Check if the request uses a SageMaker endpoint.

        :param image_request: The image request to check
        :return: True if this is a SageMaker endpoint, False otherwise
        """
        # HTTP endpoints start with http:// or https://
        if image_request.model_name.startswith(("http://", "https://")):
            return False

        # Check if the invoke mode is SageMaker
        return image_request.model_invoke_mode == ModelInvokeMode.SM_ENDPOINT

    def _needs_variant_selection(self, image_request: ImageRequest) -> bool:
        """
        Check if variant selection is needed for this request.

        :param image_request: The image request to check
        :return: True if variant selection is needed, False if TargetVariant is already set
        """
        if image_request.model_endpoint_parameters is None:
            return True

        target_variant = image_request.model_endpoint_parameters.get("TargetVariant")
        return target_variant is None or target_variant == ""

    def _get_endpoint_variants(self, endpoint_name: str) -> List[Dict[str, Any]]:
        """
        Get ProductionVariants from SageMaker endpoint configuration.

        This method caches endpoint metadata to minimize API calls. The cache
        uses TTLCache which automatically evicts expired items.

        :param endpoint_name: Name of the SageMaker endpoint
        :return: List of ProductionVariant dictionaries from the endpoint configuration
        """
        # Check cache first (TTLCache handles expiration automatically)
        if endpoint_name in self._endpoint_cache:
            logger.debug(f"Using cached variants for endpoint {endpoint_name}")
            return self._endpoint_cache[endpoint_name]

        # Cache miss or expired - query SageMaker
        try:
            logger.debug(f"Querying SageMaker for endpoint {endpoint_name} variants")
            response = self.sm_client.describe_endpoint(EndpointName=endpoint_name)
            variants = response.get("ProductionVariants", [])

            # Cache the variants (TTLCache handles eviction automatically)
            self._endpoint_cache[endpoint_name] = variants

            logger.debug(f"Cached {len(variants)} variants for endpoint {endpoint_name}")
            return variants

        except Exception as e:
            logger.error(f"Failed to query variants for endpoint {endpoint_name}: {e}")
            # If we have cached data, use it as fallback
            # Note: With TTLCache, expired items are auto-removed, but size-evicted items might still be accessible
            if endpoint_name in self._endpoint_cache:
                logger.warning(f"Using cached variants for endpoint {endpoint_name} after API failure")
                return self._endpoint_cache[endpoint_name]
            return []

    def _select_weighted_variant(self, variants: List[Dict[str, Any]]) -> str:
        """
        Select a variant using weighted random selection.

        Uses the CurrentWeight field from each variant to perform weighted random
        selection. This ensures that traffic is distributed according to the
        configured routing weights.

        :param variants: List of ProductionVariant dictionaries
        :return: Name of the selected variant
        """
        if not variants:
            raise ValueError("Cannot select variant from empty list")

        if len(variants) == 1:
            return variants[0]["VariantName"]

        # Extract variant names and weights
        variant_names = [v["VariantName"] for v in variants]
        weights = [v.get("CurrentWeight", 1.0) for v in variants]

        # Use random.choices for weighted selection
        selected = random.choices(variant_names, weights=weights, k=1)[0]

        return selected
