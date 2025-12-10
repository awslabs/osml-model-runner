#  Copyright 2025 Amazon.com, Inc. or its affiliates.

"""
EndpointCapacityEstimator calculates the maximum concurrent inference requests
an endpoint can handle, supporting both SageMaker and HTTP endpoints.
"""

import logging
from typing import Dict, Optional

from cachetools import TTLCache

logger = logging.getLogger(__name__)


class EndpointCapacityEstimator:
    """
    Calculates endpoint capacity in concurrent inference requests.

    This class supports:
    - HTTP endpoints (returns configured default concurrency)
    - SageMaker serverless endpoints (uses MaxConcurrency)
    - SageMaker instance-backed endpoints (uses instance count × per-instance concurrency)
    - Multi-variant endpoints (can calculate capacity per variant or combined)

    Capacity calculations are cached to minimize SageMaker API calls.
    """

    def __init__(
        self,
        sm_client,
        default_instance_concurrency: int = 2,
        default_http_concurrency: int = 10,
        cache_ttl_seconds: int = 300,
        cache_max_size: int = 100,
    ):
        """
        Initialize the capacity estimator.

        :param sm_client: Boto3 SageMaker client for querying endpoint metadata
        :param default_instance_concurrency: Default concurrent requests per instance
                                             when osml:instance-concurrency tag is not present
        :param default_http_concurrency: Default concurrent requests for HTTP endpoints
        :param cache_ttl_seconds: Time-to-live for cached endpoint metadata in seconds
        :param cache_max_size: Maximum number of items to cache (prevents unbounded growth)
        """
        self.sm_client = sm_client
        self.default_instance_concurrency = default_instance_concurrency
        self.default_http_concurrency = default_http_concurrency
        self.cache_ttl_seconds = cache_ttl_seconds

        # TTLCache automatically evicts expired items and enforces max size with LRU eviction
        # This prevents unbounded cache growth
        self._endpoint_cache: TTLCache = TTLCache(maxsize=cache_max_size, ttl=cache_ttl_seconds)
        self._tags_cache: TTLCache = TTLCache(maxsize=cache_max_size, ttl=cache_ttl_seconds)

    def estimate_capacity(self, endpoint_name: str, variant_name: Optional[str] = None) -> int:
        """
        Calculate endpoint capacity in concurrent inference requests.

        This method determines the maximum number of concurrent inference requests
        an endpoint can handle based on its configuration.

        For HTTP endpoints (URLs starting with http:// or https://):
        - Returns the configured default_http_concurrency
        - variant_name parameter is ignored

        For SageMaker endpoints:
        - If variant_name is specified: returns capacity for that specific variant only
        - If variant_name is None: returns combined capacity for all variants
        - For capacity-based throttling, variant_name should always be specified
          to get accurate capacity for the selected variant

        :param endpoint_name: Name of SageMaker endpoint or HTTP URL
        :param variant_name: Specific variant to calculate capacity for.
                            If None, returns capacity for all variants combined.
                            For capacity-based throttling, this should always be specified
                            to get capacity for the selected variant.
        :return: Maximum concurrent inference requests the endpoint/variant can handle
        """
        # Check if this is an HTTP endpoint
        if self._is_http_endpoint(endpoint_name):
            return self.default_http_concurrency

        # For SageMaker endpoints, query capacity
        return self._get_sagemaker_capacity(endpoint_name, variant_name)

    def _is_http_endpoint(self, endpoint_name: str) -> bool:
        """
        Check if endpoint is HTTP (not SageMaker).

        :param endpoint_name: Endpoint name or URL
        :return: True if endpoint is HTTP/HTTPS, False otherwise
        """
        return endpoint_name.startswith("http://") or endpoint_name.startswith("https://")

    def _get_sagemaker_capacity(self, endpoint_name: str, variant_name: Optional[str]) -> int:
        """
        Query SageMaker for endpoint capacity with caching.

        This method queries the SageMaker DescribeEndpoint API to get endpoint
        configuration and calculates capacity based on variant types.
        Results are cached with automatic TTL-based eviction.

        :param endpoint_name: Name of SageMaker endpoint
        :param variant_name: Specific variant to calculate capacity for, or None for all variants
        :return: Capacity in concurrent inference requests
        """
        # Check cache first (TTLCache handles expiration automatically)
        if endpoint_name in self._endpoint_cache:
            cached_metadata = self._endpoint_cache[endpoint_name]
            endpoint_arn = cached_metadata.get("EndpointArn")
            variants = cached_metadata.get("ProductionVariants", [])
            return self._calculate_capacity_from_variants(variants, variant_name, endpoint_arn)

        # Cache miss or expired - query SageMaker
        try:
            response = self.sm_client.describe_endpoint(EndpointName=endpoint_name)

            # Cache the response (TTLCache handles eviction automatically)
            self._endpoint_cache[endpoint_name] = response

            # Calculate capacity from variants
            endpoint_arn = response.get("EndpointArn")
            variants = response.get("ProductionVariants", [])
            return self._calculate_capacity_from_variants(variants, variant_name, endpoint_arn)

        except Exception as e:
            logger.error(f"Failed to describe endpoint {endpoint_name}: {e}")
            # If we have stale cached data, use it as fallback
            # Note: This won't work with TTLCache since expired items are auto-removed
            # But we can still check if the item exists (might have been evicted due to size, not TTL)
            if endpoint_name in self._endpoint_cache:
                logger.warning(f"Using cached capacity for endpoint {endpoint_name} after API failure")
                cached_metadata = self._endpoint_cache[endpoint_name]
                endpoint_arn = cached_metadata.get("EndpointArn")
                variants = cached_metadata.get("ProductionVariants", [])
                return self._calculate_capacity_from_variants(variants, variant_name, endpoint_arn)
            # No cache available, return default
            logger.warning(f"No cached data available for endpoint {endpoint_name}, using default capacity")
            return self.default_instance_concurrency

    def _calculate_capacity_from_variants(
        self, variants: list, variant_name: Optional[str], endpoint_arn: Optional[str]
    ) -> int:
        """
        Calculate capacity from list of variants.

        :param variants: List of ProductionVariant dictionaries from DescribeEndpoint
        :param variant_name: Specific variant to calculate capacity for, or None for all variants
        :param endpoint_arn: ARN of the endpoint for querying tags
        :return: Total capacity in concurrent inference requests
        """
        # Get tags for the endpoint (cached)
        tags_dict = self._get_endpoint_tags(endpoint_arn) if endpoint_arn else {}

        if variant_name:
            # Calculate capacity for specific variant only
            for variant in variants:
                if variant.get("VariantName") == variant_name:
                    return self._get_variant_capacity(variant, tags_dict)
            # Variant not found, log warning and return 0
            logger.warning(f"Variant {variant_name} not found in endpoint variants")
            return 0
        else:
            # Calculate combined capacity for all variants
            total_capacity = 0
            for variant in variants:
                total_capacity += self._get_variant_capacity(variant, tags_dict)
            return total_capacity

    def _get_endpoint_tags(self, endpoint_arn: str) -> Dict[str, str]:
        """
        Get tags for an endpoint with caching.

        :param endpoint_arn: ARN of the endpoint
        :return: Dictionary of tag key-value pairs
        """
        if not endpoint_arn:
            return {}

        # Check cache first (TTLCache handles expiration automatically)
        if endpoint_arn in self._tags_cache:
            return self._tags_cache[endpoint_arn]

        # Cache miss or expired - query tags
        try:
            response = self.sm_client.list_tags(ResourceArn=endpoint_arn)
            tags_list = response.get("Tags", [])

            # Convert list of {Key, Value} dicts to simple dict
            tags_dict = {tag["Key"]: tag["Value"] for tag in tags_list}

            # Cache the tags (TTLCache handles eviction automatically)
            self._tags_cache[endpoint_arn] = tags_dict

            return tags_dict

        except Exception as e:
            logger.warning(f"Failed to list tags for endpoint {endpoint_arn}: {e}")
            # If we have cached data, use it as fallback
            # Note: With TTLCache, expired items are auto-removed, but size-evicted items might still be accessible
            if endpoint_arn in self._tags_cache:
                return self._tags_cache[endpoint_arn]
            return {}

    def _get_variant_capacity(self, variant: Dict, tags_dict: Dict[str, str]) -> int:
        """
        Calculate capacity for a single variant.

        Handles both serverless and instance-backed variants:
        - Serverless: Uses MaxConcurrency from CurrentServerlessConfig
        - Instance-backed: Uses CurrentInstanceCount × per-instance concurrency
          - Per-instance concurrency comes from osml:instance-concurrency tag if present
          - Otherwise uses default_instance_concurrency

        :param variant: ProductionVariant dictionary from DescribeEndpoint
        :param tags_dict: Dictionary of endpoint tags
        :return: Capacity for this variant in concurrent inference requests
        """
        # Check if serverless
        serverless_config = variant.get("CurrentServerlessConfig")
        if serverless_config:
            max_concurrency = serverless_config.get("MaxConcurrency", 0)
            logger.debug(f"Serverless variant {variant.get('VariantName')}: MaxConcurrency={max_concurrency}")
            return max_concurrency

        # Instance-backed variant
        instance_count = variant.get("CurrentInstanceCount", 0)

        # Check for osml:instance-concurrency tag
        per_instance_concurrency = self.default_instance_concurrency
        if "osml:instance-concurrency" in tags_dict:
            try:
                per_instance_concurrency = int(tags_dict["osml:instance-concurrency"])
                logger.debug(
                    f"Using osml:instance-concurrency tag value: {per_instance_concurrency} "
                    f"for variant {variant.get('VariantName')}"
                )
            except ValueError:
                logger.warning(
                    f"Invalid osml:instance-concurrency tag value: {tags_dict['osml:instance-concurrency']}. "
                    f"Using default: {self.default_instance_concurrency}"
                )

        capacity = instance_count * per_instance_concurrency
        logger.debug(
            f"Instance-backed variant {variant.get('VariantName')}: "
            f"{instance_count} instances × {per_instance_concurrency} concurrency = {capacity}"
        )
        return capacity
