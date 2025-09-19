#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
import os
from dataclasses import dataclass

from aws.osml.model_runner.app_config import ServiceConfig

logger = logging.getLogger(__name__)


def var_to_bool(var_name):
    return os.getenv(var_name, "false").lower() in ("true", "1")


@dataclass
class EnhancedServiceConfig(ServiceConfig):
    """
    Enhanced ServiceConfig with extension-specific configuration options.

    This class extends the base ServiceConfig with extension settings
    while maintaining full compatibility with the base model runner.
    """

    # Extension configuration
    use_extensions: bool = var_to_bool("USE_EXTENSIONS")
    async_detector_enabled: bool = var_to_bool("ASYNC_DETECTOR_ENABLED")
    enhanced_monitoring_enabled: bool = var_to_bool("ENHANCED_MONITORING_ENABLED")
    extension_fallback_enabled: bool = var_to_bool("EXTENSION_FALLBACK_ENABLED")
    enable_async_processing: bool = var_to_bool("ENABLE_ASYNC_PROCESSING")
    max_concurrent_regions: int = int(os.getenv("MAX_CONCURRENT_REGIONS", "100"))

    # Request type configuration
    request_type: str = os.getenv("REQUEST_TYPE", "")

    # def __post_init__(self):
    #     """Post-initialization processing for configuration validation."""
    #     super().__post_init__() if hasattr(super(), "__post_init__") else None

    #     # Validate request type if specified
    #     if self.request_type:
    #         valid_request_types = ["http", "sm_endpoint", "async_sm_endpoint"]
    #         if self.request_type not in valid_request_types:
    #             logger.warning(
    #                 f"Invalid REQUEST_TYPE '{self.request_type}'. "
    #                 f"Valid options: {valid_request_types}. Falling back to auto-detection."
    #             )
    #             self.request_type = ""

    #     # Log configuration
    #     logger.info(
    #         f"EnhancedServiceConfig initialized: use_extensions={self.use_extensions}, "
    #         f"request_type='{self.request_type or 'auto-detect'}', "
    #         f"fallback_enabled={self.extension_fallback_enabled}"
    #     )

    # def get_endpoint_configs(self) -> dict:
    #     """
    #     Parse endpoint configurations from environment variables.

    #     :return: Dictionary of endpoint configurations
    #     """
    #     endpoint_configs = {}

    #     # Look for endpoint configuration environment variables
    #     # Format: ENDPOINT_<NAME>_CONFIG={"type": "async", "url": "..."}
    #     for key, value in os.environ.items():
    #         if key.startswith("ENDPOINT_") and key.endswith("_CONFIG"):
    #             endpoint_name = key[9:-7].lower()  # Remove ENDPOINT_ and _CONFIG
    #             try:
    #                 import json

    #                 endpoint_config = json.loads(value)
    #                 endpoint_configs[endpoint_name] = endpoint_config
    #                 logger.debug(f"Loaded endpoint config for '{endpoint_name}': {endpoint_config}")
    #             except (json.JSONDecodeError, ValueError) as e:
    #                 logger.warning(f"Failed to parse endpoint config for '{endpoint_name}': {e}")

    #     return endpoint_configs

    # def determine_request_type_for_endpoint(self, endpoint_name: str) -> str:
    #     """
    #     Determine request type for a specific endpoint.

    #     :param endpoint_name: Name of the endpoint
    #     :return: Request type string
    #     """
    #     endpoint_configs = self.get_endpoint_configs()

    #     if endpoint_name in endpoint_configs:
    #         endpoint_config = endpoint_configs[endpoint_name]
    #         endpoint_type = endpoint_config.get("type", "").lower()

    #         # Map endpoint types to request types
    #         if "async" in endpoint_type:
    #             return "async_sm_endpoint"
    #         elif "sagemaker" in endpoint_type or "sm" in endpoint_type:
    #             return "sm_endpoint"
    #         else:
    #             return "http"

    #     # Default fallback
    #     return self.request_type or "http"
