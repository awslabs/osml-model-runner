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
