#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
OSML Model Runner Extensions

This package provides extensions for the OSML Model Runner that enhance functionality
while maintaining full compatibility with the base open source package.
"""

from .api import ExtendedModelInvokeMode
from .detectors import AsyncSMDetector, AsyncSMDetectorBuilder
from .enhanced_model_runner import EnhancedModelRunner
from .enhanced_service_config import EnhancedServiceConfig
from .errors import ExtensionConfigurationError, ExtensionError, ExtensionRuntimeError
from .factory import EnhancedFeatureDetectorFactory
from .handlers import EnhancedImageRequestHandler, EnhancedRegionRequestHandler
from .workers import EnhancedTileWorker

__version__ = "1.0.0"
__all__ = [
    # API
    "ExtendedModelInvokeMode",
    # Configuration
    "EnhancedServiceConfig",
    # Errors
    "ExtensionError",
    "ExtensionConfigurationError",
    "ExtensionRuntimeError",
    # Core Components
    "EnhancedFeatureDetectorFactory",
    "AsyncSMDetector",
    "AsyncSMDetectorBuilder",
    "EnhancedTileWorker",
    "EnhancedImageRequestHandler",
    "EnhancedRegionRequestHandler",
    "EnhancedModelRunner",
]
