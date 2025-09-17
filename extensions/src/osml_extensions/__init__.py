"""
OSML Model Runner Extensions

This package provides extensions for the OSML Model Runner that enhance
functionality while maintaining compatibility with the base package.
"""

__version__ = "1.0.0"

from .builders.async_sm_builder import AsyncSMDetectorBuilder
from .detectors.async_sm_detector import AsyncSMDetector

# Import entry point functions
from .entry_point import initialize_extensions, setup_enhanced_model_runner

# Import key components for easy access
from .factory.enhanced_factory import EnhancedFeatureDetectorFactory

__all__ = [
    "EnhancedFeatureDetectorFactory",
    "AsyncSMDetector",
    "AsyncSMDetectorBuilder",
    "initialize_extensions",
    "setup_enhanced_model_runner",
]
