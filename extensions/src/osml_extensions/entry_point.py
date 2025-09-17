#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
import os

from aws.osml.model_runner.inference.endpoint_factory import FeatureDetectorFactory

logger = logging.getLogger(__name__)


def use_extensions() -> bool:
    """
    Check if extensions should be used based on environment variable.

    :return: bool = True if extensions should be used, False otherwise
    """
    env_value = os.getenv("USE_EXTENSIONS", "true").lower()
    return env_value in ("true", "1", "yes", "on", "enabled")


def get_enhanced_factory_class() -> type:
    """
    Get the appropriate factory class based on extension availability and configuration.

    This function provides a clean way to choose between the enhanced factory and
    the base factory with proper fallback handling.

    :return: type = Factory class to use (EnhancedFeatureDetectorFactory or FeatureDetectorFactory)
    """
    try:
        # Check if extensions should be used
        if not use_extensions():
            logger.info("Extensions disabled via configuration, using base FeatureDetectorFactory")
            return FeatureDetectorFactory

        # Try to import and validate extension components
        from osml_extensions.factory.enhanced_factory import EnhancedFeatureDetectorFactory

        logger.info("Extensions enabled and available, using EnhancedFeatureDetectorFactory")
        return EnhancedFeatureDetectorFactory

    except ImportError as e:
        logger.warning(f"Extension components not available: {e}. Using base FeatureDetectorFactory")
        return FeatureDetectorFactory
    except Exception as e:
        logger.error(f"Error checking extension availability: {e}. Using base FeatureDetectorFactory")
        return FeatureDetectorFactory


def create_feature_detector_factory(*args, **kwargs) -> FeatureDetectorFactory:
    """
    Create a feature detector factory with automatic extension support.

    This function serves as a drop-in replacement for FeatureDetectorFactory
    that automatically uses extensions when available and configured.

    :param args: Positional arguments for factory initialization
    :param kwargs: Keyword arguments for factory initialization
    :return: FeatureDetectorFactory = Factory instance (enhanced or base)
    """
    factory_class = get_enhanced_factory_class()

    try:
        # Create factory with the selected class
        factory = factory_class(*args, **kwargs)
        logger.info(f"Created {factory_class.__name__} for endpoint: {args[0] if args else 'unknown'}")
        return factory

    except Exception as e:
        logger.error(f"Failed to create {factory_class.__name__}: {e}")

        # Final fallback to base factory
        if factory_class != FeatureDetectorFactory:
            logger.info("Falling back to base FeatureDetectorFactory")
            try:
                factory = FeatureDetectorFactory(*args, **kwargs)
                logger.info("Successfully created fallback FeatureDetectorFactory")
                return factory
            except Exception as fallback_error:
                logger.error(f"Fallback factory creation also failed: {fallback_error}")
                raise
        else:
            raise


def patch_factory_imports():
    """
    Monkey patch the FeatureDetectorFactory import to use enhanced factory.

    This function replaces the FeatureDetectorFactory class in the inference module
    with our enhanced version, providing seamless integration without modifying
    the original codebase.
    """
    try:
        # Import the modules that use FeatureDetectorFactory
        import aws.osml.model_runner.inference.endpoint_factory as endpoint_factory_module
        import aws.osml.model_runner.tile_worker.tile_worker_utils as tile_worker_module

        # Store original factory for fallback (unused but kept for potential future use)
        # original_factory = endpoint_factory_module.FeatureDetectorFactory
        # Create enhanced factory class that maintains the same interface
        class PatchedFeatureDetectorFactory(FeatureDetectorFactory):
            def __init__(self, *args, **kwargs):
                # Don't call super().__init__ directly, instead use our enhanced creation logic
                enhanced_factory = create_feature_detector_factory(*args, **kwargs)

                # Copy attributes from the enhanced factory
                self.__dict__.update(enhanced_factory.__dict__)

                # Ensure we have the required attributes for compatibility
                if hasattr(enhanced_factory, "endpoint"):
                    self.endpoint = enhanced_factory.endpoint
                if hasattr(enhanced_factory, "endpoint_mode"):
                    self.endpoint_mode = enhanced_factory.endpoint_mode
                if hasattr(enhanced_factory, "assumed_credentials"):
                    self.assumed_credentials = enhanced_factory.assumed_credentials

            def build(self):
                # Delegate to the enhanced factory's build method
                return create_feature_detector_factory(self.endpoint, self.endpoint_mode, self.assumed_credentials).build()

        # Replace the factory in both modules
        endpoint_factory_module.FeatureDetectorFactory = PatchedFeatureDetectorFactory
        tile_worker_module.FeatureDetectorFactory = PatchedFeatureDetectorFactory

        logger.info("Successfully patched FeatureDetectorFactory imports to use enhanced factory")

    except Exception as e:
        logger.error(f"Failed to patch factory imports: {e}")
        logger.info("Application will continue with base factory implementation")


def initialize_extensions():
    """
    Initialize the extension system.

    This function should be called early in the application startup to set up
    extensions and patch imports as needed.
    """
    try:
        logger.info("Extension system initialization started")

        # Check if extensions are enabled
        if use_extensions():
            logger.info("Extensions are enabled, setting up enhanced functionality")

            # Patch factory imports for seamless integration
            patch_factory_imports()

            logger.info("Extension system initialization completed successfully")
        else:
            logger.info("Extensions are disabled, using base functionality only")

    except Exception as e:
        logger.error(f"Extension system initialization failed: {e}")
        logger.info("Application will continue with base functionality")


# Convenience function for backward compatibility
def setup_enhanced_model_runner():
    """
    Set up the model runner with extension support.

    This is a convenience function that can be called from the main entry point
    to enable extension functionality.
    """
    initialize_extensions()


# Auto-initialize extensions when module is imported
# This ensures extensions are set up automatically when the module is imported
if __name__ != "__main__":
    try:
        initialize_extensions()
    except Exception as e:
        # Don't let initialization errors prevent module import
        logger.error(f"Auto-initialization failed: {e}")
        pass
