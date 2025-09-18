#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
import os
import traceback

logger = logging.getLogger(__name__)


def use_extensions() -> bool:
    """
    Check if extensions should be used based on environment variable.

    :return: bool = True if extensions should be used, False otherwise
    """
    env_value = os.getenv("USE_EXTENSIONS", "true").lower()
    return env_value in ("true", "1")


# def get_enhanced_factory_class() -> type:
#     """
#     Get the appropriate factory class based on extension availability and configuration.

#     This function provides a clean way to choose between the enhanced factory and
#     the base factory with proper fallback handling.

#     :return: type = Factory class to use (EnhancedFeatureDetectorFactory or FeatureDetectorFactory)
#     """
#     # Import the original factory directly to avoid recursion
#     from aws.osml.model_runner.inference.endpoint_factory import FeatureDetectorFactory as OriginalFactory

#     try:
#         # Check if extensions should be used
#         if not use_extensions():
#             logger.info("Extensions disabled via configuration, using base FeatureDetectorFactory")
#             return OriginalFactory

#         # Try to import and validate extension components
#         from osml_extensions.factory.enhanced_factory import EnhancedFeatureDetectorFactory

#         logger.info("Extensions enabled and available, using EnhancedFeatureDetectorFactory")
#         return EnhancedFeatureDetectorFactory

#     except ImportError as e:
#         logger.warning(f"Extension components not available: {e}. Using base FeatureDetectorFactory")
#         logger.error(traceback.format_exc())
#         return OriginalFactory
#     except Exception as e:
#         logger.error(f"Error checking extension availability: {e}. Using base FeatureDetectorFactory")
#         logger.error(traceback.format_exc())
#         return OriginalFactory


# def create_feature_detector_factory(*args, **kwargs) -> FeatureDetectorFactory:
#     """
#     Create a feature detector factory with automatic extension support.

#     This function serves as a drop-in replacement for FeatureDetectorFactory
#     that automatically uses extensions when available and configured.

#     :param args: Positional arguments for factory initialization
#     :param kwargs: Keyword arguments for factory initialization
#     :return: FeatureDetectorFactory = Factory instance (enhanced or base)
#     """
#     # Import the original factory directly to avoid recursion
#     from aws.osml.model_runner.inference.endpoint_factory import FeatureDetectorFactory as OriginalFactory

#     try:
#         # Check if extensions should be used
#         if not use_extensions():
#             logger.info("Extensions disabled via configuration, using base FeatureDetectorFactory")
#             factory = OriginalFactory(*args, **kwargs)
#             logger.info(f"Created base FeatureDetectorFactory for endpoint: {args[0] if args else 'unknown'}")
#             return factory

#         # Try to import and validate extension components
#         from osml_extensions.factory.enhanced_factory import EnhancedFeatureDetectorFactory

#         logger.info("Extensions enabled and available, using EnhancedFeatureDetectorFactory")
#         factory = EnhancedFeatureDetectorFactory(*args, **kwargs)
#         logger.info(f"Created EnhancedFeatureDetectorFactory for endpoint: {args[0] if args else 'unknown'}")
#         return factory

#     except ImportError as e:
#         logger.warning(f"Extension components not available: {e}. Using base FeatureDetectorFactory")
#         logger.error(traceback.format_exc())
#         factory = OriginalFactory(*args, **kwargs)
#         logger.info("Successfully created fallback FeatureDetectorFactory")
#         return factory
#     except Exception as e:
#         logger.error(f"Error creating factory: {e}. Using base FeatureDetectorFactory")
#         logger.error(traceback.format_exc())
#         factory = OriginalFactory(*args, **kwargs)
#         logger.info("Successfully created fallback FeatureDetectorFactory")
#         return factory


# Global variable to store the original factory class
_original_feature_detector_factory = None
_patching_in_progress = False


def get_original_feature_detector_factory():
    """
    Get the original FeatureDetectorFactory class before any patching.

    This function provides access to the unpatched factory class for use
    in extensions and fallback scenarios. It includes robust fallback handling
    to ensure the original factory is always accessible.

    :return: The original FeatureDetectorFactory class
    """
    global _original_feature_detector_factory

    if _original_feature_detector_factory is not None:
        return _original_feature_detector_factory

    # If not stored yet, try to get it directly
    try:
        from aws.osml.model_runner.inference.endpoint_factory import FeatureDetectorFactory

        return FeatureDetectorFactory
    except ImportError as e:
        logger.error(f"Could not import original FeatureDetectorFactory: {e}")
        raise


def patch_factory_imports():
    """
    Monkey patch the FeatureDetectorFactory import to use enhanced factory.

    This function replaces the FeatureDetectorFactory class in the inference module
    with our enhanced version, providing seamless integration without modifying
    the original codebase.
    """
    global _original_feature_detector_factory, _patching_in_progress

    # Prevent recursive patching
    if _patching_in_progress:
        logger.warning("Patching already in progress, skipping to prevent recursion")
        return

    _patching_in_progress = True

    try:
        # Import the modules that use FeatureDetectorFactory
        import aws.osml.model_runner.inference.endpoint_factory as endpoint_factory_module
        import aws.osml.model_runner.tile_worker.tile_worker_utils as tile_worker_module

        # Store original factory class before any patching occurs (only once)
        if _original_feature_detector_factory is None:
            _original_feature_detector_factory = endpoint_factory_module.FeatureDetectorFactory
            logger.info("Stored reference to original FeatureDetectorFactory")

        # Create enhanced factory class that maintains the same interface
        class PatchedFeatureDetectorFactory:
            def __init__(self, endpoint, endpoint_mode, assumed_credentials=None):
                """
                Initialize the patched factory with the same interface as the original.

                :param endpoint: URL of the inference model endpoint
                :param endpoint_mode: the type of endpoint (HTTP, SageMaker)
                :param assumed_credentials: optional credentials to use with the model
                """
                self.endpoint = endpoint
                self.endpoint_mode = endpoint_mode
                self.assumed_credentials = assumed_credentials

            def build(self):
                """
                Build a detector using enhanced factory when available, falling back to original.

                :return: Optional[Detector] = A detector instance
                """
                try:
                    # Check if extensions should be used
                    if not use_extensions():
                        logger.debug("Extensions disabled, using original factory")
                        assert _original_feature_detector_factory is not None
                        original_factory = _original_feature_detector_factory(
                            self.endpoint, self.endpoint_mode, self.assumed_credentials
                        )
                        return original_factory.build()

                    # Try to import and use enhanced factory
                    from osml_extensions.factory.enhanced_factory import EnhancedFeatureDetectorFactory

                    # Create enhanced factory instance
                    enhanced_factory = EnhancedFeatureDetectorFactory(
                        self.endpoint, self.endpoint_mode, self.assumed_credentials
                    )
                    result = enhanced_factory.build()

                    if result is not None:
                        return result
                    else:
                        # Enhanced factory returned None, fall back to original
                        logger.debug("Enhanced factory returned None, falling back to original")
                        assert _original_feature_detector_factory is not None
                        original_factory = _original_feature_detector_factory(
                            self.endpoint, self.endpoint_mode, self.assumed_credentials
                        )
                        return original_factory.build()

                except ImportError as e:
                    logger.warning(f"Extension components not available: {e}. Using original factory")
                    assert _original_feature_detector_factory is not None
                    original_factory = _original_feature_detector_factory(
                        self.endpoint, self.endpoint_mode, self.assumed_credentials
                    )
                    return original_factory.build()
                except Exception as e:
                    logger.error(f"Error using enhanced factory: {e}. Falling back to original factory")
                    assert _original_feature_detector_factory is not None
                    original_factory = _original_feature_detector_factory(
                        self.endpoint, self.endpoint_mode, self.assumed_credentials
                    )
                    return original_factory.build()

        # Replace the factory in both modules
        endpoint_factory_module.FeatureDetectorFactory = PatchedFeatureDetectorFactory
        tile_worker_module.FeatureDetectorFactory = PatchedFeatureDetectorFactory

        logger.info("Successfully patched FeatureDetectorFactory imports to use enhanced factory")

    except Exception as e:
        logger.error(f"Failed to patch factory imports: {e}")
        logger.info("Application will continue with base factory implementation")
        logger.error(traceback.format_exc())
    finally:
        _patching_in_progress = False


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
        logger.error(traceback.format_exc())


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
        logger.error(traceback.format_exc())
        pass
