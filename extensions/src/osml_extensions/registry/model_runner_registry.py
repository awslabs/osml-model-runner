#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
Registration of ModelRunner classes with the extension registry.
"""

import logging

try:
    from aws.osml.model_runner import ModelRunner
except ImportError as e:
    logging.warning(f"Failed to import base ModelRunner from aws.osml.model_runner: {e}")
    ModelRunner = None

try:
    from osml_extensions.extensions.async_workflow.enhanced_model_runner import EnhancedModelRunner
except ImportError as e:
    logging.warning(f"Failed to import EnhancedModelRunner: {e}")
    EnhancedModelRunner = None

from .decorators import register_component
from .component_metadata import ComponentType

logger = logging.getLogger(__name__)


def register_model_runners():
    """
    Register ModelRunner classes with the extension registry.

    This function registers both base and enhanced ModelRunner classes
    for different request types.
    """
    try:
        # Register base ModelRunner for standard HTTP requests
        if ModelRunner is not None:
            # Register for sm_endpoint as fallback
            _ = register_component(
                request_type="sm_endpoint",
                component_type=ComponentType.MODEL_RUNNER,
                name="base_model_runner_sm",
                description="Base ModelRunner from aws.osml.model_runner for SageMaker endpoints",
            )(ModelRunner)

            logger.debug("Successfully registered base ModelRunner")

        # Register EnhancedModelRunner for async workflows
        if EnhancedModelRunner is not None:
            _ = register_component(
                request_type="async_sm_endpoint",
                component_type=ComponentType.MODEL_RUNNER,
                name="enhanced_model_runner",
                description="Enhanced ModelRunner with async workflow support",
            )(EnhancedModelRunner)

            logger.debug("Successfully registered EnhancedModelRunner")

    except Exception as e:
        logger.error(f"Failed to register ModelRunner classes: {e}")
        raise


# Auto-register ModelRunner classes when this module is imported
try:
    register_model_runners()
except Exception as e:
    logger.warning(f"Failed to auto-register ModelRunner classes: {e}")