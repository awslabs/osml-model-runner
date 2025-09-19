#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
Registration wrappers for base handlers from aws.osml.model_runner.
"""

import logging

try:
    from aws.osml.model_runner.region_request_handler import RegionRequestHandler
    from aws.osml.model_runner.image_request_handler import ImageRequestHandler
except ImportError as e:
    logging.warning(f"Failed to import base handlers from aws.osml.model_runner: {e}")
    RegionRequestHandler = None
    ImageRequestHandler = None

from .decorators import register_handler
from .handler_metadata import HandlerType

logger = logging.getLogger(__name__)


def register_base_handlers():
    """
    Register base handlers from aws.osml.model_runner for the 'http' request type.
    
    This function should be called during module initialization to ensure
    base handlers are available as fallbacks.
    """
    if RegionRequestHandler is None or ImageRequestHandler is None:
        logger.error("Cannot register base handlers - aws.osml.model_runner classes not available")
        return
    
    try:
        # Register base region request handler
        _ = register_handler(
            request_type="sm_endpoint",
            handler_type=HandlerType.REGION_REQUEST_HANDLER,
            name="base_region_request_handler",
            description="Base region request handler from aws.osml.model_runner"
        )(RegionRequestHandler)

        # Register base image request handler
        _ = register_handler(
            request_type="sm_endpoint",
            handler_type=HandlerType.IMAGE_REQUEST_HANDLER,
            name="base_image_request_handler",
            description="Base image request handler from aws.osml.model_runner"
        )(ImageRequestHandler)

    
        logger.info("Successfully registered base handlers for 'sm_endpoint' request type")
        
    except Exception as e:
        logger.error(f"Failed to register base handlers: {e}")
        raise


# Auto-register base handlers when this module is imported
try:
    register_base_handlers()
except Exception as e:
    logger.warning(f"Failed to auto-register base handlers: {e}")