#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
Handler selection logic for the extension registry system.
"""

import logging
import os
from typing import Dict, Optional, Tuple

from aws.osml.model_runner.app_config import ServiceConfig

from .extension_registry import get_registry
from .handler_metadata import HandlerMetadata, HandlerType
from .errors import HandlerSelectionError

logger = logging.getLogger(__name__)


class HandlerSelector:
    """Selects appropriate handlers based on configuration."""
    
    def __init__(self):
        """Initialize the handler selector."""
        self.registry = get_registry()
    
    def select_handlers(
        self,
        request_type: Optional[str] = None,
    ) -> Tuple[HandlerMetadata, HandlerMetadata]:
        """
        Select appropriate handler pair based on configuration.
        
        :param request_type: Explicit request type to use
        :return: Tuple of (region_handler, image_handler)
        :raises HandlerSelectionError: If no suitable handlers can be found
        """
        try:
            # Determine the request type to use
            determined_request_type = self._determine_request_type(request_type)
            
            logger.info(f"Selecting handlers for request_type='{determined_request_type}'")
            
            # Validate request type is supported
            if not self._validate_request_type_support(determined_request_type):
                logger.warning(f"Request type '{determined_request_type}' not supported, falling back to 'http'")
                determined_request_type = "http"
                
                # If http is also not supported, this is a critical error
                if not self._validate_request_type_support(determined_request_type):
                    raise HandlerSelectionError("No fallback handlers available - 'http' request type not registered")
            
            # Get handlers for the determined request type
            handlers = self.registry.get_handlers_for_request_type(determined_request_type)
            
            # Extract region and image handlers
            region_handler = handlers.get(HandlerType.REGION_REQUEST_HANDLER)
            image_handler = handlers.get(HandlerType.IMAGE_REQUEST_HANDLER)
            
            # Validate both handlers are available
            if not region_handler:
                raise HandlerSelectionError(
                    f"No region request handler found for request_type='{determined_request_type}'"
                )
            
            if not image_handler:
                raise HandlerSelectionError(
                    f"No image request handler found for request_type='{determined_request_type}'"
                )

            logger.info(
                f"Selected handlers for request_type='{determined_request_type}': "
                f"region='{region_handler.name}', image='{image_handler.name}'"
            )
            
            return region_handler, image_handler
            
        except Exception as e:
            error_msg = f"Failed to select handlers: {e}"
            logger.error(error_msg)
            
            if not isinstance(e, HandlerSelectionError):
                raise HandlerSelectionError(error_msg) from e
            raise
    
    def _determine_request_type(
        self,
        explicit_request_type: Optional[str],
    ) -> str:
        """
        Determine the request type to use based on configuration.
        
        :param explicit_request_type: Explicitly provided request type
        :param config: Service configuration
        :return: The determined request type
        """
        # Priority 1: Explicit request type parameter
        if explicit_request_type:
            logger.debug(f"Using explicit request_type: {explicit_request_type}")
            return explicit_request_type
        
        # Priority 2: Environment variable
        env_request_type = os.getenv("REQUEST_TYPE")
        if env_request_type:
            logger.debug(f"Using REQUEST_TYPE from environment: {env_request_type}")
            return env_request_type
        
        raise ValueError(f"Request type not recognized: {env_request_type}")

        # # Priority 3: Determine from endpoint configuration
        # if endpoint_config:
        #     inferred_type = self._infer_request_type_from_endpoint(endpoint_config)
        #     if inferred_type:
        #         logger.debug(f"Inferred request_type from endpoint config: {inferred_type}")
        #         return inferred_type
        
        # # Priority 4: Check if extensions are disabled
        # if config and hasattr(config, 'use_extensions') and not config.use_extensions:
        #     logger.debug("Extensions disabled, using 'http' request type")
        #     return "http"
        
        # # Priority 5: Default fallback
        # logger.debug("Using default request_type: http")
        # return "http"
    
    # def _infer_request_type_from_endpoint(self, endpoint_config: Dict) -> Optional[str]:
    #     """
    #     Infer request type from endpoint configuration.
        
    #     :param endpoint_config: Endpoint configuration dictionary
    #     :return: Inferred request type or None
    #     """
    #     # Look for common endpoint configuration patterns
    #     endpoint_name = endpoint_config.get("endpoint_name", "").lower()
    #     endpoint_type = endpoint_config.get("endpoint_type", "").lower()
        
    #     # Check for async endpoint indicators
    #     if "async" in endpoint_name or "async" in endpoint_type:
    #         return "async_sm_endpoint"
        
    #     # Check for SageMaker endpoint indicators
    #     if ("sagemaker" in endpoint_name or "sm" in endpoint_type or 
    #         "endpoint" in endpoint_name):
    #         return "sm_endpoint"
        
    #     # Default to http for other cases
    #     return "http"
    
    def _validate_request_type_support(self, request_type: str) -> bool:
        """
        Validate that the request type is supported by the registry.
        
        :param request_type: Request type to validate
        :return: True if supported, False otherwise
        """
        supported_types = self.registry.get_supported_request_types()
        return request_type in supported_types
    