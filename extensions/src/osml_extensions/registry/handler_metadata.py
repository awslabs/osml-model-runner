#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
Handler metadata and type definitions for the extension registry system.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Type


class HandlerType(Enum):
    """Enumeration of supported handler types."""

    REGION_REQUEST_HANDLER = "region_request_handler"
    IMAGE_REQUEST_HANDLER = "image_request_handler"


@dataclass
class HandlerMetadata:
    """Metadata describing a registered handler."""

    name: str  # Unique identifier for the handler (e.g., "enhanced_region_handler")
    handler_class: Type  # The actual handler class to instantiate
    handler_type: HandlerType  # Type of handler (REGION_REQUEST_HANDLER, IMAGE_REQUEST_HANDLER)
    description: str  # Human-readable description of handler capabilities

    def __post_init__(self):
        """Validate metadata after initialization."""
        if not self.name:
            raise ValueError("Handler name cannot be empty")
        if not isinstance(self.handler_type, HandlerType):
            raise ValueError(f"handler_type must be a HandlerType enum, got {type(self.handler_type)}")
        if not self.handler_class:
            raise ValueError("handler_class cannot be None")
