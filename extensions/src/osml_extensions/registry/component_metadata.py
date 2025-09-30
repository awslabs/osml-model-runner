#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
Component metadata and type definitions for the extension registry system.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Type


class ComponentType(Enum):
    """Enumeration of supported component types."""
    MODEL_RUNNER = "model_runner"


@dataclass
class ComponentMetadata:
    """Metadata describing a registered component."""

    name: str  # Unique identifier for the component (e.g., "enhanced_model_runner")
    component_class: Type  # The actual component class to instantiate
    component_type: ComponentType  # Type of component (MODEL_RUNNER)
    description: str  # Human-readable description of component capabilities

    def __post_init__(self):
        """Validate metadata after initialization."""
        if not self.name:
            raise ValueError("Component name cannot be empty")
        if not isinstance(self.component_type, ComponentType):
            raise ValueError(f"component_type must be a ComponentType enum, got {type(self.component_type)}")
        if not self.component_class:
            raise ValueError("component_class cannot be None")