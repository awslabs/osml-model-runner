# Telling flake8 to not flag errors in this file. It is normal that these classes are imported but not used in an
# __init__.py file.
# flake8: noqa
#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
OSML Model Runner Extensions

This package provides extensions and enhancements to the base OSML Model Runner,
including an extension registry system for dynamic handler loading and management.
"""

from .enhanced_app_config import EnhancedServiceConfig
from .enhanced_model_runner import EnhancedModelRunner

# Import registry system
from .registry import (  # Core registry components; Handler metadata and types; Registration decorator; Errors
    DependencyInjectionError,
    ExtensionRegistry,
    ExtensionRegistryError,
    HandlerMetadata,
    HandlerRegistrationError,
    HandlerSelectionError,
    HandlerSelector,
    HandlerType,
    get_registry,
    register_handler,
    reset_registry,
)

# Import extensions to trigger handler registration
from . import extensions

# Import core extension components
# from .errors import ExtensionConfigurationError, ExtensionError, ExtensionRuntimeError
