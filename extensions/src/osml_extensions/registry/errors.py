#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

"""
Exception classes for the extension registry system.
"""


class ExtensionRegistryError(Exception):
    """Base exception for registry errors."""
    pass


class HandlerRegistrationError(ExtensionRegistryError):
    """Raised when handler registration fails."""
    pass


class HandlerSelectionError(ExtensionRegistryError):
    """Raised when no suitable handler can be selected."""
    pass


class DependencyInjectionError(ExtensionRegistryError):
    """Raised when dependency injection fails."""
    pass