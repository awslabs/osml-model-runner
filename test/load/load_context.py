# Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

"""
Shared, environment-scoped state for Locust load tests.

Locust creates many User instances; doing expensive discovery (S3 listing,
SageMaker endpoint listing, request file parsing) per user can produce noisy and
expensive AWS API traffic. This module provides a single shared context per
Locust Environment.

This file intentionally uses flat imports (no package structure) so the entire
directory can be passed to Locust:

    locust -f ./test/load --class-picker ...
"""

from __future__ import annotations

from dataclasses import dataclass, field
from threading import Lock
from typing import Any, Optional


@dataclass
class LoadContext:
    """
    Shared, mutable state for a single Locust run.

    Instances of Locust `User` are created per simulated user. This context is attached
    to the Locust `Environment` so we can compute expensive discovery data once and
    reuse it across users.
    """

    lock: Lock = field(default_factory=Lock)

    # Cached discovery results (lazy, computed on first access)
    random_request_endpoints: Optional[list[str]] = None
    random_request_images: Optional[list[str]] = None

    # Cached predefined requests (lazy, computed on first access)
    predefined_requests: Optional[list[dict[str, Any]]] = None


def get_load_test_context(environment) -> LoadContext:
    """
    Get (or create) the shared context attached to the Locust Environment.

    :param environment: Locust `Environment` instance for this run.
    :returns: The shared `LoadContext`.
    """
    existing = getattr(environment, "osml_load_test_context", None)
    if isinstance(existing, LoadContext):
        return existing

    ctx = LoadContext()
    environment.osml_load_test_context = ctx
    return ctx
