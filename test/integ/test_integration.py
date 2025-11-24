#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
Pytest integration tests for OSML Model Runner.

This module provides pytest wrappers around the IntegRunner class, allowing
integration tests to be run using standard pytest commands and fixtures.

Example usage:
    # Run all integration tests
    pytest test/integ/test_integration.py

    # Run tests with verbose output
    pytest test/integ/test_integration.py -v
"""

import json
import os
import sys
from pathlib import Path
from test.integ.integ_runner import IntegRunner
from typing import Any, Dict, List

import pytest

# Get the test/integ directory for resolving relative paths
TEST_INTEG_DIR = Path(__file__).parent


@pytest.fixture(scope="session")
def integ_runner(verbose) -> IntegRunner:
    """
    Pytest fixture providing an IntegRunner instance.

    :param verbose: Verbose logging fixture.
    :returns: Configured IntegRunner instance.
    """

    print("Initializing IntegRunner...", file=sys.stderr)
    sys.stderr.flush()
    runner = IntegRunner(verbose=verbose)
    print("IntegRunner initialized successfully", file=sys.stderr)
    sys.stderr.flush()
    return runner


@pytest.fixture
def default_timeout() -> int:
    """
    Pytest fixture providing default timeout for tests.

    :returns: Default timeout in minutes.
    """
    return int(os.environ.get("INTEG_TEST_TIMEOUT", "30"))


@pytest.fixture
def delay_between_tests() -> int:
    """
    Pytest fixture providing delay between tests in a suite.

    :returns: Delay in seconds.
    """
    return int(os.environ.get("INTEG_TEST_DELAY", "5"))


def load_test_suite(suite_path: str) -> List[Dict[str, Any]]:
    """
    Load a test suite JSON file.

    :param suite_path: Path to test suite JSON file.
    :returns: List of test case dictionaries.
    :raises FileNotFoundError: If suite file cannot be found.
    """
    # Try multiple path resolution strategies
    resolved_path = None

    # Try absolute path
    if os.path.isabs(suite_path):
        if os.path.exists(suite_path):
            resolved_path = suite_path
    else:
        # Try relative to current directory
        resolved = os.path.abspath(suite_path)
        if os.path.exists(resolved):
            resolved_path = resolved
        else:
            # Try relative to test/integ directory
            resolved = TEST_INTEG_DIR / suite_path
            if resolved.exists():
                resolved_path = str(resolved)

    if not resolved_path or not os.path.exists(resolved_path):
        raise FileNotFoundError(f"Test suite file not found: {suite_path}")

    with open(resolved_path, "r") as f:
        return json.load(f)


class TestIntegrationSuite:
    """
    Pytest test class for test suite execution.

    Tests can be parametrized to run multiple test suites.
    """

    @pytest.mark.integration
    @pytest.mark.parametrize(
        "suite_path",
        [
            # Default test suite
            "suites/default.json",
        ],
    )
    def test_integration_suite(
        self,
        integ_runner: IntegRunner,
        suite_path: str,
        default_timeout: int,
        delay_between_tests: int,
    ) -> None:
        """
        Run a test suite from a JSON file.

        :param integ_runner: IntegRunner fixture.
        :param suite_path: Path to test suite JSON file.
        :param default_timeout: Default timeout for tests.
        :param delay_between_tests: Delay between tests in seconds.
        """
        # Load test suite
        test_cases = load_test_suite(suite_path)

        # Run the test suite
        results = integ_runner.run_test_suite(
            test_cases=test_cases,
            timeout_minutes=default_timeout,
            delay_between_tests=delay_between_tests,
        )

        # Assert all tests passed using pytest's assertion system
        assert results["failed"] == 0, (
            f"Test suite failed: {results['failed']} out of {results['total_tests']} tests failed. "
            f"Passed: {results['passed']}, Failed: {results['failed']}"
        )
        assert (
            results["passed"] == results["total_tests"]
        ), f"Not all tests passed: {results['passed']}/{results['total_tests']}"
