#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
Locust user that makes requests based on predefined test cases.

Reads request parameters from a JSON file and executes them sequentially.
"""

import copy
import itertools
import json
import logging
from pathlib import Path
from secrets import token_hex
from test.load.model_runner_user import ModelRunnerUser
from typing import Any, Dict, List
from urllib.parse import urlparse

from jinja2 import Template
from locust import task

logger = logging.getLogger(__name__)


class PredefinedRequestsUser(ModelRunnerUser):
    """
    Locust user that makes requests based on predefined test cases.

    Reads request parameters from a JSON file and executes them sequentially.
    """

    def __init__(self, environment):
        """
        Initialize the predefined requests user.

        :param environment: Locust environment containing test configuration
        """
        super().__init__(environment)
        self.requests: List[Dict[str, Any]] = []
        self._load_requests()
        # Create an infinite cycle iterator through the requests
        self.request_cycle = itertools.cycle(self.requests)

    def _load_requests(self, request_file_path: str = "./bin/locust/sample-requests.json") -> None:
        """
        Load test requests from a JSON file.

        :param request_file_path: Path to JSON file containing request definitions
        :raises RuntimeError: If request file cannot be loaded
        :raises FileNotFoundError: If the request file doesn't exist
        :raises json.JSONDecodeError: If the JSON file is invalid
        """
        # Try to get request file path from environment options, or use default
        request_path_str = getattr(self.environment.parsed_options, "request_file", request_file_path)
        request_path = Path(request_path_str)

        # If relative path doesn't exist, try absolute path
        if not request_path.exists() and not request_path.is_absolute():
            # Try in current directory or test/load directory
            alt_paths = [
                Path.cwd() / request_path_str,
                Path(__file__).parent / request_path_str,
                Path(__file__).parent.parent.parent / "bin" / "locust" / "sample-requests.json",
            ]
            for alt_path in alt_paths:
                if alt_path.exists():
                    request_path = alt_path
                    break

        if not request_path.exists():
            raise FileNotFoundError(f"Request file not found: {request_path}")

        logger.info(f"Using sample requests file at: {request_path.absolute()}")

        with open(request_path, "r") as f:
            request_template = Template(f.read())

        # The current structure of the image request requires separate output bucket and prefix parameters so
        # test_results_location is likely unused. It is included in these template replacements for future
        # compatibility if we ever decide to make the API more internally consistent.
        test_results_location = getattr(
            self.environment.parsed_options, "test_results_location", "s3://mr-bucket-sink-<account>"
        )
        test_imagery_location = getattr(
            self.environment.parsed_options, "test_imagery_location", "s3://osml-test-images-<account>"
        )

        # Parse S3 URL: s3://bucket/key -> (bucket, key)
        parsed = urlparse(test_results_location)
        test_results_bucket = parsed.netloc or test_results_location.replace("s3://", "").split("/")[0]
        test_results_prefix = parsed.path.lstrip("/")
        template_parameters = {
            "test_imagery_location": test_imagery_location,
            "test_results_location": test_results_location,
            "test_results_bucket": test_results_bucket,
            "test_results_prefix": test_results_prefix,
        }
        rendered_requests = request_template.render(template_parameters)

        self.requests = json.loads(rendered_requests)
        logging.info(f"Found {len(self.requests)} sample requests in configuration file.")

        if not isinstance(self.requests, list) or not self.requests:
            raise ValueError("Request file must contain a non-empty list of requests")

    @task
    def run_next_image(self):
        """
        Process the next image request from the predefined set.

        Takes the next request from the list and submits it for processing.
        Cycles back to the start when all requests have been used.
        """
        request = copy.deepcopy(next(self.request_cycle))

        job_id = token_hex(16)
        request["jobId"] = job_id
        if "jobName" in request:
            job_name = f"{request['jobName']}-{job_id}"
        else:
            job_name = f"test-{job_id}"
        request["jobName"] = job_name

        self.client.process_image(request)
