# Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

"""
Locust user that makes requests based on predefined test cases.
"""

import copy
import itertools
import json
import logging
from pathlib import Path
from secrets import token_hex
from typing import Any, Dict, List

from _load_utils import resolve_path, split_s3_path
from jinja2 import Template
from load_context import get_load_test_context
from locust import task
from model_runner_user import ModelRunnerUser

logger = logging.getLogger(__name__)


class PredefinedRequestsUser(ModelRunnerUser):
    """
    Locust user that cycles through a predefined set of requests.

    Requests are loaded once per run (cached on the shared load-test context) and then
    iterated in a round-robin fashion across tasks.
    """

    def on_start(self) -> None:
        """
        Load and cache request templates, then initialize the request cycle.

        :returns: None
        """
        ctx = get_load_test_context(self.environment)
        with ctx.lock:
            if ctx.predefined_requests is None:
                ctx.predefined_requests = self._load_requests()

        self.requests = list(ctx.predefined_requests or [])
        self.request_cycle = itertools.cycle(self.requests)

    def _load_requests(self, request_file_path: str = "./test/load/sample-requests.json") -> List[Dict[str, Any]]:
        """
        Load and render the request JSON file.

        The request file is treated as a Jinja2 template and rendered with values
        derived from the configured imagery/results locations.

        :param request_file_path: Default request-file path to use if no CLI option is provided.
        :returns: A list of request objects suitable for `ModelRunnerClient.process_image`.
        :raises FileNotFoundError: If the request file cannot be resolved.
        :raises ValueError: If the rendered JSON is not a non-empty list.
        """
        request_path_str = getattr(self.environment.parsed_options, "request_file", request_file_path)
        request_path = resolve_path(request_path_str, relative_to=Path(__file__).parent)
        if not request_path.exists() and not request_path.is_absolute():
            # If a user passes something like "./test/load/sample-requests.json" but their cwd
            # isn't the repo root, fall back to the basename in this directory.
            request_path = Path(__file__).parent / Path(request_path_str).name

        if not request_path.exists():
            raise FileNotFoundError(f"Request file not found: {request_path}")

        logger.info("Using sample requests file at: %s", request_path.absolute())

        with open(request_path, "r") as f:
            request_template = Template(f.read())

        test_results_bucket, test_results_prefix = split_s3_path(self.environment.parsed_options.test_results_location)
        template_parameters = {
            "test_imagery_location": self.environment.parsed_options.test_imagery_location,
            "test_results_location": self.environment.parsed_options.test_results_location,
            "test_results_bucket": test_results_bucket,
            "test_results_prefix": test_results_prefix,
        }
        rendered_requests = request_template.render(template_parameters)

        requests = json.loads(rendered_requests)
        logger.info("Loaded %s predefined requests", len(requests))

        if not isinstance(requests, list) or not requests:
            raise ValueError("Request file must contain a non-empty list of requests")
        return requests

    @task
    def run_next_image(self):
        """
        Submit the next request from the predefined cycle.

        A new `jobId`/`jobName` is generated for each submission.

        :returns: None
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
