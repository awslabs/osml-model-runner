#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
Locustfile for OSML Model Runner load tests.

This is the main entry point for Locust-based load testing. It imports all
necessary components and makes them available to Locust.

Usage:
    # Use ModelRunnerLoadTestUser (submits jobs, doesn't wait for completion)
    locust -f test/load/locustfile.py --headless --users 5 --spawn-rate 1 \\
        --source-bucket s3://my-bucket --result-bucket s3://my-results \\
        --model-name centerpoint --processing-window-min 10

    # Use PredefinedRequestsUser (reads from JSON file, waits for completion)
    locust -f test/load/locustfile.py --headless --users 5 --spawn-rate 1 \\
        --test-imagery-location s3://my-images --test-results-location s3://my-results \\
        --request-file ./sample-requests.json

    # Use RandomRequestUser (random images/endpoints, waits for completion)
    locust -f test/load/locustfile.py --headless --users 5 --spawn-rate 1 \\
        --test-imagery-location s3://my-images --test-results-location s3://my-results
"""

# Import load shape module (registers event listeners and makes class available)
import test.load.locust_load_shape  # noqa: F401

# Import Locust setup (registers event handlers and CLI arguments)
import test.load.locust_setup  # noqa: F401

# Import all Locust user class modules
import test.load.locust_user
import test.load.model_runner_user
import test.load.predefined_requests_user
import test.load.random_requests_user

# Default user class: ModelRunnerLoadTestUser (submits jobs without waiting)
# Users can override this by specifying a different user class via Locust's --user-class flag
User = test.load.locust_user.ModelRunnerLoadTestUser
