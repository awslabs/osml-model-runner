#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
Locustfile for OSML Model Runner load tests.

This is the main entry point for Locust-based load testing. It imports all
necessary components and makes them available to Locust.

Usage:
    locust -f test/load/locustfile.py --headless --users 5 --spawn-rate 1 \\
        --source-bucket s3://my-bucket --result-bucket s3://my-results \\
        --model-name centerpoint --processing-window-min 10
"""

# Import load shape module (registers event listeners and makes class available)
import test.load.locust_load_shape  # noqa: F401

# Import Locust setup (registers event handlers and CLI arguments)
import test.load.locust_setup  # noqa: F401

# Import Locust user class module
import test.load.locust_user

# Make user class available to Locust (must be after imports)
User = test.load.locust_user.ModelRunnerLoadTestUser
