#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

from setuptools import find_namespace_packages, setup

setup(
    name="osml-model-runner-validation-tool",
    version="0.1.0",
    description="OSML Model Runner Validation Tool",
    packages=find_namespace_packages(include=["aws.osml.model_runner_validation_tool*"]),
    install_requires=["boto3>=1.24.0", "botocore>=1.27.0", "pytest>=7.1.2"],
    python_requires=">=3.9",
)
