#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.
from setuptools import find_packages, setup

setup(
    name="osml-extensions",
    version="1.0.0",
    author="Amazon Web Services",
    description="Extensions for the OSML Model Runner with enhanced functionality",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    classifiers=[
        "Operating System :: OS Independent",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.8",
    install_requires=[
        "boto3",
        "botocore",
    ],
)
