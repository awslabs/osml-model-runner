from setuptools import find_packages, setup

setup(
    name="osml-model-runner",
    version="0.1.0",
    description="OSML Model Runner",
    packages=find_packages(where="src", exclude=["aws.osml.model_runner_validation_tool*"]),
    package_dir={"": "src"},
    install_requires=[
        "boto3>=1.24.0",
        "botocore>=1.27.0",
        "osml-imagery-toolkit",
        "geojson",
        "dacite",
        "aws-embedded-metrics",
        "numba",
    ],
    python_requires=">=3.9",
)
