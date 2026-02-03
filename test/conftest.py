#  Copyright 2025-2026 Amazon.com, Inc. or its affiliates.

import boto3
import pytest
from moto import mock_aws


@pytest.fixture
def aws_credentials(monkeypatch):
    """Set AWS credentials for moto testing"""
    # These are already set in tox.ini but fixture makes tests runnable outside tox
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_SECURITY_TOKEN", "testing")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-west-2")


@pytest.fixture
def mock_logger(mocker):
    """Create a mock logger for testing"""
    logger = mocker.Mock()
    logger.info = mocker.Mock()
    logger.debug = mocker.Mock()
    logger.warning = mocker.Mock()
    logger.error = mocker.Mock()
    return logger


@pytest.fixture
def mock_metrics_logger(mocker):
    """Create a mock metrics logger for testing"""
    logger = mocker.Mock()
    logger.put_metric = mocker.Mock()
    logger.set_property = mocker.Mock()
    logger.set_dimensions = mocker.Mock()
    logger.flush = mocker.Mock()
    return logger


@pytest.fixture
def ddb_resource(aws_credentials):
    """Create a virtual DynamoDB resource"""
    with mock_aws():
        yield boto3.resource("dynamodb", region_name="us-west-2")


@pytest.fixture
def s3_resource(aws_credentials):
    """Create a virtual S3 resource"""
    with mock_aws():
        yield boto3.resource("s3", region_name="us-west-2")


@pytest.fixture
def sqs_resource(aws_credentials):
    """Create a virtual SQS resource"""
    with mock_aws():
        yield boto3.resource("sqs", region_name="us-west-2")


@pytest.fixture
def sagemaker_client(aws_credentials):
    """Create a virtual SageMaker client"""
    with mock_aws():
        yield boto3.client("sagemaker", region_name="us-west-2")
