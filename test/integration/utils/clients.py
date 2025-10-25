#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
AWS client utilities for integration testing.

This module provides centralized AWS client management for integration tests,
ensuring consistent configuration and session management across all test utilities.
"""

import logging
from typing import Optional

import boto3

from .config import OSMLConfig

# Configure logging for boto3 to reduce noise
logging.getLogger("botocore").setLevel(logging.CRITICAL)


def get_session_credentials(region: Optional[str] = None) -> boto3.session.Session:
    """
    Get a new boto3 session with proper credentials and region configuration.

    This will prevent using outdated credentials and ensures proper region configuration
    for integration tests.

    Args:
        region: Optional region override. If not provided, uses OSMLConfig.REGION

    Returns:
        boto3.session.Session: Configured session for AWS operations
    """
    target_region = region or OSMLConfig.REGION
    return boto3.session.Session(region_name=target_region)


def sqs_client(region: Optional[str] = None) -> boto3.resource:
    """
    Get SQS resource client for queue operations.

    Args:
        region: Optional region override

    Returns:
        boto3.resource: SQS resource client
    """
    session = get_session_credentials(region)
    return session.resource("sqs", region_name=session.region_name)


def ddb_client(region: Optional[str] = None) -> boto3.resource:
    """
    Get DynamoDB resource client for table operations.

    Args:
        region: Optional region override

    Returns:
        boto3.resource: DynamoDB resource client
    """
    session = get_session_credentials(region)
    return session.resource("dynamodb", region_name=session.region_name)


def s3_client(region: Optional[str] = None) -> boto3.client:
    """
    Get S3 client for bucket and object operations.

    Args:
        region: Optional region override

    Returns:
        boto3.client: S3 client
    """
    session = get_session_credentials(region)
    return session.client("s3", region_name=session.region_name)


def kinesis_client(region: Optional[str] = None) -> boto3.client:
    """
    Get Kinesis client for stream operations.

    Args:
        region: Optional region override

    Returns:
        boto3.client: Kinesis client
    """
    session = get_session_credentials(region)
    return session.client("kinesis", region_name=session.region_name)


def sm_client(region: Optional[str] = None) -> boto3.client:
    """
    Get SageMaker client for model endpoint operations.

    Args:
        region: Optional region override

    Returns:
        boto3.client: SageMaker client
    """
    session = get_session_credentials(region)
    return session.client("sagemaker", region_name=session.region_name)


def cw_client(region: Optional[str] = None) -> boto3.client:
    """
    Get CloudWatch client for metrics and monitoring operations.

    Args:
        region: Optional region override

    Returns:
        boto3.client: CloudWatch client
    """
    session = get_session_credentials(region)
    return session.client("cloudwatch", region_name=session.region_name)


def elb_client(region: Optional[str] = None) -> boto3.client:
    """
    Get Elastic Load Balancing client for load balancer operations.

    Args:
        region: Optional region override

    Returns:
        boto3.client: ELB client
    """
    session = get_session_credentials(region)
    return session.client("elbv2", region_name=session.region_name)


def get_all_clients(region: Optional[str] = None) -> dict:
    """
    Get all AWS clients in a single call for convenience.

    Args:
        region: Optional region override

    Returns:
        dict: Dictionary containing all AWS clients
    """
    return {
        "sqs": sqs_client(region),
        "ddb": ddb_client(region),
        "s3": s3_client(region),
        "kinesis": kinesis_client(region),
        "sm": sm_client(region),
        "cw": cw_client(region),
        "elb": elb_client(region),
    }
