#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

from datetime import datetime

import boto3

MOCK_DESCRIBE_ENDPOINT_RESPONSE = {
    "EndpointName": "test",
    "EndpointArn": "arn:aws:sagemaker:region:account:endpoint/test",
    "EndpointConfigName": "test-config",
    "ProductionVariants": [{"VariantName": "variant1", "CurrentWeight": 1.0}],
    "EndpointStatus": "InService",
    "CreationTime": datetime(2024, 1, 1),
    "LastModifiedTime": datetime(2024, 1, 1),
}


def test_sm_detector_generation(mocker):
    """
    Test that the FeatureDetectorFactory correctly creates an SMDetector
    when the endpoint mode is set to ModelInvokeMode.SM_ENDPOINT.
    """
    from aws.osml.model_runner.api.inference import ModelInvokeMode
    from aws.osml.model_runner.inference import FeatureDetectorFactory, SMDetector

    # Create and stub the SageMaker client
    sm_runtime_client = boto3.client("sagemaker-runtime")

    mock_boto3 = mocker.patch("aws.osml.model_runner.inference.sm_detector.boto3")
    mock_boto3.client.return_value = sm_runtime_client

    feature_detector = FeatureDetectorFactory(
        endpoint="test",
        endpoint_mode=ModelInvokeMode.SM_ENDPOINT,
    ).build()

    # Verify that the detector is an instance of SMDetector and has the correct mode
    assert isinstance(feature_detector, SMDetector)
    assert feature_detector.mode == ModelInvokeMode.SM_ENDPOINT


def test_http_detector_generation():
    """
    Test that the FeatureDetectorFactory correctly creates an HTTPDetector
    when the endpoint mode is set to ModelInvokeMode.HTTP_ENDPOINT.
    """
    from aws.osml.model_runner.api.inference import ModelInvokeMode
    from aws.osml.model_runner.inference import FeatureDetectorFactory, HTTPDetector

    feature_detector = FeatureDetectorFactory(
        endpoint="test",
        endpoint_mode=ModelInvokeMode.HTTP_ENDPOINT,
    ).build()

    # Verify that the detector is an instance of HTTPDetector and has the correct mode
    assert isinstance(feature_detector, HTTPDetector)
    assert feature_detector.mode == ModelInvokeMode.HTTP_ENDPOINT
