#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

from datetime import datetime
from unittest import TestCase, main
from unittest.mock import patch

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


class TestFeatureDetectorFactory(TestCase):
    @patch("aws.osml.model_runner.inference.sm_detector.boto3")
    def test_sm_detector_generation(self, mock_boto3):
        """
        Test that the FeatureDetectorFactory correctly creates an SMDetector
        when the endpoint mode is set to ModelInvokeMode.SM_ENDPOINT.
        """
        from aws.osml.model_runner.api.inference import ModelInvokeMode
        from aws.osml.model_runner.inference import FeatureDetectorFactory, SMDetector

        # Create and stub the SageMaker client
        sm_runtime_client = boto3.client("sagemaker-runtime")

        mock_boto3.client.return_value = sm_runtime_client

        feature_detector = FeatureDetectorFactory(
            endpoint="test",
            endpoint_mode=ModelInvokeMode.SM_ENDPOINT,
        ).build()

        # Verify that the detector is an instance of SMDetector and has the correct mode
        assert isinstance(feature_detector, SMDetector)
        assert feature_detector.mode == ModelInvokeMode.SM_ENDPOINT

    def test_http_detector_generation(self):
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

    def test_async_sm_detector_generation(self):
        """
        Test that the FeatureDetectorFactory correctly creates an AsyncSMDetector
        when the endpoint mode is set to ModelInvokeMode.SM_ENDPOINT_ASYNC.
        """
        from unittest.mock import patch
        from aws.osml.model_runner.api.inference import ModelInvokeMode
        from aws.osml.model_runner.inference import FeatureDetectorFactory
        from aws.osml.model_runner.inference.async_sm_detector import AsyncSMDetector

        with patch("aws.osml.model_runner.inference.async_sm_detector.S3_MANAGER"):
            feature_detector = FeatureDetectorFactory(
                endpoint="test-async",
                endpoint_mode=ModelInvokeMode.SM_ENDPOINT_ASYNC,
            ).build()

            # Verify that the detector is an instance of AsyncSMDetector
            assert isinstance(feature_detector, AsyncSMDetector)
            assert feature_detector.endpoint == "test-async"

    def test_batch_sm_detector_generation(self):
        """
        Test that the FeatureDetectorFactory correctly creates a BatchSMDetector
        when the endpoint mode is set to ModelInvokeMode.SM_BATCH.
        """
        from unittest.mock import patch
        from aws.osml.model_runner.api.inference import ModelInvokeMode
        from aws.osml.model_runner.inference import FeatureDetectorFactory
        from aws.osml.model_runner.inference.batch_sm_detector import BatchSMDetector

        with patch("aws.osml.model_runner.inference.batch_sm_detector.S3_MANAGER"):
            feature_detector = FeatureDetectorFactory(
                endpoint="test-batch-model",
                endpoint_mode=ModelInvokeMode.SM_BATCH,
            ).build()

            # Verify that the detector is an instance of BatchSMDetector
            assert isinstance(feature_detector, BatchSMDetector)
            assert feature_detector.endpoint == "test-batch-model"


if __name__ == "__main__":
    main()
