#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import unittest


class TestFeatureDetectorFactory(unittest.TestCase):
    def test_sm_detector_generation(self):
        """
        Test that the FeatureDetectorFactory correctly creates an SMDetector
        when the endpoint mode is set to ModelInvokeMode.SM_ENDPOINT.
        """
        from aws.osml.model_runner.api.inference import ModelInvokeMode
        from aws.osml.model_runner.inference import FeatureDetectorFactory, SMDetector

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
    unittest.main()
