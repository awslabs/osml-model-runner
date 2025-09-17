#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import asyncio
import logging
from io import BufferedReader
from json import JSONDecodeError
from typing import Dict, Optional
import concurrent.futures

import boto3
import geojson
from aws_embedded_metrics.logger.metrics_logger import MetricsLogger
from aws_embedded_metrics.metric_scope import metric_scope
from aws_embedded_metrics.unit import Unit
from botocore.exceptions import ClientError
from geojson import FeatureCollection

from aws.osml.model_runner.api import ModelInvokeMode
from aws.osml.model_runner.app_config import BotoConfig, MetricLabels
from aws.osml.model_runner.common import Timer
from aws.osml.model_runner.inference.sm_detector import SMDetector

logger = logging.getLogger(__name__)


class AsyncSMDetector(SMDetector):
    """
    AsyncSMDetector extends SMDetector with asynchronous processing capabilities.
    
    This class maintains full compatibility with the base SMDetector while adding
    enhanced features for improved performance.
    """

    def __init__(self, endpoint: str, assumed_credentials: Dict[str, str] = None) -> None:
        """
        Initializes the AsyncSMDetector.

        :param endpoint: str = The name of the SageMaker endpoint to invoke.
        :param assumed_credentials: Dict[str, str] = Optional credentials for invoking the SageMaker model.
        """
        super().__init__(endpoint, assumed_credentials)
        logger.info(f"AsyncSMDetector initialized for endpoint: {endpoint}")

    def _add_processing_metadata(self, features: FeatureCollection) -> FeatureCollection:
        """
        Add processing metadata to the detected features.
        
        :param features: FeatureCollection = The original features from SageMaker
        :return: FeatureCollection = Features with added metadata
        """
        try:
            if 'features' in features:
                for feature in features['features']:
                    if 'properties' not in feature:
                        feature['properties'] = {}
                    feature['properties']['processed_by'] = 'AsyncSMDetector'
            
            return features
            
        except Exception as e:
            logger.warning(f"Failed to add metadata, using original features: {e}")
            return features

    @metric_scope
    def find_features(self, payload: BufferedReader, metrics: MetricsLogger) -> FeatureCollection:
        """
        Enhanced feature detection with metadata addition.
        
        This method extends the base implementation while maintaining full compatibility.

        :param payload: BufferedReader = The data to be sent to the SageMaker model for feature detection.
        :param metrics: MetricsLogger = The metrics logger to capture system performance and log metrics.

        :return: FeatureCollection = A geojson FeatureCollection containing the detected features.

        :raises ClientError: Raised if there is an error while invoking the SageMaker endpoint.
        :raises JSONDecodeError: Raised if there is an error decoding the model's response.
        """
        logger.debug(f"AsyncSMDetector processing request for endpoint: {self.endpoint}")
        
        try:
            # Add custom metrics
            if isinstance(metrics, MetricsLogger):
                metrics.put_metric("AsyncSMDetector.Invocations", 1, str(Unit.COUNT.value))
            
            # Call parent implementation
            features = super().find_features(payload, metrics)
            
            # Add processing metadata
            enhanced_features = self._add_processing_metadata(features)
            
            logger.debug(f"AsyncSMDetector completed processing: "
                        f"{len(enhanced_features.get('features', []))} features detected")
            
            return enhanced_features
            
        except Exception as e:
            logger.error(f"AsyncSMDetector error: {e}")
            if isinstance(metrics, MetricsLogger):
                metrics.put_metric("AsyncSMDetector.Errors", 1, str(Unit.COUNT.value))
            raise

    async def find_features_async(self, payload: BufferedReader, metrics: MetricsLogger) -> FeatureCollection:
        """
        Asynchronous version of find_features for improved performance in async contexts.
        
        :param payload: BufferedReader = The data to be sent to the SageMaker model
        :param metrics: MetricsLogger = The metrics logger
        :return: FeatureCollection = Detected features
        """
        loop = asyncio.get_event_loop()
        
        # Run the synchronous find_features in a thread pool
        with concurrent.futures.ThreadPoolExecutor() as executor:
            future = loop.run_in_executor(executor, self.find_features, payload, metrics)
            return await future