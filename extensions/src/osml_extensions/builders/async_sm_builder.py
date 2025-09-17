#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
from typing import Dict, Optional

from aws.osml.model_runner.inference.detector import Detector
from aws.osml.model_runner.inference.sm_detector import SMDetectorBuilder
from osml_extensions.detectors.async_sm_detector import AsyncSMDetector

logger = logging.getLogger(__name__)


class AsyncSMDetectorBuilder(SMDetectorBuilder):
    """
    AsyncSMDetectorBuilder extends SMDetectorBuilder to create AsyncSMDetector instances.
    
    This builder maintains compatibility with the base SMDetectorBuilder.
    """

    def __init__(self, endpoint: str, assumed_credentials: Dict[str, str] = None):
        """
        Initializes the AsyncSMDetectorBuilder.

        :param endpoint: str = The name of the SageMaker endpoint to be used.
        :param assumed_credentials: Dict[str, str] = Optional credentials to use with the SageMaker endpoint.
        """
        super().__init__(endpoint, assumed_credentials)
        logger.info(f"AsyncSMDetectorBuilder initialized for endpoint: {endpoint}")

    def build(self) -> Optional[Detector]:
        """
        Builds and returns an AsyncSMDetector based on the configured parameters.

        :return: Optional[Detector] = An AsyncSMDetector instance configured for the specified SageMaker endpoint.
        """
        try:
            logger.debug(f"Building AsyncSMDetector for endpoint: {self.endpoint}")
            
            detector = AsyncSMDetector(
                endpoint=self.endpoint,
                assumed_credentials=self.assumed_credentials
            )
            
            logger.info(f"Successfully built AsyncSMDetector for endpoint: {self.endpoint}")
            return detector
            
        except Exception as e:
            logger.error(f"Failed to build AsyncSMDetector for endpoint {self.endpoint}: {e}")
            # Fallback to base implementation if async detector fails
            logger.info("Falling back to base SMDetector implementation")
            return super().build()