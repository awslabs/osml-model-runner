#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

from typing import Dict, Optional

from aws.osml.model_runner.api import ModelInvokeMode

from .batch_sm_detector import BatchSMDetectorBuilder
from .async_sm_detector import AsyncSMDetectorBuilder
from .detector import Detector
from .http_detector import HTTPDetectorBuilder
from .sm_detector import SMDetectorBuilder


class FeatureDetectorFactory:
    def __init__(
        self, endpoint: str, endpoint_mode: ModelInvokeMode, assumed_credentials: Optional[Dict[str, str]] = None
    ) -> None:
        """
        :param endpoint: URL of the inference model endpoint
        :param endpoint_mode: the type of endpoint (HTTP, SageMaker)
        :param assumed_credentials: optional credentials to use with the model
        """

        self.endpoint = endpoint
        self.endpoint_mode = endpoint_mode
        self.assumed_credentials = assumed_credentials

    def build(self) -> Optional[Detector]:
        """
        :return: a feature detector based on the parameters defined during initialization
        """

        builder_class = None
        match self.endpoint_mode:
            case ModelInvokeMode.SM_ENDPOINT:
                builder_class = SMDetectorBuilder
            case ModelInvokeMode.HTTP_ENDPOINT:
                builder_class = HTTPDetectorBuilder
            case ModelInvokeMode.SM_ENDPOINT_ASYNC:
                builder_class = AsyncSMDetectorBuilder
            case ModelInvokeMode.SM_BATCH:
                builder_class = BatchSMDetectorBuilder
        
        if not builder_class:
            raise ValueError(f"Unknown endpoint mode: {self.endpoint_mode}")
        return builder_class(
            endpoint=self.endpoint, 
            assumed_credentials=self.assumed_credentials,
        ).build()
