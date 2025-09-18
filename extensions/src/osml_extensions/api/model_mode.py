from enum import auto
from aws.osml.model_runner.common import AutoStringEnum


class ExtendedModelInvokeMode(str, AutoStringEnum):
    """
    Extended enumeration defining additional hosting options for CV models.
    Inherits from ModelInvokeMode and adds async SageMaker endpoint support.
    """

    NONE = auto()
    SM_ENDPOINT = auto()
    HTTP_ENDPOINT = auto()
    SM_ENDPOINT_ASYNC = auto()
