import logging
from json import JSONDecodeError
from typing import Dict

import boto3
import botocore
import geojson
from aws_embedded_metrics.metric_scope import metric_scope
from aws_embedded_metrics.unit import Unit
from geojson import FeatureCollection

from aws_oversightml_model_runner.classes.timer import Timer
from aws_oversightml_model_runner.utils.constants import (
    BOTO_SM_CONFIG,
    ENDPOINT_LATENCY_METRIC,
    FEATURE_DECODE_ERROR_CODE,
    MODEL_ERROR_METRIC,
    MODEL_INVOCATION_METRIC,
)

logger = logging.getLogger(__name__)


class FeatureDetector:
    def __init__(self, model_name: str, assumed_credentials: Dict[str, str] = None):
        if assumed_credentials is not None:
            # Here we will be invoking the SageMaker endpoints using an IAM role other than the
            # one for this process. Use those credentials when creating the Boto3 SageMaker client.
            # This is the typical case when the SageMaker endpoints do not reside in the same AWS
            # account as the model runner.
            self.sm_client = boto3.client(
                "sagemaker-runtime",
                config=BOTO_SM_CONFIG,
                aws_access_key_id=assumed_credentials["AccessKeyId"],
                aws_secret_access_key=assumed_credentials["SecretAccessKey"],
                aws_session_token=assumed_credentials["SessionToken"],
            )
        else:
            # If no invocation role is provided the assumption is that the default role for this
            # container will be sufficient to invoke the SageMaker endpoints. This will typically
            # be the case for AWS managed models running in the same account as the model runner.
            self.sm_client = boto3.client("sagemaker-runtime", config=BOTO_SM_CONFIG)
        self.model_name = model_name
        self.request_count = 0
        self.error_count = 0

    @metric_scope
    def find_features(self, payload, metrics) -> FeatureCollection:
        logger.info("Invoking Model: {}".format(self.model_name))

        metrics.put_dimensions({"ModelName": self.model_name})

        try:
            self.request_count += 1
            metrics.put_metric(MODEL_INVOCATION_METRIC, 1, Unit.COUNT.value)

            with Timer(
                task_str="Invoke SM Endpoint",
                metric_name=ENDPOINT_LATENCY_METRIC,
                logger=logger,
                metrics_logger=metrics,
            ):
                model_response = self.sm_client.invoke_endpoint(
                    EndpointName=self.model_name, Body=payload
                )

            feature_collection = geojson.loads(model_response["Body"].read())
            return feature_collection

        except botocore.exceptions.ClientError as ce:
            self.error_count += 1
            metrics.put_dimensions(
                {"StatusCode": ce.response["ResponseMetadata"]["HTTPStatusCode"]}
            )
            metrics.put_dimensions({"ErrorCode": ce.response["Error"]["Code"]})
            metrics.put_metric(MODEL_ERROR_METRIC, 1, Unit.COUNT.value)
            logger.error("Unable to get detections from model.")
            logger.exception(ce)
        except JSONDecodeError as de:
            self.error_count += 1
            metrics.put_dimensions({"ErrorCode": FEATURE_DECODE_ERROR_CODE})
            metrics.put_metric(MODEL_ERROR_METRIC, 1, Unit.COUNT.value)
            logger.error("Unable to decode response from model.")
            logger.exception(de)

        return FeatureCollection([])
