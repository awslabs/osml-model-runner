import logging

import boto3
import geojson
import botocore
from botocore.config import Config
from geojson import FeatureCollection
from json import JSONDecodeError

from metrics import metric_scope
from metrics import now


class FeatureDetector:

    def __init__(self, model_endpoint: str):
        config = Config(
            retries={
                'max_attempts': 30,
                'mode': 'adaptive'
            }
        )
        self.sm_client = boto3.client('sagemaker-runtime', config=config)
        self.model_endpoint = model_endpoint
        self.request_count = 0
        self.error_count = 0

    @metric_scope
    def find_features(self, payload, metrics) -> FeatureCollection:
        logging.info("Invoking SM Endpoint: {}".format(self.model_endpoint))

        metrics.put_dimensions({"ModelEndpoint": self.model_endpoint})

        try:
            self.request_count += 1
            metrics.put_metric("ModelInvocation", 1, "Count")

            ml_endpoint_start = now()
            model_response = self.sm_client.invoke_endpoint(
                EndpointName=self.model_endpoint,
                Body=payload)
            ml_endpoint_end = now()
            metrics.put_metric("EndpointLatency", (ml_endpoint_end - ml_endpoint_start), "Microseconds")

            # decode_start = now()
            feature_collection = geojson.loads(model_response['Body'].read())
            # decode_end = now()

            # Excluding this metric, as measured it was very much in the weeds
            # metrics.put_metric("ResultDecodeLatency", (decode_end - decode_start), "Microseconds")

            return feature_collection

        except botocore.exceptions.ClientError as de:
            self.error_count += 1
            metrics.put_metric("ModelErrors", 1, "Count")
            logging.error("Unable to get detections from model endpoint.")
            logging.exception(de)
        except JSONDecodeError as de:
            self.error_count += 1
            metrics.put_metric("ModelErrors", 1, "Count")
            logging.error("Unable to decode response from model endpoint.")
            logging.exception(de)

        return FeatureCollection([])
