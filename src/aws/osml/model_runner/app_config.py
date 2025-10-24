#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from aws_embedded_metrics.config import Configuration, get_config
from botocore.config import Config

from aws.osml.gdal import GDALDigitalElevationModelTileFactory
from aws.osml.photogrammetry import DigitalElevationModel, ElevationModel, GenericDEMTileSet


@dataclass
class AsyncEndpointConfig:
    """
    Configuration class for async SageMaker endpoint settings.

    This class provides comprehensive configuration options for async endpoint operations
    including S3 bucket settings, polling parameters, and worker pool optimization.
    """

    # Load other environment variables with current values as defaults
    input_bucket = os.getenv("ARTIFACT_BUCKET")
    input_prefix = os.getenv("ASYNC_SM_INPUT_PREFIX", "async-inference/input/")
    max_wait_time = int(os.getenv("ASYNC_SM_MAX_WAIT_TIME", 3600))  # Maximum wait time in seconds
    max_retries = int(os.getenv("ASYNC_SM_MAX_RETRIES", 3))  # For S3 operations

    submission_workers = int(os.getenv("ASYNC_SM_SUBMISSION_WORKERS", 4))  # Number of workers for submitting async requests
    polling_workers = int(os.getenv("ASYNC_SM_POLLING_WORKERS", 2))  # Number of workers for polling results

    @staticmethod
    def get_input_s3_uri(input_bucket: str, input_prefix: str, key: str) -> str:
        """Generate input S3 URI for the given key."""
        return f"s3://{input_bucket}/{input_prefix}{key}"


@dataclass
class ServiceConfig:
    """
    ServiceConfig is a dataclass meant to house the high-level configuration settings required for Model Runner to
    operate that are provided through ENV variables. Note that required env parameters are enforced by the implied
    schema validation as os.environ[] is used to fetch the values. Optional parameters are fetched using, os.getenv(),
    which returns None.
    """

    # Required env configuration

    # region
    aws_region: str = os.environ["AWS_DEFAULT_REGION"]

    # image/region/tile tables
    job_table: str = os.environ["JOB_TABLE"]
    region_request_table: str = os.environ["REGION_REQUEST_TABLE"]
    tile_request_table: str = os.environ["TILE_REQUEST_TABLE"]
    endpoint_statistics_table = os.environ["ENDPOINT_TABLE"]

    # feature table
    feature_table: str = os.environ["FEATURE_TABLE"]

    # image/region/tile queues
    image_queue: str = os.environ["IMAGE_QUEUE"]
    region_queue: str = os.environ["REGION_QUEUE"]
    tile_queue: str = os.environ.get("TILE_QUEUE")

    # workers
    workers_per_cpu: str = os.environ["WORKERS_PER_CPU"]
    workers: str = os.environ["WORKERS"]

    # Optional parameters

    # Optional elevation data
    elevation_data_location: Optional[str] = os.getenv("ELEVATION_DATA_LOCATION")
    elevation_data_format_spec: Optional[str] = os.getenv("ELEVATION_DATA_FORMAT_SPEC", "%oh%od/%lh%ld.dt2")
    elevation_data_min_lat_deg: Optional[float] = float(os.getenv("ELEVATION_DATA_MIN_LAT_DEG", "-90.0"))
    elevation_data_max_lat_deg: Optional[float] = float(os.getenv("ELEVATION_DATA_MAX_LAT_DEG", "90.0"))
    elevation_data_min_long_deg: Optional[float] = float(os.getenv("ELEVATION_DATA_MIN_LONG_DEG", "-180.0"))
    elevation_data_max_long_deg: Optional[float] = float(os.getenv("ELEVATION_DATA_MAX_LONG_DEG", "180.0"))
    elevation_model: Optional[ElevationModel] = field(init=False, default=None)

    # Optional status topics
    image_status_topic: Optional[str] = os.getenv("IMAGE_STATUS_TOPIC")
    region_status_topic: Optional[str] = os.getenv("REGION_STATUS_TOPIC")
    tile_status_topic: Optional[str] = os.getenv("TILE_STATUS_TOPIC")

    # Optional env variables
    cp_api_endpoint: Optional[str] = os.getenv("API_ENDPOINT")
    self_throttling: bool = (
        os.getenv("SM_SELF_THROTTLING", "False") == "True" or os.getenv("SM_SELF_THROTTLING", "False") == "true"
    )

    # Optional + defaulted configuration
    region_size: str = os.getenv("REGION_SIZE", "(10240, 10240)")
    throttling_vcpu_scale_factor: str = os.getenv("THROTTLING_SCALE_FACTOR", "10")
    throttling_retry_timeout: str = os.getenv("THROTTLING_RETRY_TIMEOUT", "10")

    # Constant configuration
    kinesis_max_record_per_batch: str = "500"
    kinesis_max_record_size_batch: str = "5242880"  # 5 MB in bytes
    kinesis_max_record_size: str = "1048576"  # 1 MB in bytes
    ddb_max_item_size: str = "200000"

    # Metrics configuration
    metrics_config: Configuration = field(init=False, default=None)

    # async endpoint config
    async_endpoint_config: AsyncEndpointConfig = field(default=AsyncEndpointConfig)

    def __post_init__(self):
        """
        Post-initialization method to set up the elevation model.
        """
        self.elevation_model = self.create_elevation_model()
        self.metrics_config = self.configure_metrics()

    def create_elevation_model(self) -> Optional[ElevationModel]:
        """
        Create an elevation model if the relevant options are set in the service configuration.

        :return: Optional[ElevationModel] = the elevation model or None if not configured
        """
        if self.elevation_data_location:
            return DigitalElevationModel(
                GenericDEMTileSet(
                    format_spec=ServiceConfig.elevation_data_format_spec,
                    min_latitude_degrees=ServiceConfig.elevation_data_min_lat_deg,
                    max_latitude_degrees=ServiceConfig.elevation_data_max_lat_deg,
                    min_longitude_degrees=ServiceConfig.elevation_data_min_long_deg,
                    max_longitude_degrees=ServiceConfig.elevation_data_max_long_deg,
                ),
                GDALDigitalElevationModelTileFactory(self.elevation_data_location),
            )
        return None

    @staticmethod
    def configure_metrics():
        """
        Embedded metrics configuration
        """
        metrics_config = get_config()
        metrics_config.service_name = "OSML"
        metrics_config.log_group_name = "/aws/OSML/MRService"
        metrics_config.namespace = "OSML/ModelRunner"
        metrics_config.environment = "local"

        return metrics_config


@dataclass
class BotoConfig:
    """
    BotoConfig is a dataclass meant to vend our application the set of boto client configurations required for OSML

    The data schema is defined as follows:
    default:  (Config) the standard boto client configuration
    sagemaker: (Config) the sagemaker specific boto client configuration
    """

    default: Config = Config(region_name=ServiceConfig.aws_region, retries={"max_attempts": 15, "mode": "standard"})
    sagemaker: Config = Config(region_name=ServiceConfig.aws_region, retries={"max_attempts": 30, "mode": "adaptive"})
    ddb: Config = Config(
        region_name=ServiceConfig.aws_region, retries={"max_attempts": 3, "mode": "standard"}, max_pool_connections=50
    )


class MetricLabels(str, Enum):
    """
    Enumeration defining the metric labels used by OSML
    """

    # These are based on common metric names used by a variety of AWS services (e.g. Lambda)
    DURATION = "Duration"
    INVOCATIONS = "Invocations"
    ERRORS = "Errors"
    THROTTLES = "Throttles"
    RETRIES = "Retries"

    # These dimensions allow us to limit the scope of a metric value to a particular portion of the
    # ModelRunner application, a data type, or input format.
    OPERATION_DIMENSION = "Operation"
    MODEL_NAME_DIMENSION = "ModelName"
    INPUT_FORMAT_DIMENSION = "InputFormat"

    # These operation names can be used along with the Operation dimension to restrict the scope
    # of the common metrics to a specific portion of the ModelRunner application.
    IMAGE_PROCESSING_OPERATION = "ImageProcessing"
    REGION_PROCESSING_OPERATION = "RegionProcessing"
    TILE_GENERATION_OPERATION = "TileGeneration"
    TILE_PROCESSING_OPERATION = "TileProcessing"
    MODEL_INVOCATION_OPERATION = "ModelInvocation"
    FEATURE_REFINEMENT_OPERATION = "FeatureRefinement"
    FEATURE_STORAGE_OPERATION = "FeatureStorage"
    FEATURE_AGG_OPERATION = "FeatureAggregation"
    FEATURE_SELECTION_OPERATION = "FeatureSelection"
    FEATURE_DISSEMINATE_OPERATION = "FeatureDissemination"
