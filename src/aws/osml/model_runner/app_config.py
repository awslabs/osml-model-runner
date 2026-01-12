#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

import logging
import os
from dataclasses import dataclass, field
from enum import Enum
from typing import Optional

from aws_embedded_metrics.config import Configuration, get_config
from botocore.config import Config

from aws.osml.gdal import GDALDigitalElevationModelTileFactory
from aws.osml.photogrammetry import DigitalElevationModel, ElevationModel, SRTMTileSet

logger = logging.getLogger(__name__)


@dataclass
class ServiceConfig:
    """
    ServiceConfig is a dataclass meant to house the high-level configuration settings required for Model Runner to
    operate that are provided through ENV variables. Note that required env parameters are enforced by the implied
    schema validation as os.environ[] is used to fetch the values. Optional parameters are fetched using, os.getenv(),
    which returns None.
    """

    # Required env configuration
    aws_region: str = os.environ["AWS_DEFAULT_REGION"]
    image_request_table: str = os.environ["IMAGE_REQUEST_TABLE"]
    outstanding_jobs_table: str = os.environ["OUTSTANDING_IMAGE_REQUEST_TABLE"]
    region_request_table: str = os.environ["REGION_REQUEST_TABLE"]
    feature_table: str = os.environ["FEATURE_TABLE"]
    image_queue: str = os.environ["IMAGE_QUEUE"]
    image_dlq: str = os.environ["IMAGE_DLQ"]
    region_queue: str = os.environ["REGION_QUEUE"]
    workers: str = os.environ["WORKERS"]

    # Optional elevation data
    elevation_data_location: Optional[str] = os.getenv("ELEVATION_DATA_LOCATION")
    elevation_data_extension: str = os.getenv("ELEVATION_DATA_EXTENSION", ".tif")
    elevation_data_version: str = os.getenv("ELEVATION_DATA_VERSION", "1arc_v3")
    elevation_model: Optional[ElevationModel] = field(init=False, default=None)

    # Optional env configuration
    image_status_topic: Optional[str] = os.getenv("IMAGE_STATUS_TOPIC")
    region_status_topic: Optional[str] = os.getenv("REGION_STATUS_TOPIC")
    cp_api_endpoint: Optional[str] = os.getenv("API_ENDPOINT")

    # Optional + defaulted configuration
    ddb_ttl_in_days: int = int(os.getenv("DDB_TTL_IN_DAYS", "1"))
    region_size: str = os.getenv("REGION_SIZE", "(10240, 10240)")

    # Capacity-based throttling configuration
    scheduler_throttling_enabled: bool = os.getenv("SCHEDULER_THROTTLING_ENABLED", "True") in ["True", "true"]
    default_instance_concurrency: int = int(os.getenv("DEFAULT_INSTANCE_CONCURRENCY", "2"))
    default_http_endpoint_concurrency: int = int(os.getenv("DEFAULT_HTTP_ENDPOINT_CONCURRENCY", "10"))
    tile_workers_per_instance: int = int(os.getenv("TILE_WORKERS_PER_INSTANCE", "4"))
    capacity_target_percentage: float = float(os.getenv("CAPACITY_TARGET_PERCENTAGE", "1.0"))

    # Constant configuration
    kinesis_max_record_per_batch: str = "500"
    kinesis_max_record_size_batch: str = "5242880"  # 5 MB in bytes
    kinesis_max_record_size: str = "1048576"  # 1 MB in bytes
    ddb_max_item_size: str = "200000"

    # Metrics configuration
    metrics_config: Configuration = field(init=False, default=None)

    def __post_init__(self):
        """
        Post-initialization method to set up the elevation model and validate configuration.
        """
        self._validate_configuration()
        self.elevation_model = self.create_elevation_model()
        self.metrics_config = self.configure_metrics()

    def _validate_configuration(self) -> None:
        """
        Validate capacity-based throttling configuration parameters.

        Invalid values are replaced with safe defaults and warnings are logged.
        """
        # Validate capacity_target_percentage > 0.0
        if self.capacity_target_percentage <= 0.0:
            logger.warning(
                f"Invalid capacity_target_percentage: {self.capacity_target_percentage}. "
                "Must be greater than 0.0. Defaulting to 1.0."
            )
            self.capacity_target_percentage = 1.0

        # Validate default_instance_concurrency >= 1
        if self.default_instance_concurrency < 1:
            logger.warning(
                f"Invalid default_instance_concurrency: {self.default_instance_concurrency}. "
                "Must be at least 1. Defaulting to 2."
            )
            self.default_instance_concurrency = 2

        # Validate tile_workers_per_instance >= 1
        if self.tile_workers_per_instance < 1:
            logger.warning(
                f"Invalid tile_workers_per_instance: {self.tile_workers_per_instance}. "
                "Must be at least 1. Defaulting to 4."
            )
            self.tile_workers_per_instance = 4

    def create_elevation_model(self) -> Optional[ElevationModel]:
        """
        Create an elevation model if the relevant options are set in the service configuration.

        :return: Optional[ElevationModel] = the elevation model or None if not configured
        """
        if self.elevation_data_location:
            return DigitalElevationModel(
                SRTMTileSet(
                    version=self.elevation_data_version,
                    format_extension=self.elevation_data_extension,
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
    UTILIZATION = "Utilization"

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
    SCHEDULING_OPERATION = "Scheduling"
