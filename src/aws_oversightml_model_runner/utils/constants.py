from botocore.config import Config

from aws_oversightml_model_runner.classes.service_config import ServiceConfig

# SERVICE CONFIGURATION
SERVICE_CONFIG = ServiceConfig()
BOTO_CONFIG = Config(
    region_name=SERVICE_CONFIG.region, retries={"max_attempts": 15, "mode": "standard"}
)
BOTO_SM_CONFIG = Config(
    region_name=SERVICE_CONFIG.region, retries={"max_attempts": 30, "mode": "adaptive"}
)

# METRICS
ENDPOINT_LATENCY_METRIC = "EndpointLatency"
FEATURE_AGG_LATENCY_METRIC = "FeatureAggLatency"
FEATURE_ERROR_METRIC = "FeatureError"
FEATURE_STORE_LATENCY_METRIC = "FeatureStoreLatency"
IMAGE_PROCESSING_ERROR_METRIC = "ImageProcessingError"
METADATA_LATENCY_METRIC = "MetadataLatency"
MODEL_INVOCATION_METRIC = "ModelInvocation"
MODEL_ERROR_METRIC = "ModelError"
REGION_LATENCY_METRIC = "RegionLatency"
REGION_PROCESSING_ERROR_METRIC = "RegionProcessingError"
REGIONS_PROCESSED_METRIC = "RegionsProcessed"
TILING_LATENCY_METRIC = "TilingLatency"
TILES_PROCESSED_METRIC = "TilesProcessed"

# ERROR CODES
FEATURE_DECODE_ERROR_CODE = "FeatureDecodeError"
FEATURE_MISSING_GEO_ERROR_CODE = "FeatureMissingGeometry"
FEATURE_TO_SHAPE_ERROR_CODE = "FeatureToShapeConversion"
FEATURE_UPDATE_ERROR_CODE = "FeatureUpdateFailure"
FEATURE_UPDATE_EXCEPTION_ERROR_CODE = "FeatureUpdateException"
INVALID_REQUEST_ERROR_CODE = "InvalidRequest"
INVALID_ROI_ERROR_CODE = "InvalidROI"
NO_IMAGE_URL_ERROR_CODE = "NoImageURL"
PROCESSING_FAILURE_ERROR_CODE = "ProcessingFailure"
TILE_CREATION_FAILURE_ERROR_CODE = "TileCreationFailure"
UNSUPPORTED_MODEL_HOST_ERROR_CODE = "UnsupportedModelHost"
