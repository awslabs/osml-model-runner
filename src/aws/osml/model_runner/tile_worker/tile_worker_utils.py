#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import ast
import json
import logging
from queue import Queue
from typing import List, Optional, Tuple

from aws_embedded_metrics import MetricsLogger
from aws_embedded_metrics.metric_scope import metric_scope
from aws_embedded_metrics.unit import Unit
from geojson import Feature

from aws.osml.features import Geolocator, ImagedFeaturePropertyAccessor
from aws.osml.model_runner.api import RegionRequest
from aws.osml.model_runner.app_config import MetricLabels, ServiceConfig
from aws.osml.model_runner.common import (
    FeatureDistillationDeserializer,
    ImageRegion,
    Timer,
    get_credentials_for_assumed_role
)
from aws.osml.model_runner.database import FeatureTable, RegionRequestTable
from aws.osml.model_runner.inference import FeatureSelector
from aws.osml.model_runner.inference.endpoint_factory import FeatureDetectorFactory
from aws.osml.photogrammetry import ElevationModel, SensorModel

from .exceptions import SetupTileWorkersException
from .tile_worker import TileWorker
from .tiling_strategy import TilingStrategy

logger = logging.getLogger(__name__)


def setup_tile_workers(
    region_request: RegionRequest,
    sensor_model: Optional[SensorModel] = None,
    elevation_model: Optional[ElevationModel] = None,
) -> Tuple[Queue, List[TileWorker]]:
    """
    Sets up a pool of tile-workers to process image tiles from a region request

    :param region_request: RegionRequest = the region request to update.
    :param sensor_model: Optional[SensorModel] = the sensor model for this raster dataset
    :param elevation_model: Optional[ElevationModel] = an elevation model used to fix the elevation of the image coordinate

    :return: Tuple[Queue, List[TileWorker] = a list of tile workers and the queue that manages them
    """
    try:
        model_invocation_credentials = None
        if region_request.model_invocation_role:
            model_invocation_credentials = get_credentials_for_assumed_role(region_request.model_invocation_role)

        # Set up a Queue to manage our tile workers
        tile_queue: Queue = Queue()
        tile_workers = []

        for _ in range(int(ServiceConfig.workers)):
            # Set up our feature table to work with the region quest
            feature_table = FeatureTable(
                ServiceConfig.feature_table,
                region_request.tile_size,
                region_request.tile_overlap,
            )

            # Set up our feature table to work with the region quest
            region_request_table = RegionRequestTable(ServiceConfig.region_request_table)

            # Ignoring mypy error - if model_name was None the call to validate the region
            # request at the start of this function would have failed
            feature_detector = FeatureDetectorFactory(
                endpoint=region_request.model_name,
                endpoint_mode=region_request.model_invoke_mode,
                assumed_credentials=model_invocation_credentials,
            ).build()

            geolocator = None
            if sensor_model is not None:
                geolocator = Geolocator(ImagedFeaturePropertyAccessor(), sensor_model, elevation_model=elevation_model)

            worker = TileWorker(tile_queue, feature_detector, geolocator, feature_table, region_request_table)
            worker.start()
            tile_workers.append(worker)

        logger.debug(f"Setup pool of {len(tile_workers)} tile workers")

        return tile_queue, tile_workers
    except Exception as err:
        logger.exception(f"Failed to setup tile workers!: {err}")
        raise SetupTileWorkersException("Failed to setup tile workers!") from err


@metric_scope
def _create_tile(gdal_tile_factory, tile_bounds, tmp_image_path, metrics: MetricsLogger = None) -> Optional[str]:
    """
    Create an encoded tile of the requested image region.

    :param gdal_tile_factory: the factory used to create the tile
    :param tile_bounds: the requested tile boundary
    :param tmp_image_path: the output location of the tile
    :param metrics: the current metrics scope
    :return: the resulting tile path or None if the tile could not be created
    """
    if isinstance(metrics, MetricsLogger):
        metrics.set_dimensions()
        metrics.put_dimensions(
            {
                MetricLabels.OPERATION_DIMENSION: MetricLabels.TILE_GENERATION_OPERATION,
                MetricLabels.INPUT_FORMAT_DIMENSION: str(gdal_tile_factory.raster_dataset.GetDriver().ShortName).upper(),
            }
        )

    # Use GDAL to create an encoded tile of the image region
    absolute_tile_path = tmp_image_path.absolute()
    with Timer(
        task_str=f"Creating image tile: {absolute_tile_path}",
        metric_name=MetricLabels.DURATION,
        logger=logger,
        metrics_logger=metrics,
    ):
        if isinstance(metrics, MetricsLogger):
            metrics.put_metric(MetricLabels.INVOCATIONS, 1, str(Unit.COUNT.value))

        encoded_tile_data = gdal_tile_factory.create_encoded_tile(
            [tile_bounds[0][1], tile_bounds[0][0], tile_bounds[1][0], tile_bounds[1][1]]
        )

        with open(absolute_tile_path, "wb") as binary_file:
            binary_file.write(encoded_tile_data)

    # GDAL doesn't always generate errors, so we need to make sure the NITF
    # encoded region was actually created.
    if not tmp_image_path.is_file():
        logger.error(
            "GDAL unable to create tile %s. Does not exist!",
            absolute_tile_path,
        )
        if isinstance(metrics, MetricsLogger):
            metrics.put_metric(MetricLabels.ERRORS, 1, str(Unit.COUNT.value))
        return None
    else:
        logger.debug(
            "Created %s size %s",
            absolute_tile_path,
            sizeof_fmt(tmp_image_path.stat().st_size),
        )

    return absolute_tile_path


def sizeof_fmt(num: float, suffix: str = "B") -> str:
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, "Yi", suffix)


def select_features(
    feature_distillation_option: str,
    features: List[Feature],
    processing_bounds: ImageRegion,
    region_size: str,
    tile_size: str,
    tile_overlap: str,
    tiling_strategy: TilingStrategy,
) -> List[Feature]:
    """
    Selects the desired features using the options in the JobItem (NMS, SOFT_NMS, etc.).
    This code applies a feature selector only to the features that came from regions of the image
    that were processed multiple times. First features are grouped based on the region they were
    processed in. Any features found in the overlap area between regions are run through the
    FeatureSelector. If they were not part of an overlap area between regions, they will be grouped
    based on tile boundaries. Any features that fall into the overlap of adjacent tiles are filtered
    by the FeatureSelector. All other features should not be duplicates; they are added to the result
    without additional filtering.

    Computationally, this implements two critical factors that lower the overall processing time for the
    O(N^2) selection algorithms. First, it will filter out the majority of features that couldn't possibly
    have duplicates generated by our tiled image processing; Second, it runs the selection algorithms
    incrementally on much smaller groups of features.

    :param region_size:
    :param feature_distillation_option: str = the options used in selecting features (e.g., NMS/SOFT_NMS, thresholds)
    :param features: List[Feature] = the list of geojson features to process
    :param processing_bounds: the requested area of the image
    :param region_size: str = region size to use for feature dedup
    :param tile_size: str = size of the tiles used during processing
    :param tile_overlap: str = overlap between tiles during processing
    :param tiling_strategy: the tiling strategy to use for feature dedup
    :return: List[Feature] = the list of geojson features after processing
    """
    feature_distillation_option_dict = json.loads(feature_distillation_option)
    feature_distillation_option = FeatureDistillationDeserializer().deserialize(feature_distillation_option_dict)
    feature_selector = FeatureSelector(feature_distillation_option)

    region_size = ast.literal_eval(region_size)
    tile_size = ast.literal_eval(tile_size)
    overlap = ast.literal_eval(tile_overlap)
    deduped_features = tiling_strategy.cleanup_duplicate_features(
        processing_bounds, region_size, tile_size, overlap, features, feature_selector
    )

    return deduped_features
