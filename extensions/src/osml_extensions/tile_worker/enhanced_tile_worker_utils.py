#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
import traceback
from queue import Queue
from typing import List, Optional, Tuple, Type

from aws.osml.features import Geolocator, ImagedFeaturePropertyAccessor
from aws.osml.model_runner.api import RegionRequest
from aws.osml.model_runner.common import get_credentials_for_assumed_role
from aws.osml.model_runner.database import FeatureTable, RegionRequestTable
from aws.osml.model_runner.inference import FeatureDetectorFactory
from aws.osml.model_runner.tile_worker import TileWorker
from aws.osml.photogrammetry import ElevationModel, SensorModel

from osml_extensions import EnhancedServiceConfig
from osml_extensions.enhanced_tile_worker import EnhancedTileWorker
from osml_extensions.factory import EnhancedFeatureDetectorFactory

logger = logging.getLogger(__name__)


def setup_enhanced_tile_workers(
    region_request: RegionRequest,
    sensor_model: Optional[SensorModel] = None,
    elevation_model: Optional[ElevationModel] = None,
    factory: Optional[FeatureDetectorFactory] = None,
) -> Tuple[Queue, List[TileWorker]]:
    """
    Sets up enhanced tile workers with support for async detectors and enhanced factory.
    
    This function maintains the ability to use AsyncSMDetector and EnhancedFeatureDetectorFactory
    while providing a simplified interface for extension.
    """
    logger.debug("Setting up enhanced tile workers")
    
    # Get model invocation credentials
    model_invocation_credentials = None
    if region_request.model_invocation_role:
        model_invocation_credentials = get_credentials_for_assumed_role(region_request.model_invocation_role)

    # Set up a Queue to manage our tile workers
    tile_queue: Queue = Queue()
    tile_workers = []

    for _ in range(int(EnhancedServiceConfig.workers)):
        worker = _create_tile_worker(
            tile_queue=tile_queue,
            region_request=region_request,
            model_invocation_credentials=model_invocation_credentials,
            sensor_model=sensor_model,
            elevation_model=elevation_model,
            factory=factory
        )
        
        if worker:
            worker.start()
            tile_workers.append(worker)

    logger.info(f"Setup pool of {len(tile_workers)} enhanced tile workers")
    return tile_queue, tile_workers


def _create_tile_worker(
    tile_queue: Queue,
    region_request: RegionRequest,
    model_invocation_credentials: Optional[dict] = None,
    sensor_model: Optional[SensorModel] = None,
    elevation_model: Optional[ElevationModel] = None,
    factory: Optional[FeatureDetectorFactory] = None,
) -> Optional[TileWorker]:
    """
    Create a tile worker with enhanced factory support.
    """
    try:
        # Set up tables
        feature_table = FeatureTable(
            EnhancedServiceConfig.feature_table,
            region_request.tile_size,
            region_request.tile_overlap,
        )
        region_request_table = RegionRequestTable(EnhancedServiceConfig.region_request_table)

        # Create feature detector using enhanced factory if not provided
        if factory is None:
            factory = EnhancedFeatureDetectorFactory(
                endpoint=region_request.model_name,
                endpoint_mode=region_request.model_invoke_mode,
                assumed_credentials=model_invocation_credentials,
            )

        feature_detector = factory.build()
        if feature_detector is None:
            logger.error("Failed to create feature detector")
            return None

        # Set up geolocator
        geolocator = None
        if sensor_model is not None:
            geolocator = Geolocator(ImagedFeaturePropertyAccessor(), sensor_model, elevation_model=elevation_model)

        # Create worker instance
        return EnhancedTileWorker(tile_queue, 
                                feature_detector, 
                                geolocator, 
                                feature_table, 
                                region_request_table)
        
    except Exception as err:
        logger.error(f"Failed to create tile worker: {err}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        return None
