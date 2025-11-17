#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

import logging
import traceback
from queue import Queue
from typing import List, Optional, Tuple

from aws.osml.model_runner.api import TileRequest, ImageRequest
from aws.osml.model_runner.app_config import ServiceConfig
from aws.osml.model_runner.common import get_credentials_for_assumed_role
from aws.osml.model_runner.database import FeatureTable, RegionRequestTable
from aws.osml.model_runner.inference.endpoint_factory import FeatureDetectorFactory
from aws.osml.model_runner.tile_worker.exceptions import SetupTileWorkersException
from aws.osml.photogrammetry import ElevationModel, SensorModel

# from .async_tile_results_worker import AsyncResultsWorker
from .batch_tile_workers import BatchUploadWorker, BatchSubmissionWorker
from .tile_worker import TileWorker

# Set up logging configuration
logger = logging.getLogger(__name__)


def setup_batch_submission_worker(image_request: ImageRequest) -> Tuple[Queue, List[TileWorker]]:

    try:
        model_invocation_credentials = None
        if image_request.model_invocation_role:
            model_invocation_credentials = get_credentials_for_assumed_role(tile_request.model_invocation_role)

        in_queue: Queue = Queue()

        # Ignoring mypy error - if model_name was None the call to validate the region
        # request at the start of this function would have failed
        feature_detector = FeatureDetectorFactory(
            endpoint=image_request.model_name,
            endpoint_mode=image_request.model_invoke_mode,
            assumed_credentials=model_invocation_credentials,
        ).build()

        # Don't create geolocator here - will be created per request in worker
        logger.info(f"Starting the AsyncResultsWorker with {feature_detector=}")
        worker = BatchSubmissionWorker(
            worker_id=0,
            in_queue=in_queue,
            feature_detector=feature_detector,
        )
        logger.info("Created results worker")
        worker.start()
        logger.info("Result worker started")

        return in_queue, worker

    except Exception as err:
        logger.exception(f"Failed to setup tile workers!: {err}")
        raise SetupTileWorkersException("Failed to setup tile workers!") from err


def setup_upload_tile_workers(
    image_request: ImageRequest,
    sensor_model: Optional[SensorModel] = None,
    elevation_model: Optional[ElevationModel] = None,
) -> Tuple[Queue, List[TileWorker]]:
    """
    Sets up a pool of tile-workers to process image tiles from a region request

    :param image_request: ImageRequest = the region request to update.
    :param sensor_model: Optional[SensorModel] = the sensor model for this raster dataset
    :param elevation_model: Optional[ElevationModel] = an elevation model used to fix the elevation of the image coordinate

    :return: Tuple[Queue, List[TileWorker] = a list of tile workers and the queue that manages them
    """
    try:
        model_invocation_credentials = None
        if image_request.model_invocation_role:
            model_invocation_credentials = get_credentials_for_assumed_role(image_request.model_invocation_role)

        # Set up a Queue to manage our tile workers
        in_queue: Queue = Queue()
        tile_workers = []

        for i in range(int(ServiceConfig.async_endpoint_config.submission_workers)):

            # Ignoring mypy error - if model_name was None the call to validate the region
            # request at the start of this function would have failed
            feature_detector = FeatureDetectorFactory(
                endpoint=image_request.model_name,
                endpoint_mode=image_request.model_invoke_mode,
                assumed_credentials=model_invocation_credentials,
            ).build()

            worker = BatchUploadWorker(worker_id=i, 
                in_queue=in_queue, 
                feature_detector=feature_detector
                )

            worker.start()
            tile_workers.append(worker)

        logger.debug(f"Setup pool of {len(tile_workers)} tile workers")

        return in_queue, tile_workers
    except Exception as err:
        logger.exception(f"Failed to setup tile workers!: {err}")
        raise SetupTileWorkersException("Failed to setup tile workers!") from err
