import logging
from typing import Optional, Tuple, List
from queue import Queue
import traceback

from aws.osml.model_runner.tile_worker.exceptions import SetupTileWorkersException
from aws.osml.model_runner.database import FeatureTable, RegionRequestTable
from aws.osml.model_runner.api import RegionRequest
from aws.osml.photogrammetry import ElevationModel, SensorModel
from aws.osml.model_runner.tile_worker import TileWorker
from aws.osml.model_runner.common import get_credentials_for_assumed_role

from .async_tile_submission_worker import AsyncSubmissionWorker
from .async_tile_results_worker import AsyncResultsWorker
from ..async_app_config import AsyncServiceConfig
from ..factory import EnhancedFeatureDetectorFactory
from ..api import TileRequest

# Set up logging configuration
logger = logging.getLogger(__name__)


def setup_result_tile_workers(
    tile_request: TileRequest,
    sensor_model: Optional[SensorModel] = None,  # Keep for backward compatibility but ignore
    elevation_model: Optional[ElevationModel] = None,  # Keep for backward compatibility but ignore
    completion_queue: Optional[Queue] = None,
) -> Tuple[Queue, List[TileWorker]]:

    try:
        model_invocation_credentials = None
        if tile_request.model_invocation_role:
            model_invocation_credentials = get_credentials_for_assumed_role(tile_request.model_invocation_role)

        tile_queue: Queue = Queue()
        tile_workers = []

        # Start polling workers
        for i in range(1):  # AsyncServiceConfig.polling_workers):

            # Set up our feature table to work with the region quest
            feature_table = FeatureTable(
                AsyncServiceConfig.feature_table,
                tile_request.tile_size,
                tile_request.tile_overlap,
            )

            # Set up our feature table to work with the region quest
            region_request_table = RegionRequestTable(AsyncServiceConfig.region_request_table)

            # Ignoring mypy error - if model_name was None the call to validate the region
            # request at the start of this function would have failed
            feature_detector = EnhancedFeatureDetectorFactory(
                endpoint=tile_request.model_name,
                endpoint_mode=tile_request.model_invoke_mode,
                assumed_credentials=model_invocation_credentials,
            ).build()

            if feature_detector is None:
                logger.error("Failed to create feature detector")
                logger.error(f"Exception details: {traceback.format_exc()}")
                return None

            # Don't create geolocator here - will be created per request in worker
            logger.info(f"Starting the AsyncResultsWorker with {feature_detector=}")
            worker = AsyncResultsWorker(
                worker_id=i,
                feature_table=feature_table,
                geolocator=None,  # No geolocator at initialization
                region_request_table=region_request_table,
                in_queue=tile_queue,
                feature_detector=feature_detector,
                completion_queue=completion_queue,
            )
            logger.info("Created results worker")
            worker.start()
            logger.info("Result worker started")
            tile_workers.append(worker)

        return tile_queue, tile_workers

    except Exception as err:
        logger.exception(f"Failed to setup tile workers!: {err}")
        raise SetupTileWorkersException("Failed to setup tile workers!") from err


def setup_submission_tile_workers(
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

        for i in range(int(AsyncServiceConfig.workers)):

            # Ignoring mypy error - if model_name was None the call to validate the region
            # request at the start of this function would have failed
            feature_detector = EnhancedFeatureDetectorFactory(
                endpoint=region_request.model_name,
                endpoint_mode=region_request.model_invoke_mode,
                assumed_credentials=model_invocation_credentials,
            ).build()

            worker = AsyncSubmissionWorker(worker_id=i, tile_queue=tile_queue, feature_detector=feature_detector)

            worker.start()
            tile_workers.append(worker)

        logger.debug(f"Setup pool of {len(tile_workers)} tile workers")

        return tile_queue, tile_workers
    except Exception as err:
        logger.exception(f"Failed to setup tile workers!: {err}")
        raise SetupTileWorkersException("Failed to setup tile workers!") from err
