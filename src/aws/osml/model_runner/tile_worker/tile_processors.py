#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
import tempfile
from pathlib import Path
from queue import Queue
from secrets import token_hex
from typing import List, Optional, Tuple

from osgeo import gdal

from aws.osml.gdal import GDALConfigEnv
from aws.osml.image_processing.gdal_tile_factory import GDALTileFactory
from aws.osml.model_runner.api import RegionRequest, TileRequest
from aws.osml.model_runner.app_config import ServiceConfig
from aws.osml.model_runner.common import (
    ImageRegion,
    get_credentials_for_assumed_role,
    RequestStatus
)
from aws.osml.model_runner.database import RegionRequestItem
from aws.osml.model_runner.queue import RequestQueue
from aws.osml.photogrammetry import SensorModel

from .exceptions import ProcessTilesException
from .tile_worker import TileWorker
from .tiling_strategy import TilingStrategy

logger = logging.getLogger(__name__)


class TileProcessor:
    def __init__(self):
        self.shutdown_workers = True
        self.tile_request_queue = RequestQueue(ServiceConfig.tile_queue, wait_seconds=0)

    def handle_tile(
        self,
        tile_queue,
        region_request: RegionRequest,
        region_request_item: RegionRequestItem,
        tmp_image_path: Path,
        tile_bounds,
    ):

        # Put the image info on the tile worker queue allowing each tile to be
        # processed in parallel.
        image_info = {
            "image_path": tmp_image_path,
            "region": tile_bounds,
            "image_id": region_request_item.image_id,
            "job_id": region_request_item.job_id,
            "region_id": region_request_item.region_id,
        }

        # Place the image info onto our processing queue
        tile_queue.put(image_info)

    def get_tile_array(
        self,
        tiling_strategy: TilingStrategy,
        region_request_item: RegionRequestItem,
    ):

        # Grab completed tiles from region item
        # Explicitly cast to Tuple[Tuple[int, int], Tuple[int, int]]
        # Ensure the bounds have exactly two integers before converting
        region_bounds: Tuple[Tuple[int, int], Tuple[int, int]] = (
            (region_request_item.region_bounds[0][0], region_request_item.region_bounds[0][1]),
            (region_request_item.region_bounds[1][0], region_request_item.region_bounds[1][1]),
        )

        # Explicitly cast tile_size to Tuple[int, int]
        tile_size: Tuple[int, int] = (region_request_item.tile_size[0], region_request_item.tile_size[1])

        # Explicitly cast tile_overlap to Tuple[int, int]
        tile_overlap: Tuple[int, int] = (region_request_item.tile_overlap[0], region_request_item.tile_overlap[1])

        tile_array = tiling_strategy.compute_tiles(region_bounds, tile_size, tile_overlap)

        if region_request_item.succeeded_tiles is not None:
            # Filter ImageRegions based on matching in succeeded_tiles
            filtered_regions = [
                region
                for region in tile_array
                if [[region[0][0], region[0][1]], [region[1][0], region[1][1]]] not in region_request_item.succeeded_tiles
            ]
            if len(tile_array) != len(tile_array):
                logger.debug(f"{len(tile_array) - len(tile_array)} tiles have already been processed!")

            tile_array = filtered_regions

        return tile_array

    def process_tiles(
        self,
        tiling_strategy: TilingStrategy,
        region_request: RegionRequest,
        region_request_item: RegionRequestItem,
        tile_queue: Queue,
        tile_workers: List[TileWorker],
        raster_dataset: gdal.Dataset,
        sensor_model: Optional[SensorModel] = None,
    ) -> Tuple[int, int]:
        """
        Loads a GDAL dataset into memory and processes it with a pool of tile workers.

        :param tiling_strategy: the approach used to decompose the region into tiles for the ML model
        :param region_request_item: RegionRequestItem = the region request to update.
        :param tile_queue: Queue = keeps the image in the queue for processing
        :param tile_workers: List[TileWorker] = the list of tile workers
        :param raster_dataset: gdal.Dataset = the raster dataset containing the region
        :param sensor_model: Optional[SensorModel] = the sensor model for this raster dataset

        :return: Tuple[int, int, List[ImageRegion]] = number of tiles processed, number of tiles with an error
        """

        tile_array = self.get_tile_array(tiling_strategy, region_request_item)
        total_tile_count = len(tile_array)
        try:
            # This will update the GDAL configuration options to use the security credentials for
            # this request. Any GDAL managed AWS calls (i.e. incrementally fetching pixels from a
            # dataset stored in S3) within this "with" statement will be made using customer
            # credentials. At the end of the "with" scope the credentials will be removed.
            image_read_credentials = None
            if region_request_item.image_read_role:
                image_read_credentials = get_credentials_for_assumed_role(region_request_item.image_read_role)

            with GDALConfigEnv().with_aws_credentials(image_read_credentials):
                # Use the request and metadata from the raster dataset to create a set of keyword
                # arguments for the gdal.Translate() function. This will configure that function to
                # create image tiles using the format, compression, etc. needed by the CV container.
                gdal_tile_factory = GDALTileFactory(
                    raster_dataset=raster_dataset,
                    tile_format=region_request_item.tile_format,
                    tile_compression=region_request_item.tile_compression,
                    sensor_model=sensor_model,
                )

                # Calculate a set of ML engine sized regions that we need to process for this image
                # and set up a temporary directory to store the temporary files. 
                tmp = tempfile.gettempdir()

                # Ignoring mypy error - if region_bounds was None the call to validate the
                # image region request at the start of this function would have failed
                for tile_bounds in tile_array:
                    # Create a temp file name for the encoded region
                    region_image_filename = (
                        f"{token_hex(16)}-region-{tile_bounds[0][0]}-{tile_bounds[0][1]}-"
                        f"{tile_bounds[1][0]}-{tile_bounds[1][1]}.{region_request_item.tile_format}"
                    )

                    # Set a path for the tmp image
                    tmp_image_path = Path(tmp, region_image_filename)

                    # Generate an encoded tile of the requested image region
                    absolute_tile_path = _create_tile(gdal_tile_factory, tile_bounds, tmp_image_path)
                    
                    if not absolute_tile_path:
                        continue

                    self.handle_tile(tile_queue, region_request, region_request_item, tmp_image_path, tile_bounds)

            tile_error_count = self.shut_down_workers(tile_workers, tile_queue)
            
            logger.debug(
                (
                    f"Model Runner Stats Processed {total_tile_count} image tiles for "
                    f"region {region_request_item.region_bounds}. {tile_error_count} tiles failed to process."
                )
            )
        except Exception as err:
            logger.exception(f"File processing tiles: {err}")
            raise ProcessTilesException("Failed to process tiles!") from err

        return total_tile_count, tile_error_count

    def shut_down_workers(self, tile_workers, tile_queue):

        tile_error_count = 0
        if self.shutdown_workers:
            # Put enough empty messages on the queue to shut down the workers
            for i in range(len(tile_workers)):
                tile_queue.put(None)

            # Ensure the wait for tile workers happens within the context where we create
            # the temp directory. If the context is exited before all workers return then
            # the directory will be deleted, and we will potentially lose tiles.
            # Wait for all the workers to finish gracefully before we clean up the temp directory
            for worker in tile_workers:
                worker.join()
                tile_error_count += worker.failed_tile_count

        return tile_error_count


class AsyncTileProcessor(TileProcessor):
    def __init__(self, tile_request_table):
        super().__init__()
        self.tiles_submitted = 0
        self.tile_request_table = tile_request_table

    def handle_tile(
        self,
        tile_queue,
        region_request: RegionRequest,
        region_request_item: RegionRequestItem,
        tmp_image_path: Path,
        tile_bounds,
    ):

        tile_id = f"{region_request_item.region_id}-{tile_bounds[0][0]}-{tile_bounds[0][1]}"

        # Create image info
        tile_request = TileRequest(
            tile_id=tile_id,
            region_id=region_request_item.region_id,
            image_id=region_request_item.image_id,
            job_id=region_request_item.job_id,
            image_path=str(tmp_image_path.absolute()),
            image_url=region_request.image_url,
            tile_bounds=tile_bounds,
            model_invocation_role=region_request.model_invocation_role,
            tile_size=region_request.tile_size,
            tile_overlap=region_request.tile_overlap,
            model_invoke_mode=region_request.model_invoke_mode,
            model_name=region_request.model_name,
            image_read_role=region_request.image_read_role,
        )

        # tile_item = self.tile_request_table.get_tile_request(tile_id, tile_request.region_id)

        # Add tile to tracking database
        tile_request_item = self.tile_request_table.get_or_create_tile_request_item(tile_request)
        
        # Check if tile is already done
        if tile_request_item.tile_status == RequestStatus.SUCCESS:
            return

        # submit tile to submission worker
        tile_queue.put(tile_request.__dict__)
        self.tiles_submitted += 1

        if ServiceConfig.use_tile_poller:
            # remind me in 1 minute - Insurance in case notifications are not received from S3/SageMaker
            message = dict(
                PollerInfo=dict(
                    tile_id=tile_request.tile_id, 
                    region_id=tile_request.region_id
                    )
                )
            self.tile_request_queue.send_request(message, delay_seconds=ServiceConfig.tile_poller_delay)


class BatchTileProcessor(AsyncTileProcessor):
    """
    Currently a wrapper for AyncTileProcessor. 
    Meant to be extended if functionality needs to diverge.
    """
    def __init__(self, tile_request_table):
        super().__init__(tile_request_table)
        self.shutdown_workers = False # delay shutdown until all regions completed

