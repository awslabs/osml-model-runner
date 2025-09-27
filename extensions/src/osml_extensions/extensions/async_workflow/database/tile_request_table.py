#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
import time
from dataclasses import dataclass
from typing import List, Optional

from dacite import from_dict

from aws_embedded_metrics.metric_scope import metric_scope
from aws_embedded_metrics.unit import Unit
from aws_embedded_metrics.logger.metrics_logger import MetricsLogger

from aws.osml.model_runner.app_config import MetricLabels
from aws.osml.model_runner.database.ddb_helper import DDBHelper, DDBItem, DDBKey
from aws.osml.model_runner.database.exceptions import (
    GetRegionRequestItemException,
    StartRegionException,
    UpdateRegionException,
    CompleteRegionException,
)
from aws.osml.model_runner.database import RegionRequestTable, JobTable
from aws.osml.model_runner.api import RegionRequest

from ..async_app_config import AsyncServiceConfig
from ..api import TileRequest

logger = logging.getLogger(__name__)


@dataclass
class TileRequestItem(DDBItem):
    """
    TileRequestItem is a dataclass meant to represent a single item in the Tile table.

    The data schema is defined as follows:
    tile_id: str = primary key - unique identifier for the tile
    job_id: str = secondary key - job identifier for tracking
    image_path: Optional[str] = path to the image file
    region: Optional[str] = region identifier
    image_id: Optional[str] = image identifier
    region_id: Optional[str] = region identifier
    ttl: Optional[int] = time to live for the item (expire_time)
    model_name: Optional[str] = name of the model used for processing
    tile_size: Optional[List[int]] = size dimensions of the tile [width, height]
    start_time: Optional[int] = time in epoch milliseconds when tile processing started
    end_time: Optional[int] = time in epoch milliseconds when tile processing ended
    status: Optional[str] = tile processing status - PENDING, PROCESSING, COMPLETED, FAILED
    processing_duration: Optional[int] = time in milliseconds to complete tile processing
    retry_count: Optional[int] = number of times the tile processing has been retried
    error_message: Optional[str] = error message if tile processing failed
    tile_bounds: Optional[List[List[int]]] = pixel bounds that define the tile [[x1, y1], [x2, y2]]
    """

    tile_id: str
    job_id: str
    image_url: str
    image_path: Optional[str] = None
    image_id: Optional[str] = None
    region_id: Optional[str] = None
    ttl: Optional[int] = None
    model_name: Optional[str] = None
    tile_size: Optional[List[int]] = None
    start_time: Optional[int] = None
    end_time: Optional[int] = None
    status: Optional[str] = None
    processing_duration: Optional[int] = None
    retry_count: Optional[int] = None
    error_message: Optional[str] = None
    tile_bounds: Optional[List[List[int]]] = None

    def __post_init__(self):
        self.region = self.tile_bounds

        self.ddb_key = DDBKey(
            hash_key="tile_id",
            hash_value=self.tile_id,
            range_key="job_id",
            range_value=self.job_id,
        )

    @classmethod
    def from_tile_request(cls, tile_request) -> "TileRequestItem":
        """
        Create a TileRequestItem from a TileRequest.

        :param tile_request: TileRequest object to convert
        :return: TileRequestItem instance
        """
        # Convert region bounds from List format to List[List[int]] format for tile_bounds
        tile_bounds = None
        if hasattr(tile_request, "region") and tile_request.region:
            try:
                # Convert region coordinates to integer bounds if they're numeric
                if (
                    isinstance(tile_request.region, list)
                    and len(tile_request.region) == 2
                    and all(isinstance(coord_pair, list) and len(coord_pair) == 2 for coord_pair in tile_request.region)
                ):

                    tile_bounds = [
                        [int(tile_request.region[0][0]), int(tile_request.region[0][1])],
                        [int(tile_request.region[1][0]), int(tile_request.region[1][1])],
                    ]
            except (ValueError, TypeError, IndexError) as e:
                logger.warning(f"Could not convert region bounds to tile_bounds: {e}")
                tile_bounds = None

        return cls(
            tile_id=tile_request.tile_id,
            job_id=tile_request.job_id,
            image_path=str(tile_request.image_path),
            image_url=str(tile_request.image_url),
            image_id=tile_request.image_id,
            region_id=tile_request.region_id,
            tile_bounds=tile_bounds,
            # Set default values for optional fields
            ttl=None,  # Will be set when starting the request
            model_name=None,  # Can be set later if needed
            tile_size=None,  # Can be derived from tile_bounds if needed
            start_time=None,  # Will be set when starting processing
            end_time=None,
            status=None,  # Will be set to PENDING when starting
            processing_duration=None,
            retry_count=None,  # Will be set to 0 when starting
            error_message=None,
        )

    # TODO: SHOULD THIS MOVE TO THE TILE REQUEST TABLE
    def is_region_request_complete(self, tile_request):
        """
        Check if all tiles for a region are done processing.

        :param tile_request: TileRequest to check
        :return: Tuple of (all_done, total_tile_count, failed_tile_count, region_request, region_request_item)
        """
        try:
            # Get all tiles for this job
            tiles = self.tile_request_table.get_tiles_for_region(tile_request.region_id)

            total_tile_count = len(tiles)
            failed_tile_count = 0
            completed_count = 0

            for tile in tiles:
                if tile.status == "COMPLETED":
                    completed_count += 1
                elif tile.status == "FAILED":
                    failed_tile_count += 1

            all_done = (completed_count + failed_tile_count) == total_tile_count

            # Get region request and region request item
            region_request = self.tile_request_table.get_region_request(tile_request.tile_id)
            region_request_item = self.region_request_table.get_region_request(tile_request.region_id)

            return all_done, total_tile_count, failed_tile_count, region_request, region_request_item

        except Exception as e:
            logger.error(f"Error checking if region is done: {e}")
            # Return safe defaults
            return False, 0, 0, None, None

    @metric_scope
    def complete_region_request(self, tile_request: TileRequest, job_table: JobTable, metrics):

        all_done, total_tile_count, failed_tile_count, region_request, region_request_item = self.is_region_request_complete(
            tile_request
        )

        # Update table w/ total tile counts
        region_request_item.total_tiles = total_tile_count
        region_request_item.succeeded_tile_count = total_tile_count - failed_tile_count
        region_request_item.failed_tile_count = failed_tile_count
        region_request_item = self.region_request_table.update_region_request(region_request_item)

        # Update the image request to complete this region
        _ = job_table.complete_region_request(region_request.image_id, bool(failed_tile_count))  # image_request_item

        # Update region request table if that region succeeded
        region_status = self.region_status_monitor.get_status(region_request_item)
        region_request_item = self.region_request_table.complete_region_request(region_request_item, region_status)

        self.region_status_monitor.process_event(region_request_item, region_status, "Completed region processing")

        # Write CloudWatch Metrics to the Logs
        if isinstance(metrics, MetricsLogger):
            metrics.put_metric(MetricLabels.INVOCATIONS, 1, str(Unit.COUNT.value))


class TileRequestTable(DDBHelper):
    """
    TileRequestTable is a class meant to help OSML with accessing and interacting with the tile processing jobs we
    track as part of the tile table. It extends the DDBHelper class and provides its own item data class for use when
    working with items from the table.

    Access patterns:
    1. Get tile by id (primary table): Direct lookup using tile_id
    2. Get tiles for region_id (GSI): Query using RegionIdIndex GSI

    :param table_name: str = the name of the table to interact with

    :return: None
    """

    def __init__(self, table_name: str) -> None:
        super().__init__(table_name)

    def start_tile_request(self, tile_request_item: TileRequestItem) -> TileRequestItem:
        """
        Start a tile processing request, this should be the first record for this tile in the table.

        :param tile_request_item: TileRequestItem = the tile request item to add to ddb

        :return: TileRequestItem = Updated tile request item
        """
        try:
            start_time_millisec = int(time.time() * 1000)

            # Update the tile item to have the correct start parameters
            tile_request_item.start_time = start_time_millisec
            tile_request_item.status = "PENDING"
            tile_request_item.retry_count = 0
            tile_request_item.processing_duration = 0
            # Set TTL to 7 days from now
            tile_request_item.ttl = int((start_time_millisec / 1000) + (7 * 24 * 60 * 60))

            # Put the item into the table
            self.put_ddb_item(tile_request_item)

            return tile_request_item
        except Exception as err:
            raise StartRegionException("Failed to add tile request to the table!") from err

    def update_tile_status(
        self, tile_id: str, job_id: str, status: str, error_message: Optional[str] = None
    ) -> TileRequestItem:
        """
        Update the status of a tile processing request.

        :param tile_id: str = the unique identifier for the tile
        :param job_id: str = the job identifier
        :param status: str = new status (PROCESSING, COMPLETED, FAILED)
        :param error_message: Optional[str] = error message if status is FAILED

        :return: TileRequestItem = Updated tile request item
        """
        try:
            current_time = int(time.time() * 1000)

            # Create a minimal item for the update
            tile_item = TileRequestItem(tile_id=tile_id, job_id=job_id)

            # Build update expression
            update_expr = "SET #status = :status, last_updated_time = :current_time"
            update_attr = {":status": status, ":current_time": current_time}
            expr_attr_names = {"#status": "status"}

            # Add start_time when status changes to PROCESSING
            if status == "PROCESSING":
                update_expr += ", start_time = :start_time"
                update_attr[":start_time"] = current_time

            # Add end_time and processing_duration if completing
            if status in ["COMPLETED", "FAILED"]:
                update_expr += ", end_time = :end_time"
                update_attr[":end_time"] = current_time

                # Calculate processing duration if start_time exists
                existing_item = self.get_tile_request(tile_id, job_id)
                if existing_item and existing_item.start_time:
                    processing_duration = current_time - existing_item.start_time
                    update_expr += ", processing_duration = :processing_duration"
                    update_attr[":processing_duration"] = processing_duration

            # Add error message if provided
            if error_message:
                update_expr += ", error_message = :error_message"
                update_attr[":error_message"] = error_message

            updated_item = self.update_ddb_item(tile_item, update_expr, update_attr, expr_attr_names)

            return from_dict(TileRequestItem, updated_item)
        except Exception as e:
            raise UpdateRegionException("Failed to update tile status!") from e

    def complete_tile_request(
        self, tile_request_item: TileRequestItem, status: str, error_message: Optional[str] = None
    ) -> TileRequestItem:
        """
        Complete a tile processing request with final status.

        :param tile_request_item: TileRequestItem = the tile request item to complete
        :param status: str = final status (COMPLETED, FAILED)
        :param error_message: Optional[str] = error message if status is FAILED

        :return: TileRequestItem = Updated tile request item
        """
        try:
            current_time = int(time.time() * 1000)

            tile_request_item.end_time = current_time
            tile_request_item.status = status

            if tile_request_item.start_time:
                tile_request_item.processing_duration = current_time - tile_request_item.start_time

            if error_message:
                tile_request_item.error_message = error_message

            return from_dict(
                TileRequestItem,
                self.update_ddb_item(tile_request_item),
            )
        except Exception as e:
            raise CompleteRegionException("Failed to complete tile request!") from e

    def get_tile_request(self, tile_id: str, job_id: str) -> Optional[TileRequestItem]:
        """
        Get a TileRequestItem object from the table based on the tile_id and job_id provided.
        This uses the primary table for optimal performance.

        :param tile_id: str = the unique identifier for the tile
        :param job_id: str = the job identifier

        :return: Optional[TileRequestItem] = tile request item
        """
        try:
            return from_dict(
                TileRequestItem,
                self.get_ddb_item(TileRequestItem(tile_id=tile_id, job_id=job_id)),
            )
        except Exception as err:
            logger.warning(GetRegionRequestItemException(f"Failed to get TileRequestItem! {err}"))
            return None

    def is_tile_done(self, tile_id: str, job_id: str) -> bool:
        """
        Check if a tile is done processing (COMPLETED or FAILED status).
        Optimized for the primary access pattern "is tile with id done?".

        :param tile_id: str = the unique identifier for the tile
        :param job_id: str = the job identifier

        :return: bool = True if tile is done (COMPLETED or FAILED), False otherwise
        """
        try:
            tile_item = self.get_tile_request(tile_id, job_id)
            if tile_item and tile_item.status:
                return tile_item.status in ["COMPLETED", "FAILED"]
            return False
        except Exception as err:
            logger.warning(f"Failed to check tile status for tile_id={tile_id}: {err}")
            return False

    def get_tiles_for_region(self, region_id: str, status_filter: Optional[str] = None) -> List[TileRequestItem]:
        """
        Get all tiles for a specific job using the RegionIdIndex GSI.

        :param regin_id: str = the job identifier
        :param status_filter: Optional[str] = filter by specific status

        :return: List[TileRequestItem] = list of tile request items for the job
        """
        try:
            # Query the GSI using job_id as partition key
            query_kwargs = {
                "IndexName": "RegionIdIndex",
                "KeyConditionExpression": "region_id = :region_id",
                "ExpressionAttributeValues": {":region_id": region_id},
            }

            # Add status filter if provided
            if status_filter:
                query_kwargs["FilterExpression"] = "#status = :status"
                query_kwargs["ExpressionAttributeNames"] = {"#status": "status"}
                query_kwargs["ExpressionAttributeValues"][":status"] = status_filter

            response = self.table.query(**query_kwargs)

            tiles = []
            for item in response.get("Items", []):
                converted_item = self.convert_decimal(item)
                tiles.append(from_dict(TileRequestItem, converted_item))

            return tiles
        except Exception as err:
            logger.error(f"Failed to get tiles for region_id={region_id}: {err}")
            return []

    def increment_retry_count(self, tile_id: str, job_id: str) -> TileRequestItem:
        """
        Increment the retry count for a tile processing request.

        :param tile_id: str = the unique identifier for the tile
        :param job_id: str = the job identifier

        :return: TileRequestItem = Updated tile request item
        """
        try:
            tile_item = TileRequestItem(tile_id=tile_id, job_id=job_id)

            update_expr = "ADD retry_count :increment"
            update_attr = {":increment": 1}

            updated_item = self.update_ddb_item(tile_item, update_expr, update_attr)

            return from_dict(TileRequestItem, updated_item)
        except Exception as e:
            raise UpdateRegionException("Failed to increment retry count!") from e

    def get_region_request(self, tile_id: str, region_request_table=None):
        """
        Get the region request associated with a tile ID by querying the RegionRequestTable.

        :param tile_id: str = the unique identifier for the tile
        :param region_request_table: Optional RegionRequestTable instance to use for lookup
        :return: RegionRequest object or None
        """
        try:
            # Since we don't have job_id, we need to scan the table to find the tile
            # This is less efficient but necessary for this lookup pattern
            response = self.table.scan(
                FilterExpression="tile_id = :tile_id",
                ExpressionAttributeValues={":tile_id": tile_id},
                Limit=1,  # We only need one result
            )

            items = response.get("Items", [])
            if not items:
                logger.warning(f"No tile request found for tile_id: {tile_id}")
                return None

            # Convert the first item to TileRequestItem
            tile_item_data = self.convert_decimal(items[0])
            tile_item = from_dict(TileRequestItem, tile_item_data)

            # Get the region_id from the tile item
            region_id = tile_item.region_id
            if not region_id:
                logger.warning(f"No region_id found in tile item for tile_id: {tile_id}")
                return None

            # Use RegionRequestTable to get the actual region request
            if region_request_table is None:
                config = AsyncServiceConfig()
                region_request_table = RegionRequestTable(config.region_request_table)

            # Get the region request from the table
            region_request_item = region_request_table.get_region_request(region_id)

            if not region_request_item:
                logger.warning(f"No region request found for region_id: {region_id}")
                return None

            # Convert RegionRequestItem to RegionRequest
            region_request = RegionRequest()
            region_request.region_id = region_request_item.region_id
            region_request.image_id = region_request_item.image_id
            region_request.job_id = region_request_item.job_id
            region_request.image_url = region_request_item.image_url
            region_request.image_read_role = region_request_item.image_read_role
            region_request.model_name = region_request_item.model_name
            region_request.model_invoke_mode = region_request_item.model_invoke_mode
            region_request.model_invocation_role = region_request_item.model_invocation_role
            region_request.tile_size = region_request_item.tile_size
            region_request.tile_overlap = region_request_item.tile_overlap
            region_request.tile_format = region_request_item.tile_format
            region_request.tile_compression = region_request_item.tile_compression
            region_request.region_bounds = region_request_item.region_bounds

            return region_request

        except Exception as err:
            logger.error(f"Failed to get region request for tile_id {tile_id}: {err}")
            return None
