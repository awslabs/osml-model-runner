#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
import time
from dataclasses import dataclass
from typing import List, Optional

from dacite import from_dict

from aws.osml.model_runner.database.ddb_helper import DDBHelper, DDBItem, DDBKey
from aws.osml.model_runner.database.exceptions import (
    GetRegionRequestItemException,
    StartRegionException,
    UpdateRegionException,
    CompleteRegionException,
)

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
    image_path: Optional[str] = None
    region: Optional[str] = None
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
        self.ddb_key = DDBKey(
            hash_key="tile_id",
            hash_value=self.tile_id,
            range_key="job_id",
            range_value=self.job_id,
        )


class TileRequestTable(DDBHelper):
    """
    TileRequestTable is a class meant to help OSML with accessing and interacting with the tile processing jobs we
    track as part of the tile table. It extends the DDBHelper class and provides its own item data class for use when
    working with items from the table.

    Access patterns:
    1. Get tile by id (primary table): Direct lookup using tile_id
    2. Get tiles for job_id (GSI): Query using JobIdIndex GSI

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

    def update_tile_status(self, tile_id: str, job_id: str, status: str, error_message: Optional[str] = None) -> TileRequestItem:
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
            update_attr = {
                ":status": status,
                ":current_time": current_time
            }
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

    def complete_tile_request(self, tile_request_item: TileRequestItem, status: str, error_message: Optional[str] = None) -> TileRequestItem:
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

    def get_tiles_for_job(self, job_id: str, status_filter: Optional[str] = None) -> List[TileRequestItem]:
        """
        Get all tiles for a specific job using the JobIdIndex GSI.

        :param job_id: str = the job identifier
        :param status_filter: Optional[str] = filter by specific status

        :return: List[TileRequestItem] = list of tile request items for the job
        """
        try:
            # Query the GSI using job_id as partition key
            query_kwargs = {
                'IndexName': 'JobIdIndex',
                'KeyConditionExpression': 'job_id = :job_id',
                'ExpressionAttributeValues': {':job_id': job_id}
            }
            
            # Add status filter if provided
            if status_filter:
                query_kwargs['FilterExpression'] = '#status = :status'
                query_kwargs['ExpressionAttributeNames'] = {'#status': 'status'}
                query_kwargs['ExpressionAttributeValues'][':status'] = status_filter

            response = self.table.query(**query_kwargs)
            
            tiles = []
            for item in response.get('Items', []):
                converted_item = self.convert_decimal(item)
                tiles.append(from_dict(TileRequestItem, converted_item))
            
            return tiles
        except Exception as err:
            logger.error(f"Failed to get tiles for job_id={job_id}: {err}")
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