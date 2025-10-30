#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
import time
import traceback
from dataclasses import dataclass
from typing import List, Optional

from dacite import from_dict

from aws.osml.model_runner.database.ddb_helper import DDBHelper, DDBItem, DDBKey
from aws.osml.model_runner.database.exceptions import (
    CompleteRegionException,
    GetRegionRequestItemException,
    StartRegionException,
    UpdateRegionException,
)
from aws.osml.model_runner.utilities import S3Manager

logger = logging.getLogger(__name__)

S3_MANAGER = S3Manager()


@dataclass
class TileRequestItem(DDBItem):
    """
    TileRequestItem is a dataclass representing a single tile processing request in the DynamoDB table.

    DynamoDB Schema:
    - Primary Key: region_id (hash key) + tile_id (range key)
    - GSI: OutputLocationIndex on output_location (projects region_id, tile_id only)
    - TTL: expire_time (set to 7 days from creation)

    Attributes:
        tile_id (str): Unique identifier for the tile (range key)
        region_id (str): Region identifier (hash key)
        job_id (Optional[str]): Job identifier for tracking
        image_url (Optional[str]): URL to the image file
        image_path (Optional[str]): Path to the image file
        image_id (Optional[str]): Image identifier
        expire_time (Optional[int]): TTL timestamp (7 days from creation)
        start_time (Optional[int]): Processing start time in epoch milliseconds
        end_time (Optional[int]): Processing end time in epoch milliseconds
        tile_status (Optional[str]): Processing status - PENDING, PROCESSING, COMPLETED, FAILED
        processing_duration (Optional[int]): Processing time in milliseconds (end_time - start_time)
        retry_count (Optional[int]): Number of processing retries (initialized to 0)
        error_message (Optional[str]): Error message if processing failed
        tile_bounds (Optional[List[List[int]]]): Pixel bounds [[x1, y1], [x2, y2]] (converted to tuple)
        inference_id (Optional[str]): SageMaker async inference job ID
        output_location (Optional[str]): S3 output location for results
        failure_location (Optional[str]): S3 output location for failure results
        model_invocation_role (str): IAM role for model invocation
        tile_size (Optional[List[int]]): Tile dimensions [width, height]
        tile_overlap (Optional[List[int]]): Tile overlap dimensions
        model_invoke_mode (Optional[str]): Model invocation mode
        model_name (str): Name of the model used for processing
        image_read_role (Optional[str]): IAM role for reading images

    Note:
        - tile_bounds is converted to tuple of tuples in __post_init__ for DynamoDB compatibility
        - region attribute is set to tile_bounds for backward compatibility
    """

    tile_id: str
    region_id: str
    job_id: Optional[str] = None
    image_url: Optional[str] = None
    image_path: Optional[str] = None
    image_id: Optional[str] = None
    expire_time: Optional[int] = None
    start_time: Optional[int] = None
    end_time: Optional[int] = None
    tile_status: Optional[str] = None
    processing_duration: Optional[int] = None
    retry_count: Optional[int] = None
    error_message: Optional[str] = None
    tile_bounds: Optional[List[List[int]]] = None
    inference_id: Optional[str] = None  # SageMaker async inference ID
    output_location: Optional[str] = None  # S3 output location for results
    failure_location: Optional[str] = ""
    model_invocation_role: str = ""
    tile_size: Optional[List[int]] = None
    tile_overlap: Optional[List[int]] = None
    model_invoke_mode: Optional[str] = None
    model_name: str = ""
    image_read_role: Optional[str] = None

    def __post_init__(self):
        # needs to be a tuple of tuples for the add_tile operation
        if self.tile_bounds is not None:
            self.tile_bounds = tuple([tuple(x) for x in self.tile_bounds])
        self.region = self.tile_bounds

        self.ddb_key = DDBKey(
            hash_key="region_id",
            hash_value=self.region_id,
            range_key="tile_id",
            range_value=self.tile_id,
        )

    @classmethod
    def from_tile_request(cls, tile_request) -> "TileRequestItem":
        """
        Create a TileRequestItem from a TileRequest.

        :param tile_request: TileRequest object to convert
        :return: TileRequestItem instance
        """
        return cls(
            tile_id=tile_request.tile_id,
            job_id=tile_request.job_id,
            image_path=str(tile_request.image_path),
            image_url=str(tile_request.image_url),
            image_id=tile_request.image_id,
            region_id=tile_request.region_id,
            tile_bounds=tile_request.tile_bounds,
            inference_id=tile_request.inference_id or "UNKNOWN",
            output_location=tile_request.output_location or "UNKNOWN",
            failure_location=tile_request.failure_location or "",
            expire_time=None,  # Will be set when starting the request
            start_time=None,  # Will be set when starting processing
            end_time=None,
            tile_status=None,  # Will be set to PENDING when starting
            processing_duration=None,
            retry_count=None,  # Will be set to 0 when starting
            error_message=None,
            model_invocation_role=tile_request.model_invocation_role,
            tile_size=tile_request.tile_size,
            tile_overlap=tile_request.tile_overlap,
            model_invoke_mode=tile_request.model_invoke_mode,
            model_name=tile_request.model_name,
            image_read_role=tile_request.image_read_role,
        )


class TileRequestTable(DDBHelper):
    """
    TileRequestTable is a class meant to help OSML with accessing and interacting with the tile processing jobs we
    track as part of the tile table. It extends the DDBHelper class and provides its own item data class for use when
    working with items from the table.

    Access patterns:
    1. Get tile by region_id + tile_id (primary table): Direct lookup using region_id + tile_id
    2. Get tiles for region_id (primary table): Query using region_id
    3. Get tile by output_location (GSI): Query using OutputLocationIndex GSI (projects region_id, tile_id only)

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
            tile_request_item.tile_status = "PENDING"
            tile_request_item.retry_count = 0
            tile_request_item.processing_duration = 0
            # Set TTL to 7 days from now
            tile_request_item.expire_time = int((start_time_millisec / 1000) + (7 * 24 * 60 * 60))

            # Put the item into the table
            self.put_ddb_item(tile_request_item)

            return tile_request_item
        except Exception as err:
            raise StartRegionException("Failed to add tile request to the table!") from err

    def update_tile_status(
        self, tile_id: str, region_id: str, tile_status: str, error_message: Optional[str] = None
    ) -> TileRequestItem:
        """
        Update the tile_status of a tile processing request.

        :param tile_id: str = the unique identifier for the tile
        :param region_id: str = the region identifier
        :param tile_status: str = new status (PROCESSING, COMPLETED, FAILED)
        :param error_message: Optional[str] = error message if tile_status is FAILED

        :return: TileRequestItem = Updated tile request item
        """
        try:
            current_time = int(time.time() * 1000)

            # Create item with correct primary key (region_id + tile_id)
            tile_item = TileRequestItem(tile_id=tile_id, region_id=region_id)

            # Build update expression
            update_expr = "SET #tile_status = :tile_status, last_updated_time = :current_time"
            update_attr = {":tile_status": tile_status, ":current_time": current_time}
            expr_attr_names = {"#tile_status": "tile_status"}

            # Add start_time when tile_status changes to PROCESSING
            # TODO: Update these to use RequestStatus
            if tile_status == "PROCESSING":
                update_expr += ", start_time = :start_time"
                update_attr[":start_time"] = current_time

            # Add end_time and processing_duration if completing
            if tile_status in ["COMPLETED", "FAILED"]:
                update_expr += ", end_time = :end_time"
                update_attr[":end_time"] = current_time

                # Calculate processing duration if start_time exists
                existing_item = self.get_tile_request(tile_id, region_id)
                if existing_item and existing_item.start_time:
                    processing_duration = current_time - existing_item.start_time
                    update_expr += ", processing_duration = :processing_duration"
                    update_attr[":processing_duration"] = processing_duration

            # Add error message if provided
            if error_message:
                update_expr += ", error_message = :error_message"
                update_attr[":error_message"] = error_message

            # Use direct table.update_item call since we need ExpressionAttributeNames
            response = self.table.update_item(
                Key=self.get_keys(ddb_item=tile_item),
                UpdateExpression=update_expr,
                ExpressionAttributeValues=update_attr,
                ExpressionAttributeNames=expr_attr_names,
                ReturnValues="ALL_NEW",
            )
            updated_item = self.convert_decimal(response["Attributes"])

            return from_dict(TileRequestItem, updated_item)
        except Exception as e:
            logger.error(traceback.format_exc())
            raise UpdateRegionException("Failed to update tile_status!") from e

    def complete_tile_request(
        self, tile_request_item: TileRequestItem, tile_status: str, error_message: Optional[str] = None
    ) -> TileRequestItem:
        """
        Complete a tile processing request with final tile_status.

        :param tile_request_item: TileRequestItem = the tile request item to complete
        :param tile_status: str = final tile_status (COMPLETED, FAILED)
        :param error_message: Optional[str] = error message if tile_status is FAILED

        :return: TileRequestItem = Updated tile request item
        """
        try:
            current_time = int(time.time() * 1000)

            tile_request_item.end_time = current_time
            tile_request_item.tile_status = tile_status

            if tile_request_item.start_time:
                tile_request_item.processing_duration = current_time - tile_request_item.start_time

            if error_message:
                tile_request_item.error_message = error_message

            # Ensure the item has the correct primary key structure
            if not tile_request_item.output_location:
                tile_request_item.output_location = ""
            else:  # Output location available. Do cleanup.
                S3_MANAGER.delete_object(tile_request_item.output_location)

            return from_dict(
                TileRequestItem,
                self.update_ddb_item(tile_request_item),
            )
        except Exception as e:
            logger.error(traceback.format_exc())
            raise CompleteRegionException("Failed to complete tile request!") from e

    def get_tile_request(self, tile_id: str, region_id: str) -> Optional[TileRequestItem]:
        """
        Get a TileRequestItem object from the table based on the tile_id and region_id provided.
        This uses the primary table for optimal performance.

        :param tile_id: str = the unique identifier for the tile
        :param region_id: str = the region identifier

        :return: Optional[TileRequestItem] = tile request item
        """
        try:
            return from_dict(
                TileRequestItem,
                self.get_ddb_item(TileRequestItem(tile_id=tile_id, region_id=region_id)),
            )
        except Exception as err:
            logger.warning(GetRegionRequestItemException(f"Failed to get TileRequestItem! {err}"))
            return None

    def is_tile_item_done(self, tile_item: TileRequestItem):
        if tile_item and tile_item.tile_status:
            return tile_item.tile_status in ["COMPLETED", "FAILED"]
        return False

    def is_tile_done(self, tile_id: str, region_id: str) -> bool:
        """
        NOT USED
        Check if a tile is done processing (COMPLETED or FAILED status).
        Optimized for the primary access pattern "is tile with id done?".

        :param tile_id: str = the unique identifier for the tile
        :param region_id: str = the region identifier

        :return: bool = True if tile is done (COMPLETED or FAILED), False otherwise
        """
        try:
            tile_item = self.get_tile_request(tile_id, region_id)
            return self.is_tile_item_done(tile_item)
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
            # Query the table using job_id as partition key
            query_kwargs = {
                "KeyConditionExpression": "region_id = :region_id",
                "ExpressionAttributeValues": {":region_id": region_id},
            }

            # Add status filter if provided
            if status_filter:
                query_kwargs["FilterExpression"] = "#tile_status = :tile_status"
                query_kwargs["ExpressionAttributeNames"] = {"#tile_status": "tile_status"}
                query_kwargs["ExpressionAttributeValues"][":tile_status"] = status_filter

            response = self.table.query(**query_kwargs)

            tiles = []
            for item in response.get("Items", []):
                converted_item = self.convert_decimal(item)
                tiles.append(from_dict(TileRequestItem, converted_item))

            return tiles
        except Exception as err:
            logger.error(f"Failed to get tiles for region_id={region_id}: {err}")
            return []

    def increment_retry_count(self, tile_id: str, region_id: str) -> TileRequestItem:
        """
        NOT USED
        Increment the retry count for a tile processing request.

        :param tile_id: str = the unique identifier for the tile
        :param region_id: str = the region identifier

        :return: TileRequestItem = Updated tile request item
        """
        try:
            tile_item = TileRequestItem(tile_id=tile_id, region_id=region_id)

            update_expr = "ADD retry_count :increment"
            update_attr = {":increment": 1}

            updated_item = self.update_ddb_item(tile_item, update_expr, update_attr)

            return from_dict(TileRequestItem, updated_item)
        except Exception as e:
            raise UpdateRegionException("Failed to increment retry count!") from e

    def get_tile_request_by_inference_id(self, inference_id: str) -> Optional[TileRequestItem]:
        """
        Get a TileRequestItem by its output location using the InferenceIdIndex GSI.
        Note: This GSI only projects region_id and tile_id, so a second query is needed for full item.

        :param inference_id: str = the inference id from SageMaker to search for

        :return: Optional[TileRequestItem] = tile request item if found
        """
        try:
            # Query the InferenceIdIndex GSI using inference_id as partition key
            response = self.table.query(
                IndexName="InferenceIdIndex",
                KeyConditionExpression="inference_id = :inference_id",
                ExpressionAttributeValues={":inference_id": inference_id},
                Limit=1,  # We only need one result
            )

            items = response.get("Items", [])
            if not items:
                logger.warning(f"No tile request found for inference_id: {inference_id}")
                return None

            # The GSI only projects region_id and tile_id, so we need to get the full item
            gsi_item = self.convert_decimal(items[0])
            region_id = gsi_item.get("region_id")
            tile_id = gsi_item.get("tile_id")

            if not region_id or not tile_id:
                logger.error(f"Missing region_id or tile_id in GSI result for inference_id: {inference_id}")
                return None

            # Get the full item from the primary table
            return self.get_tile_request(tile_id, region_id)

        except Exception as err:
            logger.error(f"Failed to get tile request by inference_id {inference_id}: {err}")
            return None

    def get_tile_request_by_output_location(self, output_location: str) -> Optional[TileRequestItem]:
        """
        Get a TileRequestItem by its output location using the OutputLocationIndex GSI.
        Note: This GSI only projects region_id and tile_id, so a second query is needed for full item.

        :param output_location: str = the S3 output location to search for

        :return: Optional[TileRequestItem] = tile request item if found
        """
        try:
            # Query the OutputLocationIndex GSI using output_location as partition key
            response = self.table.query(
                IndexName="OutputLocationIndex",
                KeyConditionExpression="output_location = :output_location",
                ExpressionAttributeValues={":output_location": output_location},
                Limit=1,  # We only need one result
            )

            items = response.get("Items", [])
            if not items:
                logger.warning(f"No tile request found for output_location: {output_location}")
                return None

            # The GSI only projects region_id and tile_id, so we need to get the full item
            gsi_item = self.convert_decimal(items[0])
            region_id = gsi_item.get("region_id")
            tile_id = gsi_item.get("tile_id")

            if not region_id or not tile_id:
                logger.error(f"Missing region_id or tile_id in GSI result for output_location: {output_location}")
                return None

            # Get the full item from the primary table
            return self.get_tile_request(tile_id, region_id)

        except Exception as err:
            logger.error(f"Failed to get tile request by output_location {output_location}: {err}")
            return None

    def update_tile_inference_info(
        self, tile_id: str, region_id: str, inference_id: str, output_location: str, failure_location: str
    ) -> TileRequestItem:
        """
        Update the inference_id and output_location for a tile processing request.

        :param tile_id: str = the unique identifier for the tile
        :param region_id: str = the region identifier
        :param inference_id: str = SageMaker async inference ID
        :param output_location: str = S3 output location for results
        :param failure_location: str = S3 output failure location for results

        :return: TileRequestItem = Updated tile request item
        """
        try:
            current_time = int(time.time() * 1000)

            # Create item with correct primary key (region_id + tile_id)
            tile_item = TileRequestItem(tile_id=tile_id, region_id=region_id)

            # Build update expression
            update_expr = (
                "SET inference_id = :inference_id, output_location = :output_location, failure_location = :failure_location, last_updated_time = :current_time"
            )
            update_attr = {
                ":inference_id": inference_id,
                ":output_location": output_location,
                ":failure_location": failure_location,
                ":current_time": current_time,
            }

            # Use direct table.update_item call
            response = self.table.update_item(
                Key=self.get_keys(ddb_item=tile_item),
                UpdateExpression=update_expr,
                ExpressionAttributeValues=update_attr,
                ReturnValues="ALL_NEW",
            )
            updated_item = self.convert_decimal(response["Attributes"])

            return from_dict(TileRequestItem, updated_item)
        except Exception as e:
            logger.error(traceback.format_exc())
            raise UpdateRegionException("Failed to update tile inference info!") from e

    def get_region_request_complete_counts(self, tile_request_item: TileRequestItem):
        """
        Check if all tiles for a region are done processing.

        :param tile_request_item: TileRequestItem to check
        :return: Tuple of (failed_tile_count, complete_tile_count)
        """
        try:
            # Get all tiles for this job
            tiles = self.get_tiles_for_region(tile_request_item.region_id)

            failed_tile_count = 0
            completed_count = 0

            for tile in tiles:
                if tile.tile_status == "COMPLETED":
                    completed_count += 1
                elif tile.tile_status == "FAILED":
                    failed_tile_count += 1

            return failed_tile_count, completed_count

        except Exception as e:
            logger.error(f"Error checking if region is done: {e}")
            # Return safe defaults
            return False, 0, 0, None, None
