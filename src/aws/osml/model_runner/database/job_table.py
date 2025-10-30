#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
import time
from dataclasses import dataclass
from json import dumps
from typing import List, Optional

from dacite import from_dict

from aws.osml.model_runner.api import ImageRequest

from .ddb_helper import DDBHelper, DDBItem, DDBKey
from .exceptions import (
    CompleteRegionException,
    EndImageException,
    GetImageRequestItemException,
    IsImageCompleteException,
    StartImageException,
)
from .region_request_table import RegionRequestItem

logger = logging.getLogger(__name__)


@dataclass
class JobItem(DDBItem):
    """
    JobItem is a dataclass meant to represent a single item in the JobStatus table.

    The data schema is defined as follows:
    image_id: str = unique identifier for the image associated with the job
    job_id: Optional[str] = unique identifier for the job
    image_url: Optional[str] = S3 URL or another source location for the image
    image_read_role: Optional[str] = IAM role ARN for accessing the image from its source
    model_invoke_mode: Optional[str] = mode in which the model is invoked (e.g., batch or streaming)
    start_time: Optional[int] = time in epoch milliseconds when the job started
    expire_time: Optional[int] = time in epoch seconds when the job will expire
    end_time: Optional[int] = time in epoch milliseconds when the job ended
    region_success: Optional[int] = current count of regions that have successfully processed for this image
    region_error: Optional[int] = current count of regions that have errored during processing
    region_count: Optional[int] = total count of regions expected for this image
    width: Optional[int] = width of the image in pixels
    height: Optional[int] = height of the image in pixels
    extents: Optional[str] = string representation of the image extents
    tile_size: Optional[str] = size of the tiles used during processing
    tile_overlap: Optional[str] = overlap between tiles during processing
    model_name: Optional[str] = name of the model used for processing
    outputs: Optional[str] = details about the job output
    processing_duration: Optional[int] = time in seconds taken to complete processing
    feature_properties: Optional[str] = additional feature properties or metadata from the image processing
    feature_distillation_option: Optional[str] = the options used in selecting features (e.g., NMS/SOFT_NMS, thresholds)
    roi_wkt: Optional[str] = a Well-Known Text (WKT) representation of the requested processing bounds
    """

    image_id: str
    job_id: Optional[str] = None
    image_url: Optional[str] = None
    image_read_role: Optional[str] = None
    model_invoke_mode: Optional[str] = None
    start_time: Optional[int] = None
    expire_time: Optional[int] = None
    end_time: Optional[int] = None
    region_success: Optional[int] = None
    region_error: Optional[int] = None
    region_count: Optional[int] = None
    width: Optional[int] = None
    height: Optional[int] = None
    extents: Optional[str] = None
    tile_size: Optional[str] = None
    tile_overlap: Optional[str] = None
    model_name: Optional[str] = None
    outputs: Optional[str] = None
    processing_duration: Optional[int] = None
    feature_properties: Optional[str] = None
    feature_distillation_option: Optional[str] = None
    roi_wkt: Optional[str] = None

    def __post_init__(self):
        self.ddb_key = DDBKey(hash_key="image_id", hash_value=self.image_id)

    @classmethod
    def from_image_request(cls, image_request: ImageRequest) -> "JobItem":
        """
        Create a JobItem from an ImageRequest instance.

        :param image_request: ImageRequest = The image request from which to generate the JobItem.

        :return: JobItem = A new JobItem instance with the relevant fields populated.
        """
        return cls(
            image_id=image_request.image_id,
            job_id=image_request.job_id,
            tile_size=str(image_request.tile_size),
            tile_overlap=str(image_request.tile_overlap),
            model_name=image_request.model_name,
            model_invoke_mode=image_request.model_invoke_mode,
            outputs=dumps(image_request.outputs),
            image_url=image_request.image_url,
            image_read_role=image_request.image_read_role,
            feature_properties=dumps(image_request.feature_properties),
            roi_wkt=image_request.roi.wkt if image_request.roi else None,
        )


class JobTable(DDBHelper):
    """
    JobTable is a class meant to help OSML with accessing and interacting with the image processing jobs we track
    as part of the job status table. It extends the DDBHelper class and provides its own item data class for use when
    working with items from the table. It also  sets the key for which we index on this table in the constructor.

    :param table_name: str = the name of the table to interact with

    :return: None
    """

    def __init__(self, table_name: str) -> None:
        super().__init__(table_name)

    def start_image_request(self, image_request_item: JobItem) -> JobItem:
        """
        Start an image processing request for given image_id, this should be the first record for this image in the
        table.

        :param image_request_item: the unique identifier for the image we want to add to ddb

        :return: JobItem = response from ddb
        """

        try:
            # These records are temporary and will expire 7 days after creation. Jobs should take
            # minutes to run so this time should be conservative enough to let a team debug an urgent
            # issue without leaving a ton of state leftover in the system.
            start_time_millisec = int(time.time() * 1000)
            expire_time_epoch_sec = int(int(start_time_millisec / 1000) + (7 * 24 * 60 * 60))

            # Update the job item to have the correct start parameters
            image_request_item.start_time = start_time_millisec
            image_request_item.processing_duration = 0
            image_request_item.expire_time = expire_time_epoch_sec
            image_request_item.region_success = 0
            image_request_item.region_error = 0

            # Put the item into the table
            self.put_ddb_item(image_request_item)

            # Return the updated image request
            return image_request_item
        except Exception as err:
            raise StartImageException("Failed to start image processing!") from err

    def complete_region_request(self, image_id: str, error: bool) -> JobItem:
        """
        Update the image job to reflect that a region has succeeded or failed.

        :param image_id: str = the unique identifier for the image we want to update
        :param error: bool = if there was an error processing the region, is true else false

        :return: None
        """
        try:
            # Determine if we increment the success or error counts
            if error:
                # Build custom update expression for updating region_error in DDB
                update_exp = "SET region_error = region_error + :error_count"
                # Build custom update attributes for updating region_error in DDB
                update_attr = {":error_count": int(1)}
            else:
                # Build custom update expression for updating region_error in DDB
                update_exp = "SET region_success = region_success + :success_count"
                # Build custom update attributes for updating region_error in DDB
                update_attr = {":success_count": int(1)}

            # Update item in the table and translate to a JobItem
            return from_dict(
                JobItem,
                self.update_ddb_item(
                    ddb_item=JobItem(image_id=image_id),
                    update_exp=update_exp,
                    update_attr=update_attr,
                ),
            )

        except Exception as err:
            raise CompleteRegionException("Failed to complete region!") from err

    @staticmethod
    def is_image_request_complete(region_table, image_request_item: JobItem) -> bool:
        """
        Read the table for a ddb item and determine if the image_id associated with the job has completed processing all
        regions associated with that image.

        :param region_table: RegionRequestTable = region tracking table to check for completion status
        :param image_request_item: JobItem = the unique identifier for the image we want to check if the image is completed

        :return: bool

        Alternative version of JobTable.is_image_request_complete_v1 which doesn't use the region_success field.
        TODO: Review when the region success counter is updated. Or if its even needed.
        """
        image_id = image_request_item.image_id
        total_expected_region_count = image_request_item.region_count
        failed_count, completed = get_image_request_complete_counts(region_table, image_id)
        done = (completed + failed_count) == total_expected_region_count

        return done, completed, failed_count

    @staticmethod
    def is_image_request_complete_v1(image_request_item: JobItem) -> bool:
        """
        Read the table for a ddb item and determine if the image_id associated with the job has completed processing all
        regions associated with that image.

        :param image_request_item: JobItem = the unique identifier for the image we want to check if the image is completed

        :return: bool
        """
        # Check that the image request has valid properties
        if (
            image_request_item.region_count is not None
            and image_request_item.region_success is not None
            and image_request_item.region_error is not None
        ):
            # Determine if we have completed all regions
            completed_regions = image_request_item.region_success + image_request_item.region_error
            return image_request_item.region_count == completed_regions
        else:
            raise IsImageCompleteException("Failed to check if image is complete!")

    def end_image_request(self, image_id: str) -> JobItem:
        """
        Stop an image processing job for given image_id and record the time the job ended, this should be the last
        record for this image in the table.

        :param image_id: str = the unique identifier for the image we want to stop processing

        :return: None
        """
        try:
            # Get the latest item
            image_request_item = self.get_image_request(image_id)

            # Give it an end time
            image_request_item.end_time = int(time.time() * 1000)

            # Update the item in the table
            return self.update_image_request(image_request_item)

        except Exception as e:
            raise EndImageException("Failed to end image!") from e

    def get_image_request(self, image_id: str) -> JobItem:
        """
        Get a JobItem object from the table based on the image_id provided

        :param image_id: str = the unique identifier for the image we want to start processing

        :return: JobItem = updated image request item from ddb
        """
        try:
            # Retrieve job item from our table and set to expected JobItem class
            return from_dict(JobItem, self.get_ddb_item(JobItem(image_id=image_id)))
        except Exception as e:
            raise GetImageRequestItemException("Failed to get ImageRequestItem!") from e

    def update_image_request(self, image_request_item: JobItem) -> JobItem:
        """
        Get a JobItem object from the table based on the image_id provided

        :param image_request_item: JobItem =

        :return: ImageRequestItem = updated image request item from ddb
        """
        # Update the processing time on our message
        if image_request_item.start_time is not None:
            image_request_item.processing_duration = self.get_processing_duration(int(image_request_item.start_time))

        return from_dict(JobItem, self.update_ddb_item(image_request_item))

    @staticmethod
    def get_processing_duration(start_time: int) -> int:
        return int(time.time() - (start_time / 1000))


def get_regions_for_image(table, image_id: str, status_filter: Optional[str] = None) -> List[RegionRequestItem]:
    """
    Get all regions for a specific image using the image_id as partition key.

    :param table: RegionRequestTable instance
    :param image_id: str = the image identifier
    :param status_filter: Optional[str] = filter by specific status

    :return: List[RegionRequestItem] = list of region request items for the image
    """
    try:
        # Query the table using image_id as partition key
        query_kwargs = {
            "KeyConditionExpression": "image_id = :image_id",
            "ExpressionAttributeValues": {":image_id": image_id},
        }

        # Add status filter if provided
        if status_filter:
            query_kwargs["FilterExpression"] = "#region_status = :region_status"
            query_kwargs["ExpressionAttributeNames"] = {"#region_status": "region_status"}
            query_kwargs["ExpressionAttributeValues"][":region_status"] = status_filter

        response = table.table.query(**query_kwargs)

        regions = []
        for item in response.get("Items", []):
            converted_item = table.convert_decimal(item)
            regions.append(from_dict(RegionRequestItem, converted_item))

        return regions
    except Exception as err:
        logger.error(f"Failed to get regions for image_id={image_id}: {err}")
        return []


def get_image_request_complete_counts(table, image_id: str):
    """
    Check completion status for all regions of an image and return counts.

    :param table: RegionRequestTable instance
    :param image_id: image to check
    :return: Tuple of (failed_region_count, complete_region_count)
    """
    try:
        # Get all regions for this image
        regions = get_regions_for_image(table, image_id)

        failed_region_count = 0
        completed_count = 0

        for region in regions:
            if region.region_status == "SUCCESS":
                completed_count += 1
            elif region.region_status == "FAILED":
                failed_region_count += 1
            else:
                logger.debug(f"region found in incomplete status: {region.__dict__}")

        return failed_region_count, completed_count

    except Exception as e:
        logger.error(f"Error checking image completion status: {e}")
        # Return safe defaults
        return 0, 0
