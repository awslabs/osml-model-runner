import time
import uuid
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from typing import Optional

from dacite import from_dict

from aws_oversightml_model_runner.database.ddb_helper import DDBHelper, DDBItem, DDBKey
from aws_oversightml_model_runner.exceptions.exceptions import (
    CompleteRegionException,
    InvalidRegionRequestException,
    StartRegionException,
)


class RegionRequestStatus(str, Enum):
    """
    Enumeration defining job status for region
    """

    STARTING = "STARTING"
    PARTIAL = "PARTIAL"
    IN_PROGRESS = "IN_PROGRESS"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


@dataclass
class RegionRequestItem(DDBItem):
    """
    RegionRequestItem is a dataclass meant to represent a single item in the Region table

    The data schema is defined as follows:
    region_id: str = primary key - formatted as image_id + "-" + region (pixel bounds)
    image_id: str = secondary key - image_id for the job
    start_time: Optional[Decimal] = time in epoch seconds when the job started
    last_updated_time: Optional[Decimal] = time in epoch seconds when job is processing (periodically)
    end_time: Optional[Decimal] = time in epoch seconds when the job ended
    message: Optional[str] = information about the region job
    status: Optional[str] = region job status - PROCESSING, COMPLETED, FAILED
    total_tiles: Optional[str] = number of tiles in a region
    tiles_success: Optional[Decimal] = current count of regions that have succeeded for this image
    tiles_error: Optional[Decimal] = current count of regions that have errored for this image
    region_retry_count: Optional[Decimal] = total count of regions expected for this image
    region_pixel_bounds: = Region pixel bounds
    """

    region_id: str
    image_id: str
    uuid_key: str = str(uuid.uuid4())
    start_time: Optional[Decimal] = None
    last_updated_time: Optional[Decimal] = None
    end_time: Optional[Decimal] = None
    message: Optional[str] = None
    status: Optional[str] = None
    total_tiles: Optional[str] = None
    tiles_success: Optional[Decimal] = None
    tiles_error: Optional[Decimal] = None
    region_retry_count: Optional[Decimal] = None
    region_pixel_bounds: Optional[int] = None

    def __post_init__(self):
        self.ddb_key = DDBKey(
            hash_key="image_id",
            hash_value=self.image_id,
            range_key="region_id",
            range_value=f"{self.region_pixel_bounds}-{self.uuid_key}",
        )


class RegionRequestTable(DDBHelper):
    """
    RegionRequestTable is a class meant to help OSML with accessing and interacting with the region processing jobs we track
    as part of the region table. It extends the DDBHelper class and provides its own item data class for use when
    working with items from the table. It also sets the key for which we index on this table in the constructor.

    :param table_name: (str) the name of the table to interact with
    """

    def __init__(self, table_name: str):
        super().__init__(table_name)

    def start_region_request(self, region_request_item: RegionRequestItem) -> dict:
        """
        Start an region processing request for given region pixel bounds, this should be the first record
        for this region in the table.

        :param region_request_item:
        :return: dict: response from ddb
        """

        try:
            start_time_millisec = Decimal(time.time() * 1000)

            # Update the job item to have the correct start parameters
            region_request_item.start_time = start_time_millisec
            region_request_item.tiles_success = Decimal(0)
            region_request_item.tiles_error = Decimal(0)
            region_request_item.status = RegionRequestStatus.STARTING

            # Put the item into the table
            return self.put_ddb_item(region_request_item)
        except Exception as e:
            raise StartRegionException("Failed to start region processing!") from e

    def get_region_request_item(self, region_id: str, image_id: str) -> Optional[RegionRequestItem]:
        """
        Get the item from the DDB, if it exist, return RegionRequestItem otherwise None

        :param region_id: str = unique idenitifer for the region we want to fetch

        :return: bool
        """
        try:
            return from_dict(
                RegionRequestItem,
                self.get_ddb_item(RegionRequestItem(region_id=region_id, image_id=image_id)),
            )
        except Exception:
            return None  # it does not exist

    def complete_region_request(
        self, region_id: str, image_id: str, error: bool = False
    ) -> RegionRequestItem:
        """
        Update the image job to reflect that a region has succeeded or failed.

        :param image_id: str = the unique identifier for the image we want to update
        :param error: bool = if there was an error processing the region, is true else false

        :return: None
        """
        try:
            # Determine if we increment the success or error counts, useful for retry counts
            update_exp = "SET status = status + :status;"
            if error:
                # Build custom update expression for updating tiles_error in DDB
                update_exp += "SET tiles_error = tiles_error + :error_count;"
                update_attr = {":error_count": Decimal(1), ":status": str}
            else:
                # Build custom update expression for updating region_success in DDB
                update_exp += "SET tiles_success = tiles_success + :success_count;"
                update_attr = {":success_count": Decimal(1), ":status": str}

            # Update item in the table and translate to a JobItem
            return from_dict(
                RegionRequestItem,
                self.update_ddb_item(
                    ddb_item=RegionRequestItem(region_id=region_id, image_id=image_id),
                    update_exp=update_exp,
                    update_attr=update_attr,
                ),
            )

        except Exception as e:
            raise CompleteRegionException("Failed to complete region!") from e

    def update_region_request(self, region_request_item: RegionRequestItem) -> RegionRequestItem:
        """
        Get a RegionRequestItem object from the table based on the region_id provided

        :param region_request_item:

        :return: RegionRequestItem
        """
        return from_dict(RegionRequestItem, self.update_ddb_item(region_request_item))

    @staticmethod
    def get_region_request_status(region_request_item: RegionRequestItem) -> RegionRequestStatus:
        """
        Produce a region request status from a given region request

        :param region_request_item:

        :return: RegionRequestStatus
        """
        # Check that the region request has valid properties
        if (
            region_request_item.total_tiles is not None
            and region_request_item.tiles_success is not None
            and region_request_item.tiles_error is not None
        ):
            if region_request_item.tiles_success == region_request_item.total_tiles:
                return RegionRequestStatus.SUCCESS
            elif (
                region_request_item.tiles_success + region_request_item.tiles_error
                == region_request_item.total_tiles
            ):
                return RegionRequestStatus.PARTIAL
            elif region_request_item.tiles_error == region_request_item.total_tiles:
                return RegionRequestStatus.FAILED
            else:
                return RegionRequestStatus.IN_PROGRESS
        else:
            raise InvalidRegionRequestException("Failed get status for given region request!")
