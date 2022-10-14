import time
from dataclasses import dataclass
from decimal import Decimal
from typing import Optional

from dacite import from_dict

from aws_oversightml_model_runner.ddb.ddb_helper import DDBHelper, DDBKey
from aws_oversightml_model_runner.exceptions.exceptions import (
    CompleteRegionFailed,
    EndImageFailed,
    GetJobItemFailed,
    ImageStatsFailed,
    IsImageCompleteFailed,
    StartImageFailed,
)


@dataclass
class JobItem(DDBKey):
    """
    JobItem is a dataclass meant to represent a single item in the JobStatus table

    The data schema is defined as follows:
    image_id: str = unique image_id for the job
    start_time: Optional[Decimal] = time in epoch seconds when the job started
    expire_time: Optional[Decimal] = time in epoch seconds when the job will expire
    end_time: Optional[Decimal] = time in epoch seconds when the job ended
    region_success: Optional[Decimal] = current count of regions that have succeeded for this image
    region_error: Optional[Decimal] = current count of regions that have errored for this image
    region_count: Optional[Decimal] = total count of regions expected for this image
    width: Optional[Decimal] = width of the image
    height: Optional[Decimal] = height of the image
    """

    image_id: str
    start_time: Optional[Decimal] = None
    expire_time: Optional[Decimal] = None
    end_time: Optional[Decimal] = None
    region_success: Optional[Decimal] = None
    region_error: Optional[Decimal] = None
    region_count: Optional[Decimal] = None
    width: Optional[Decimal] = None
    height: Optional[Decimal] = None

    def __post_init__(self):
        self.hash_key = "image_id"
        self.hash_value = self.image_id


class JobTable(DDBHelper):
    """
    JobTable is a class meant to help OSML with accessing and interacting with the image processing jobs we track
    as part of the job status table. It extends the DDBHelper class and provides its own item data class for use when
    working with items from the table. It also  sets the key for which we index on this table in the constructor.

    :param table_name: (str) the name of the table to interact with
    """

    def __init__(self, table_name: str):
        super().__init__(table_name)

    def start_image(self, image_id: str) -> dict:
        """
        Start an image processing job for given image_id, this should be the first record for this image in the table.

        :param image_id: str = the unique identifier for the image we want to start processing.

        :return: None
        """

        try:
            # These records are temporary and will expire 24 hours after creation. Jobs should take
            # minutes to run so this time should be conservative enough to let a team debug an urgent
            # issue without leaving a ton of state leftover in the system.
            start_time_millisec = Decimal(time.time() * 1000)
            expire_time_millisec = Decimal(start_time_millisec + (24 * 60 * 60 * 1000))

            # Create a JobItem data contract to work with
            job_item = JobItem(
                image_id=image_id,
                start_time=start_time_millisec,
                expire_time=expire_time_millisec,
                region_success=Decimal(0),
                region_error=Decimal(0),
            )

            # Put the item into the table
            return self.put_ddb_item(job_item)
        except Exception as e:
            raise StartImageFailed("Failed to start image processing!") from e

    def complete_region(self, image_id: str, error: bool = False) -> dict:
        """
        Update the image job to reflect that a region has succeeded or failed.

        :param image_id: str = the unique identifier for the image we want to update
        :param error: bool = if there was an error processing the region, is true else false

        :return: None
        """

        try:
            # Create a JobItem data contract to work with
            job_item = JobItem(image_id=image_id)

            # Determine if we increment the success or error counts
            if error:
                # Build custom update expression for updating region_error in DDB
                update_exp = "SET region_error = region_error + :error_count"
                # Build custom update attributes for updating region_error in DDB
                update_attr = {":error_count": Decimal(1)}
            else:
                # Build custom update expression for updating region_error in DDB
                update_exp = "SET region_success = region_success + :success_count"
                # Build custom update attributes for updating region_error in DDB
                update_attr = {":success_count": Decimal(1)}

            # Update item in the table
            return self.update_ddb_item(
                ddb_item=job_item, update_exp=update_exp, update_attr=update_attr
            )

        except Exception as e:
            raise CompleteRegionFailed("Failed to complete region!") from e

    def is_image_complete(self, image_id: str) -> bool:
        """
        Read the table for a ddb item and determine if the image_id associated with the job has completed processing all
        regions associated with that image.

        :param image_id: str = the unique identifier for the image we want to read the status for

        :return: None
        """

        try:
            # Grab our JobItem from the table
            job_item = self.get_job_item(image_id)

            # If this item already has valid regions properties
            if (
                job_item.region_count is not None
                and job_item.region_success is not None
                and job_item.region_error is not None
            ):
                # Determine if we have completed all regions
                return job_item.region_count <= (job_item.region_success + job_item.region_error)
            # Else wait for valid region count
            return False

        except Exception as e:
            raise IsImageCompleteFailed("Failed to complete image!") from e

    def image_stats(
        self, image_id: str, region_count: Decimal, width: Decimal, height: Decimal
    ) -> None:
        """
        Update the image job to reflect statistics gathered about the source image for that job

        :param image_id: str = the unique identifier for the image we want to update
        :param region_count: int = total number of regions expected for this image
        :param width: int = width of the image
        :param height: int = height of the image

        :return: None
        """
        try:
            # Build a job item
            job_item = JobItem(
                image_id=image_id,
                region_count=region_count,
                width=width,
                height=height,
            )

            # Update the item in the table
            self.update_ddb_item(job_item)
        except Exception as e:
            raise ImageStatsFailed("Failed to stat image!") from e

    def end_image(self, image_id: str) -> None:
        """
        Stop an image processing job for given image_id and record the time the job ended, this should be the last
        record for this image in the table.

        :param image_id: str = the unique identifier for the image we want to start processing

        :return: None
        """
        try:
            # Grab the current time
            end_time_millisec = Decimal(time.time() * 1000)

            # Build a job item
            job_item = JobItem(image_id=image_id, end_time=end_time_millisec)

            # Update the item in the table
            self.update_ddb_item(job_item)
        except Exception as e:
            raise EndImageFailed("Failed to end image!") from e

    def get_job_item(self, image_id: str) -> JobItem:
        """
        Get a JobItem object from the table based on the image_id provided

        :param image_id: str = the unique identifier for the image we want to start processing

        :return: JobItem
        """
        try:
            # Retrieve job item from our table and set to expected JobItem class
            return from_dict(JobItem, self.get_ddb_item(JobItem(image_id=image_id)))
        except Exception as e:
            raise GetJobItemFailed("Failed to get JobItem!") from e
