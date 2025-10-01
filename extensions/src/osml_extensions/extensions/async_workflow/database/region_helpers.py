import logging
from typing import List, Optional
from dacite import from_dict

from aws.osml.model_runner.database import JobItem, RegionRequestItem

logger = logging.getLogger(__name__)


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
                logger.info(f"region found in incomplete status: {region.__dict__}")

        return failed_region_count, completed_count

    except Exception as e:
        logger.error(f"Error checking image completion status: {e}")
        # Return safe defaults
        return 0, 0


def is_image_request_complete(region_table, image_request_item: JobItem) -> bool:
    """
    Read the table for a ddb item and determine if the image_id associated with the job has completed processing all
    regions associated with that image.

    :param region_table: RegionRequestTable = region tracking table to check for completion status
    :param image_request_item: JobItem = the unique identifier for the image we want to check if the image is completed

    :return: bool

    Alternative version of JobTable.is_image_request_complete which doesn't use the region_success field.
    TODO: Review when the region success counter is updated. Or if its even needed.
    """
    image_id = image_request_item.image_id
    total_expected_region_count = image_request_item.region_count
    failed_count, completed = get_image_request_complete_counts(region_table, image_id)
    done = (completed + failed_count) == total_expected_region_count

    logger.info(f"{image_id=}: Found counts:  {done=} = ({completed=} + {failed_count=}) == {total_expected_region_count=}")
    return done, completed, failed_count
