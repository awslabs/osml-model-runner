import logging

from aws.osml.model_runner.status.base_status_monitor import BaseStatusMonitor
from aws.osml.model_runner.common import RequestStatus

logger = logging.getLogger(__name__)


class TileStatusMonitor(BaseStatusMonitor):
    def __init__(self, tile_status_topic: str):
        super().__init__(tile_status_topic)
        logger.debug(f"Initialized TileStatusMonitor with topic: {tile_status_topic}")

    def get_status(self, tile_request_item) -> RequestStatus:
        """
        Get the status of a tile request item.

        :param tile_request_item: TileRequestItem to get status for
        :return: RequestStatus enum value
        """
        try:
            if hasattr(tile_request_item, "tile_status") and tile_request_item.tile_status:
                status_str = tile_request_item.tile_status.upper()

                if status_str == "COMPLETED":
                    return RequestStatus.SUCCESS
                elif status_str == "FAILED":
                    return RequestStatus.FAILED
                elif status_str == "PROCESSING":
                    return RequestStatus.IN_PROGRESS
                elif status_str == "PENDING":
                    return RequestStatus.IN_PROGRESS
                else:
                    logger.warning(f"Unknown tile_status: {status_str}, defaulting to IN_PROGRESS")
                    return RequestStatus.IN_PROGRESS
            else:
                logger.warning("Tile request item has no tile_status, defaulting to IN_PROGRESS")
                return RequestStatus.IN_PROGRESS

        except Exception as e:
            logger.error(f"Error getting tile tile_status: {e}")
            return RequestStatus.FAILED

    def process_event(self, tile_request_item, status: RequestStatus, message: str) -> None:
        """
        Process a tile status event and publish to the status topic.

        :param tile_request_item: TileRequestItem that generated the event
        :param status: RequestStatus of the event
        :param message: Descriptive message for the event
        """
        try:
            # Create event data
            event_data = {
                "tile_id": getattr(tile_request_item, "tile_id", "unknown"),
                "job_id": getattr(tile_request_item, "job_id", "unknown"),
                "region_id": getattr(tile_request_item, "region_id", "unknown"),
                "image_id": getattr(tile_request_item, "image_id", "unknown"),
                "status": status.value if hasattr(status, "value") else str(status),
                "message": message,
                "processing_duration": getattr(tile_request_item, "processing_duration", None),
                "retry_count": getattr(tile_request_item, "retry_count", 0),
            }

            # Use the base class method to publish the event
            self.publish_event(event_data)

            logger.debug(f"Published tile status event: {event_data}")

        except Exception as e:
            logger.error(f"Error processing tile status event: {e}")
            # Don't re-raise as this is a monitoring function
