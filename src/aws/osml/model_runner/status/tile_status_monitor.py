import logging

from aws.osml.model_runner.common import RequestStatus
from aws.osml.model_runner.status.base_status_monitor import BaseStatusMonitor
from aws.osml.model_runner.status.status_message import StatusMessage

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
                status = tile_request_item.tile_status

                if status == RequestStatus.SUCCESS:
                    return RequestStatus.SUCCESS
                elif status == RequestStatus.FAILED:
                    return RequestStatus.FAILED
                elif status == RequestStatus.IN_PROGRESS:
                    return RequestStatus.IN_PROGRESS
                elif status == RequestStatus.PENDING:
                    return RequestStatus.IN_PROGRESS
                else:
                    logger.warning(f"Unknown tile_status: {status}, defaulting to IN_PROGRESS")
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

            logger.info(
                "TileStatusMonitorUpdate",
                extra={
                    "reason": message,
                    "status": status,
                    "request": tile_request_item.__dict__,
                },
            )

            sns_message_attributes = StatusMessage(
                status=status,
                image_status=status,
                job_id=tile_request_item.job_id,
                image_id=tile_request_item.image_id,
                processing_duration=tile_request_item.processing_duration,
            )

            # region_id = tile_request_item.region_id
            # job_id = tile_request_item.job_id
            # tile_id = tile_request_item.tile_id
            # status_message = f"StatusMonitor update: {status} {job_id=} {region_id=} {tile_id=}: {message}"
            # self.sns_helper.publish_message(
            #     status_message,
            #     sns_message_attributes.asdict_str_values(),
            # )

            logger.debug(f"Published tile status event: {sns_message_attributes}")

        except Exception as e:
            logger.error(f"Error processing tile status event: {e}")
            # Don't re-raise as this is a monitoring function
