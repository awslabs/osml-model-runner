#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

import logging

from osgeo import gdal

from aws.osml.gdal import load_gdal_dataset, set_gdal_default_configuration
from aws.osml.model_runner.api import get_image_path

from .api import ImageRequest, RegionRequest, TileRequest
from .app_config import ServiceConfig
from .common import EndpointUtils, RequestStatus, ThreadingLocalContextFilter
from .database import (
    EndpointStatisticsTable,
    ImageRequestItem,
    ImageRequestTable,
    RegionRequestItem,
    RegionRequestTable,
    RequestedJobsTable,
    TileRequestItem,
    TileRequestTable,
)
from .exceptions import (
    ProcessImageException,
    RetryableJobException,
    SelfThrottledRegionException,
    SelfThrottledTileException,
    InvocationFailure,
    SkipException
)
from .image_request_handler import ImageRequestHandler
from .queue import BufferedImageRequestQueue, RequestQueue
from .region_request_handler import RegionRequestHandler
from .tile_request_handler import TileRequestHandler
from .scheduler import EndpointLoadImageScheduler
from .status import ImageStatusMonitor, RegionStatusMonitor, TileStatusMonitor
from .tile_worker import TilingStrategy, VariableOverlapTilingStrategy

# Set up logging configuration
logger = logging.getLogger(__name__)
gdal.UseExceptions()


class ModelRunner:
    """
    Main class for operating the ModelRunner application. It monitors input queues for processing requests,
    decomposes the image into smaller regions and tiles, invokes an ML model on each tile, and aggregates
    the results into a single output, which can be sent to the configured output sinks.
    """

    def __init__(self, tiling_strategy: TilingStrategy = VariableOverlapTilingStrategy()) -> None:
        """
        Initialize a model runner with the injectable behaviors.

        :param tiling_strategy: Defines how a larger image will be broken into chunks for processing

        :return: None
        """
        self.config = ServiceConfig()
        self.tiling_strategy = tiling_strategy

        # Set up internal queues and monitors
        self.region_request_queue = RequestQueue(self.config.region_queue, wait_seconds=10)
        self.region_requests_iter = iter(self.region_request_queue)

        # Set up tables and status monitors
        self.image_request_table = ImageRequestTable(self.config.image_request_table)
        self.region_request_table = RegionRequestTable(self.config.region_request_table)
        self.endpoint_statistics_table = EndpointStatisticsTable(self.config.endpoint_statistics_table)
        self.image_status_monitor = ImageStatusMonitor(self.config.image_status_topic)
        self.region_status_monitor = RegionStatusMonitor(self.config.region_status_topic)
        self.endpoint_utils = EndpointUtils()

        # Set up async-specific components
        self.tile_request_queue = RequestQueue(ServiceConfig.tile_queue, wait_seconds=0)
        self.tile_requests_iter = iter(self.tile_request_queue)

        self.tile_request_table = TileRequestTable(ServiceConfig.tile_request_table)
        self.tile_status_monitor = TileStatusMonitor(ServiceConfig.tile_status_topic)

        # Create enhanced handlers with async workflow
        self.tile_request_handler = TileRequestHandler(
            tile_request_table=self.tile_request_table,
            image_request_table=self.image_request_table,
            tile_status_monitor=self.tile_status_monitor,
        )

        # Handlers for image and region processing
        self.region_request_handler = RegionRequestHandler(
            region_request_table=self.region_request_table,
            image_request_table=self.image_request_table,
            region_status_monitor=self.region_status_monitor,
            endpoint_statistics_table=self.endpoint_statistics_table,
            tiling_strategy=self.tiling_strategy,
            endpoint_utils=self.endpoint_utils,
            config=self.config,
            tile_request_table=self.tile_request_table,
        )
        self.image_request_handler = ImageRequestHandler(
            image_request_table=self.image_request_table,
            image_status_monitor=self.image_status_monitor,
            endpoint_statistics_table=self.endpoint_statistics_table,
            tiling_strategy=self.tiling_strategy,
            region_request_queue=self.region_request_queue,
            region_request_table=self.region_request_table,
            endpoint_utils=self.endpoint_utils,
            config=self.config,
            region_request_handler=self.region_request_handler,
            tile_request_table=self.tile_request_table,
        )

        # Set up the job scheduler
        self.requested_jobs_table = RequestedJobsTable(self.config.outstanding_jobs_table)
        self.image_job_scheduler = EndpointLoadImageScheduler(
            BufferedImageRequestQueue(self.config.image_queue, self.config.image_dlq, self.requested_jobs_table)
        )
        self.region_request_handler.on_region_complete.subscribe(
            lambda image_request, region_request, region_status: self.requested_jobs_table.complete_region(
                image_request, region_request.region_id
            )
        )
        self.image_request_handler.on_image_update.subscribe(
            lambda image_request: self.requested_jobs_table.update_request_details(image_request, image_request.region_count)
        )

        self.running = False

    def run(self) -> None:
        """
        Start the ModelRunner to continuously monitor and process work queues.

        :return: None
        """
        logger.info("Starting ModelRunner")
        self.running = True
        self.monitor_work_queues()

    def stop(self) -> None:
        """
        Stop the ModelRunner.

        :return: None
        """
        logger.info("Stopping ModelRunner")
        self.running = False

    def monitor_work_queues(self) -> None:
        """
        Continuously monitors the SQS queues for RegionRequest and ImageRequest.
        :return: None
        """
        set_gdal_default_configuration()
        logger.info("Beginning monitoring request queues")
        while self.running:
            try:
                # If there are tiles to process
                if not self._process_tile_requests():
                    # If there are regions to process
                    if not self._process_region_requests():
                        # Move along to the next image request if present
                        self._process_image_requests()
            except Exception as err:
                logger.error(f"Unexpected error in monitor_work_queues: {err}", exc_info=True)
                self.running = False
        logger.info("Stopped monitoring request queues")

    def _process_tile_requests(self) -> bool:
        try:
            receipt_handle, event_message = next(self.tile_requests_iter)
        except StopIteration:
            # No tiles to process
            logger.debug("No tile requests available to process")
            return False

        if event_message:
            ThreadingLocalContextFilter.set_context(event_message)
            try:
                # get tile item from event message
                tile_request_item = self.tile_request_table.get_tile_request_by_event(event_message)

                if not tile_request_item:
                    # logger.warning(f"No tile request found for: {event_message}")
                    raise RetryableJobException(f"No tile request found for: {event_message}")

                # Check if tile failed
                if tile_request_item.tile_status == RequestStatus.FAILED:
                    raise InvocationFailure(event_message.get("failureReason"))

                # Check if tile already done
                if tile_request_item.tile_status == RequestStatus.SUCCESS:
                    logger.info(
                        f"Tile {tile_request_item.tile_id} already completed with status: {tile_request_item.tile_status}"
                    )
                else:
                    # Create TileRequest from TileRequestItem for processing
                    tile_request = TileRequest.from_tile_request_item(tile_request_item)

                    # Process the completed tile request
                    self.tile_request_handler.process_tile_request(tile_request, tile_request_item)

                    # Complete the tile request
                    tile_request_item = self.tile_request_table.complete_tile_request(tile_request_item, RequestStatus.SUCCESS)
                
                # check if completed the region and image
                self.complete_tile_request(tile_request_item)
                self.tile_request_queue.finish_request(receipt_handle)

            except InvocationFailure as err:
                logger.warning(f"Setting tile ({tile_request_item.region_id=}, {tile_request_item.tile_id=}) failure due to: {err}")
                tile_request_item = self.tile_request_handler.fail_tile_request(tile_request_item)
                self.complete_tile_request(tile_request_item)
                self.tile_request_queue.finish_request(receipt_handle)
            except RetryableJobException as err:
                logger.warning(f"Retrying tile request due to: {err}")
                self.tile_request_queue.reset_request(receipt_handle, visibility_timeout=60)
            except SelfThrottledTileException as err:
                logger.warning(f"Retrying tile request due to throttling error: {err}")
                self.tile_request_queue.reset_request(receipt_handle, visibility_timeout=int(ServiceConfig.throttling_retry_timeout))
            except SkipException as err:
                self.tile_request_queue.finish_request(receipt_handle)
            except Exception as err:
                logger.exception(f"Error processing tile request: {err}", exc_info=True)
            finally:
                return True
        else:
            return False

    def _process_region_requests(self) -> bool:
        """
        Process messages from the region request queue.

        :return: True if a region request was processed, False if not.
        """
        logger.debug("Checking work queue for regions to process...")
        try:
            receipt_handle, region_request_attributes = next(self.region_requests_iter)
        except StopIteration:
            # No region requests available in the queue
            logger.debug("No region requests available to process.")
            return False

        if region_request_attributes:
            ThreadingLocalContextFilter.set_context(region_request_attributes)
            try:
                region_request = RegionRequest(region_request_attributes)
                image_path = get_image_path(region_request.image_url, region_request.image_read_role)
                raster_dataset, sensor_model = load_gdal_dataset(image_path)
                region_request_item = self.region_request_table.get_or_create_region_request_item(region_request)
                image_request_item = self.region_request_handler.process_region_request(
                    region_request, region_request_item, raster_dataset, sensor_model
                )
                image_is_done, _, _ = self.region_request_table.is_image_request_complete(image_request_item)
                if image_is_done:
                    self.image_request_handler.complete_image_request(
                        region_request, str(raster_dataset.GetDriver().ShortName).upper(), raster_dataset, sensor_model
                    )
                self.region_request_queue.finish_request(receipt_handle)
            except RetryableJobException as err:
                logger.warning(f"Retrying region request due to: {err}")
                self.region_request_queue.reset_request(receipt_handle, visibility_timeout=0)
            except SelfThrottledRegionException as err:
                logger.warning(f"Retrying region request due to: {err}")
                self.region_request_queue.reset_request(
                    receipt_handle, visibility_timeout=int(self.config.throttling_retry_timeout)
                )
            except Exception as err:
                logger.exception(f"Error processing region request: {err}")
                self.region_request_queue.finish_request(receipt_handle)
            finally:
                ThreadingLocalContextFilter.set_context(None)
            return True
        else:
            return False

    def _process_image_requests(self) -> bool:
        """
        Processes messages from the image job scheduler.

        :return: True if an image request was processed, False if not.
        """
        image_request = self.image_job_scheduler.get_next_scheduled_request()
        if image_request:
            try:
                ThreadingLocalContextFilter.set_context(image_request.__dict__)
                self.image_request_handler.process_image_request(image_request)
                self.image_job_scheduler.finish_request(image_request)
            except RetryableJobException:
                self.image_job_scheduler.finish_request(image_request, should_retry=True)
            except Exception as err:
                logger.error(f"Error processing image request: {err}")
                self._fail_image_request(image_request, err)
                self.image_job_scheduler.finish_request(image_request)
            finally:
                ThreadingLocalContextFilter.set_context(None)
            return True
        else:
            return False

    def _fail_image_request(self, image_request: ImageRequest, error: Exception) -> None:
        """
        Handles failing an image request by updating its status and logging the failure.

        This method is called when an image request cannot be processed due to an error.
        It marks the image request as failed and updates the job status using the
        `ImageRequestHandler`.

        :param image_request: The image request that failed to process.
        :param error: The exception that caused the failure.

        :return: None
        """
        min_image_id = image_request.image_id if image_request else ""
        min_job_id = image_request.job_id if image_request else ""
        minimal_image_request_item = ImageRequestItem(image_id=min_image_id, job_id=min_job_id, processing_duration=0)
        self.image_request_handler.fail_image_request(minimal_image_request_item, error)

    def check_if_region_request_complete(self, tile_request_item: TileRequestItem):
        """
            Check if the region is complete by checking the tables for completed tiles for the given region.
        """
        # Get region request and region request item
        region_request_item = self.region_request_table.get_region_request(
            tile_request_item.region_id, tile_request_item.image_id
        )
        total_expected_tile_count = region_request_item.total_tiles
        if not total_expected_tile_count:
            # region request hasn't finished submitting tiles
            return region_request_item, False

        failed_count, completed = self.tile_request_table.get_region_request_complete_counts(tile_request_item)

        done = (completed + failed_count) >= total_expected_tile_count

        region_status = region_request_item.region_status
        region_id = region_request_item.region_id
        logger.debug(
            f"{region_id=}: Found counts:  {done=} = ({completed=} + {failed_count=}) == {total_expected_tile_count=}"
        )
        logger.debug(f"{region_id=}: {region_status=} in [{RequestStatus.SUCCESS.name}, {RequestStatus.FAILED.name}]")
        if not done:
            return region_request_item, False

        # region done

        # if aleady marked done, finish
        if region_status in [RequestStatus.SUCCESS.name, RequestStatus.FAILED.name]:
            logger.info(f"{region_id=} already completed, skipping")
            return region_request_item, True

        # Update table w/ total tile counts
        region_request_item.succeeded_tile_count = completed
        region_request_item.failed_tile_count = failed_count

        # Update region request table
        region_status = self.region_status_monitor.get_status(region_request_item) # returns status based on counts
        region_request_item = self.region_request_table.complete_region_request(region_request_item, region_status)

        # Update the image request to complete this region
        _ = self.image_request_table.complete_region_request(tile_request_item.image_id, bool(failed_count))
        logger.debug(f"{region_id=}: Marking region success in job table for : {tile_request_item.__dict__}")

        # region_request_item = self.region_request_table.update_region_request(region_request_item)

        self.region_status_monitor.process_event(region_request_item, region_status, "Completed region processing")

        return region_request_item, True

    def complete_tile_request(self, tile_request_item: TileRequestItem):

        # Check if the region is done
        completed_region_request_item, region_is_done = self.check_if_region_request_complete(tile_request_item)

        if region_is_done:
            # Check if the whole image is done
            image_request_item = self.image_request_table.get_image_request(tile_request_item.image_id)
            image_is_done, region_complete, region_failures = self.region_request_table.is_image_request_complete(
                image_request_item
            )
            logger.debug(f"image complete check for {tile_request_item.image_id} = {image_is_done}")
            if image_is_done:
                # update region counts
                image_request_item.region_error = region_failures
                image_request_item.region_success = region_complete
                image_request_item = self.image_request_table.update_image_request(image_request_item)

                if not region_complete:  # no success in any region
                    image_request = ImageRequest()
                    image_request.image_id = image_request_item.image_id
                    image_request.job_id = image_request_item.job_id
                    self._fail_image_request(image_request, ProcessImageException("All Regions Failed/Partial"))
                else:
                    # close and get features
                    image_path = get_image_path(
                        tile_request_item.image_url, tile_request_item.image_read_role
                    )
                    raster_dataset, sensor_model = load_gdal_dataset(image_path)

                    region_request = RegionRequest()
                    region_request.image_id = completed_region_request_item.image_id
                    region_request.region_id = completed_region_request_item.region_id
                    region_request.tile_size = completed_region_request_item.tile_size
                    region_request.tile_overlap = completed_region_request_item.tile_overlap

                    logger.debug(
                        f"Calling image handler complete image request for {tile_request_item.image_id}"
                    )
                    self.image_request_handler.complete_image_request(
                        region_request,
                        str(raster_dataset.GetDriver().ShortName).upper(),
                        raster_dataset,
                        sensor_model,
                    )
