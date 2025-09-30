#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import json
import logging
import traceback

from aws.osml.gdal import set_gdal_default_configuration, load_gdal_dataset
from aws.osml.model_runner import ModelRunner
from aws.osml.model_runner.api import RegionRequest, get_image_path
from aws.osml.model_runner.tile_worker import TilingStrategy, VariableOverlapTilingStrategy
from aws.osml.model_runner.queue import RequestQueue
from aws.osml.model_runner.common import ThreadingLocalContextFilter
from aws_embedded_metrics.metric_scope import metric_scope
from aws_embedded_metrics.unit import Unit
from aws_embedded_metrics.logger.metrics_logger import MetricsLogger
from aws.osml.model_runner.app_config import MetricLabels

from .async_app_config import AsyncServiceConfig
from .api import TileRequest
from .database import TileRequestItem, TileRequestTable
from .enhanced_tile_handler import TileRequestHandler
from .status import TileStatusMonitor
from .errors import SelfThrottledTileException, RetryableJobException
from .enhanced_image_handler import EnhancedImageRequestHandler
from .enhanced_region_handler import EnhancedRegionRequestHandler

logger = logging.getLogger(__name__)


class EnhancedModelRunner(ModelRunner):
    """
    Enhanced ModelRunner with dependency injection support for extensions.

    This class demonstrates how the ModelRunner could be modified to support
    dependency injection while maintaining backward compatibility.
    """

    def __init__(
        self,
        tiling_strategy: TilingStrategy = VariableOverlapTilingStrategy(),
    ) -> None:
        """
        Initialize an enhanced model runner with dependency injection support.

        :param tiling_strategy: Defines how a larger image will be broken into chunks for processing

        :return: None
        """
        # Call parent constructor to set up base functionality
        super().__init__(tiling_strategy)

        # Override handlers with enhanced versions if extensions are enabled
        self._setup_enhanced_components()

        logger.debug(f"EnhancedModelRunner initialized with factory: {type(self.region_request_handler).__name__}")

    def _setup_enhanced_components(self) -> None:
        """
        Set up enhanced components for async workflow processing.

        :return: None
        """
        try:
            # Set up async-specific components
            self.tile_request_queue = RequestQueue(AsyncServiceConfig.tile_queue, wait_seconds=0)
            self.tile_requests_iter = iter(self.tile_request_queue)

            self.tile_request_table = TileRequestTable(AsyncServiceConfig.tile_request_table)
            self.tile_status_monitor = TileStatusMonitor(AsyncServiceConfig.tile_status_topic)

            # Create enhanced handlers with async workflow support
            self.image_request_handler = EnhancedImageRequestHandler(
                job_table=self.job_table,
                image_status_monitor=self.image_status_monitor,
                endpoint_statistics_table=self.endpoint_statistics_table,
                tiling_strategy=self.tiling_strategy,
                region_request_queue=self.region_request_queue,
                region_request_table=self.region_request_table,
                endpoint_utils=self.endpoint_utils,
                config=AsyncServiceConfig,
                region_request_handler=self.region_request_handler,
            )

            self.region_request_handler = EnhancedRegionRequestHandler(
                tile_request_table=self.tile_request_table,
                tile_request_queue=self.tile_request_queue,
                region_request_table=self.region_request_table,
                job_table=self.job_table,
                region_status_monitor=self.region_status_monitor,
                endpoint_statistics_table=self.endpoint_statistics_table,
                tiling_strategy=self.tiling_strategy,
                endpoint_utils=self.endpoint_utils,
                config=AsyncServiceConfig,
            )

            self.tile_request_handler = TileRequestHandler(
                tile_request_table=self.tile_request_table,
                job_table=self.job_table,
                tile_status_monitor=self.tile_status_monitor,
            )

            logger.debug("Successfully configured enhanced components for async workflow")

        except Exception as e:
            logger.error(f"Unexpected error setting up enhanced components: {e}")
            logger.debug(f"Traceback: {traceback.format_exc()}")
            raise

    def monitor_work_queues(self) -> None:
        """
        Continuously monitors the SQS queues for RegionRequest and ImageRequest.
        :return: None
        """
        set_gdal_default_configuration()
        logger.info("Beginning monitoring request queues")
        while self.running:
            try:
                # If there are no tiles to process
                if not self._process_tile_requests():
                    # If there are regions to process
                    if not self._process_region_requests():
                        # Move along to the next image request if present
                        self._process_image_requests()
            except Exception as err:
                logger.error(f"Unexpected error in monitor_work_queues: {err}")
                self.running = False
        logger.info("Stopped monitoring request queues")

    def _process_tile_requests(self) -> bool:
        try:
            receipt_handle, s3_event_message = next(self.tile_requests_iter)
        except StopIteration:
            # No tiles to process
            logger.debug("No tile requests available to process")
            return False

        if s3_event_message:
            ThreadingLocalContextFilter.set_context(s3_event_message)
            try:
                # Parse S3 event notification to get output location
                output_location = self._parse_s3_event_for_output_location(s3_event_message)
                if not output_location:
                    logger.warning(f"Could not extract output location from S3 event: {s3_event_message}")
                    self.tile_request_queue.finish_request(receipt_handle)
                    return True

                logger.debug(f"Processing completed inference result at: {output_location}")

                # Get tile request by output location
                tile_request_item = self.tile_request_table.get_tile_request_by_output_location(output_location)
                if not tile_request_item:
                    logger.warning(f"No tile request found for output_location: {output_location}")
                    self.tile_request_queue.finish_request(receipt_handle)
                    return True
                if self.tile_request_table.is_tile_item_done(tile_request_item):
                    logger.info(
                        f"Tile {tile_request_item.tile_id} already completed with status: {tile_request_item.tile_status}"
                    )
                    self.tile_request_queue.finish_request(receipt_handle)
                    return True

                # Create TileRequest from TileRequestItem for processing
                tile_request = TileRequest.from_tile_request_item(tile_request_item)
                tile_request.output_location = output_location  # update the output location

                # Process the completed tile request
                self.tile_request_handler.process_tile_request(tile_request, tile_request_item)

                # Complete the tile request
                completed_tile_request_item = self.tile_request_table.complete_tile_request(tile_request_item, "COMPLETED")

                # Check if the region is done
                completed_region_request_item = self.check_if_region_request_complete(completed_tile_request_item)

                # Check if the whole image is done
                image_request_item = self.job_table.get_image_request(completed_tile_request_item.image_id)
                is_done = self.job_table.is_image_request_complete(image_request_item)
                logger.info(f"image complete check for {completed_tile_request_item.image_id} = {is_done}")
                if is_done:
                    image_path = get_image_path(
                        completed_tile_request_item.image_url, completed_tile_request_item.image_read_role
                    )
                    raster_dataset, sensor_model = load_gdal_dataset(image_path)

                    region_request = RegionRequest()
                    region_request.image_id = completed_region_request_item.image_id
                    region_request.region_id = completed_region_request_item.region_id
                    region_request.tile_size = completed_region_request_item.tile_size
                    region_request.tile_overlap = completed_region_request_item.tile_overlap
                    # region_request.job_id = completed_region_request_item.job_id

                    logger.info(f"Calling image handler complete image request for {completed_tile_request_item.image_id}")
                    self.image_request_handler.complete_image_request(
                        region_request, str(raster_dataset.GetDriver().ShortName).upper(), raster_dataset, sensor_model
                    )

                # Finish the current request
                self.tile_request_queue.finish_request(receipt_handle)
            except RetryableJobException as err:
                logger.warning(f"Retrying tile request due to: {err}")
                self.tile_request_queue.reset_request(receipt_handle, visibility_timeout=0)
            except SelfThrottledTileException as err:
                logger.warning(f"Retrying tile request due to: {err}")
                self.tile_request_queue.reset_request(
                    receipt_handle, visibility_timeout=int(AsyncServiceConfig.throttling_retry_timeout)
                )
            except Exception as err:
                logger.exception(f"Error processing tile request: {err}")
                self.tile_request_queue.finish_request(receipt_handle)
            finally:
                return True
        else:
            return False

    def _parse_s3_event_for_output_location(self, s3_event_message: dict) -> str:
        """
        Parse S3 event notification to extract the output location (S3 URI).

        :param s3_event_message: S3 event notification message from SQS
        :return: S3 URI of the output location, or empty string if parsing fails
        """
        try:

            # S3 message
            # {
            #     'Records': [{
            #         'eventVersion': '2.1',
            #         'eventSource': 'aws:s3',
            #         'awsRegion': 'us-west-2',
            #         'eventTime': '2025-09-28T07:28:42.606Z',
            #         'eventName': 'ObjectCreated:Put',
            #         'userIdentity': {
            #             'principalId': 'AWS:AROAZI2LIXEZ7G5XQA3QP:SageMaker'
            #         },
            #         'requestParameters': {
            #             'sourceIPAddress': '10.2.11.121'
            #         },
            #         'responseElements': {
            #             'x-amz-request-id': 'JT4J929BNCAZ6MZS',
            #             'x-amz-id-2': 'oRE6zlp4M1...AHiOjvHDGG5'
            #         },
            #         's3': {
            #             's3SchemaVersion': '1.0',
            #             'configurationId': 'NzI1NzcwNDEtNTU0Mi00NzQ3LTk0YzktY2NiMjZjNTljYzJl',
            #             'bucket': {
            #                 'name': 'modelrunner-infra-mrartifactbucketf483353e-x0nfaecdvfrr',
            #                 'ownerIdentity': {
            #                     'principalId': 'A244AJ6LIN4SSK'
            #                 },
            #                 'arn': 'arn:aws:s3:::modelrunner-infra-mrartifactbucketf483353e-x0nfaecdvfrr'
            #             },
            #             'object': {
            #                 'key': 'async-inference/output/e52dabe9-938b-4135-8baa-34af002548f6.out',
            #                 'size': 873,
            #                 'eTag': '52c8bd24d8391a30f3f87b609c973e31',
            #                 'sequencer': '0068D8E3AA906D1928'
            #             }
            #         }
            #     }]
            # }

            # sns message from sagemaker
            # {
            #     'awsRegion': 'us-west-2',
            #     'eventTime': '2025-09-28T07:28:43.333Z',
            #     'receivedTime': '2025-09-28T07:28:43.203Z',
            #     'invocationStatus': 'Completed',
            #     'requestParameters': {
            #         'accept': 'application/json',
            #         'contentType': 'application/json',
            #         'customAttributes': '{}',
            #         'endpointName': 'Endpoint-control-model-3-dice-async',
            #         'inputLocation': 's3://....0928_072843_8af55b3b'
            #     },
            #     'responseParameters': {
            #         'contentType': 'text/html; charset=utf-8',
            #         'outputLocation': 's3://....-8e93-758efa240cdf.out'
            #     },
            #     'inferenceId': '1d2071a5-65c5-40f8-ae06-f61367050695',
            #     'eventVersion': '1.0',
            #     'eventSource': 'aws:sagemaker',
            #     'eventName': 'InferenceResult'
            # }

            # Handle both direct S3 events and SNS-wrapped S3 events
            if "Records" in s3_event_message:
                # Direct S3 event
                records = []  # Turn this off to not get double messages s3_event_message["Records"]
                logger.debug(f"Processing event from S3: {s3_event_message}")
            elif "Message" in s3_event_message:
                # SNS-wrapped S3 event
                message = json.loads(s3_event_message["Message"])
                records = message.get("Records", [])
                logger.debug(f"Processing event from S3->SNS: {s3_event_message}")
            elif "responseParameters" in s3_event_message:
                logger.debug(f"Processing event from SageMaker->SNS: {s3_event_message}")
                return s3_event_message["responseParameters"]["outputLocation"]
            else:
                logger.error(f"Unrecognized S3 event format: {s3_event_message}")
                return ""

            for record in records:
                if "s3" in record:
                    s3_info = record["s3"]
                    bucket_name = s3_info["bucket"]["name"]
                    object_key = s3_info["object"]["key"]

                    # Construct S3 URI
                    output_location = f"s3://{bucket_name}/{object_key}"
                    logger.debug(f"Extracted output location: {output_location}")
                    return output_location

            logger.warning(f"No S3 records found in event: {s3_event_message}")
            return ""

        except Exception as e:
            logger.error(f"Error parsing S3 event: {e}")
            logger.debug(f"S3 event content: {s3_event_message}")
            return ""

    def _get_or_create_tile_request_item(self, tile_request: TileRequest) -> TileRequestItem:
        tile_request_item = self.tile_request_table.get_tile_request(tile_request.tile_id, tile_request.region_id)
        if tile_request_item is None:
            tile_request_item = TileRequestItem.from_tile_request(tile_request)
            self.tile_request_table.start_tile_request(tile_request_item)
        return tile_request_item

    @metric_scope
    def check_if_region_request_complete(self, tile_request_item: TileRequestItem, metrics):

        done, total_tile_count, failed_tile_count = self.tile_request_table.is_region_request_complete(tile_request_item)
        if not done:
            return None

        # region done

        # Get region request and region request item
        # region_request = self.get_region_request(tile_request.tile_id)
        region_request_item = self.region_request_table.get_region_request(
            tile_request_item.region_id, tile_request_item.image_id
        )

        # Update table w/ total tile counts
        region_request_item.total_tiles = total_tile_count
        region_request_item.succeeded_tile_count = total_tile_count - failed_tile_count
        region_request_item.failed_tile_count = failed_tile_count
        region_request_item = self.region_request_table.update_region_request(region_request_item)

        # Update the image request to complete this region
        _ = self.job_table.complete_region_request(tile_request_item.image_id, bool(failed_tile_count))  # image_request_item

        # Update region request table if that region succeeded
        region_status = self.region_status_monitor.get_status(region_request_item)
        region_request_item = self.region_request_table.complete_region_request(region_request_item, region_status)

        self.region_status_monitor.process_event(region_request_item, region_status, "Completed region processing")

        # Write CloudWatch Metrics to the Logs
        if isinstance(metrics, MetricsLogger):
            metrics.put_metric(MetricLabels.INVOCATIONS, 1, str(Unit.COUNT.value))

        return region_request_item
