#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
import traceback

from aws.osml.gdal import set_gdal_default_configuration, load_gdal_dataset
from aws.osml.model_runner import ModelRunner
from aws.osml.model_runner.tile_worker import TilingStrategy, VariableOverlapTilingStrategy
from aws.osml.model_runner.queue import RequestQueue
from aws.osml.model_runner.common import ThreadingLocalContextFilter

from osml_extensions.registry import DependencyInjectionError, HandlerSelectionError, HandlerSelector

from .async_app_config import AsyncServiceConfig
from .api import TileRequest
from .database import TileRequestItem, TileRequestTable
from .enhanced_tile_handler import TileRequestHandler
from .status import TileStatusMonitor
from .errors import SelfThrottledTileException, RetryableJobException

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
        Set up enhanced components based on configuration and dependency injection.

        :return: None
        """
        try:

            # TODO: The registry mechanism is now broken. Update to get the handlers from the registry itself.

            self.tile_request_queue = RequestQueue(AsyncServiceConfig.tile_queue, wait_seconds=0)
            self.tile_requests_iter = iter(self.tile_request_queue)

            self.tile_request_table = TileRequestTable(AsyncServiceConfig.tile_request_table)
            self.tile_status_monitor = TileStatusMonitor(AsyncServiceConfig.tile_status_topic)

            # Initialize handler selector and dependency injector
            handler_selector = HandlerSelector()  # TODO: REMOVE THIS AND JUST USE THE ASYNC HANDLERS

            # Determine request type from environment or configuration
            request_type = AsyncServiceConfig.request_type  # ['sm_endpoint', 'async_sm_endpoint']

            logger.debug(f"Setting up components for request_type='{request_type}'")

            region_handler_metadata, image_handler_metadata = handler_selector.select_handlers(request_type)

            # Create image request handler
            # TODO: For now this assumes all image handlers and all region handlers
            # have the same class signature, update this so this is configurable.
            image_handler_args = []
            image_handler_kwargs = dict(
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

            region_handler_args = []
            region_handler_kwargs = dict(
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

            self.image_request_handler = image_handler_metadata.handler_class(*image_handler_args, **image_handler_kwargs)
            self.region_request_handler = region_handler_metadata.handler_class(
                *region_handler_args, **region_handler_kwargs
            )
            self.tile_request_handler = TileRequestHandler(
                tile_request_table=self.tile_request_table, 
                job_table=self.job_table, 
                tile_status_monitor=self.tile_status_monitor
            )

            logger.debug(
                f"Successfully configured handlers: region='{region_handler_metadata.name}', "
                f"image='{image_handler_metadata.name}'"
            )

        except (HandlerSelectionError, DependencyInjectionError) as e:
            logger.error(f"Failed to set up enhanced components: {e}")
            if (
                hasattr(AsyncServiceConfig, "extension_fallback_enabled")
                and not AsyncServiceConfig.extension_fallback_enabled
            ):
                raise

        except Exception as e:
            logger.error(f"Unexpected error setting up enhanced components: {e}")
            logger.debug(f"Traceback: {traceback.format_exc()}")
            if (
                hasattr(AsyncServiceConfig, "extension_fallback_enabled")
                and not AsyncServiceConfig.extension_fallback_enabled
            ):
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
            receipt_handle, tile_request_attributes = next(self.tile_requests_iter)
        except StopIteration:
            # No tiles to process
            logger.debug("No tiles requests available to process")
            return False

        if tile_request_attributes:
            ThreadingLocalContextFilter.set_context(tile_request_attributes)
            try:
                tile_request = TileRequest(tile_request_attributes)
                tile_request_item = self._get_or_create_tile_request_item(tile_request)
                region_request, region_request_item, image_request, image_request_item = (
                    self.tile_request_handler.process_tile_request(tile_request, tile_request_item)
                )

                # check if the region is done
                if self.job_table.is_region_request_complete(region_request_item):
                    self.region_request_handler.complete_region_request(tile_request)

                # Check if the whole image is done
                if self.job_table.is_image_request_complete(image_request_item):

                    raster_dataset, sensor_model = load_gdal_dataset(tile_request.image_path)

                    self.image_request_handler.complete_image_request(
                        region_request, str(raster_dataset.GetDriver().ShortName).upper(), raster_dataset, sensor_model
                    )
                # finish the current request
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

    def _get_or_create_tile_request_item(self, tile_request: TileRequest) -> TileRequestItem:
        tile_request_item = self.tile_request_table.get_tile_request(tile_request.tile_id)
        if tile_request_item is None:
            tile_request_item = TileRequestItem.from_tile_request(tile_request)
            self.tile_request_table.start_tile_request(tile_request_item)
        return tile_request_item
