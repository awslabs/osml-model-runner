from .async_tile_submission_worker import AsyncSubmissionWorker
from .async_tile_results_worker import AsyncPollingWorker
from ..async_app_config import EnhancedServiceConfig

def setup_polling_tile_workers(
    region_request: RegionRequest,
    sensor_model: Optional[SensorModel] = None,
    elevation_model: Optional[ElevationModel] = None,
) -> Tuple[Queue, List[TileWorker]]:


    # Start polling workers
    for i in range(EnhancedServiceConfig.polling_workers):

        # Set up our feature table to work with the region quest
        feature_table = FeatureTable(
            EnhancedServiceConfig.feature_table,
            region_request.tile_size,
            region_request.tile_overlap,
        )

        # Set up our feature table to work with the region quest
        region_request_table = RegionRequestTable(EnhancedServiceConfig.region_request_table)

        # Ignoring mypy error - if model_name was None the call to validate the region
        # request at the start of this function would have failed
        feature_detector = EnhancedFeatureDetectorFactory(
            endpoint=self.region_request.model_name,
            endpoint_mode=self.region_request.model_invoke_mode,
            assumed_credentials=self.model_invocation_credentials,
        ).build()

        if feature_detector is None:
            logger.error("Failed to create feature detector")
            return None

        # Set up geolocator
        geolocator = None
        if self.sensor_model is not None:
            geolocator = Geolocator(
                ImagedFeaturePropertyAccessor(), self.sensor_model, elevation_model=self.elevation_model
            )
        worker = AsyncPollingWorker(
            worker_id=i,
            feature_table=feature_table,
            geolocator=geolocator,
            region_request_table=region_request_table,
            in_queue=self.job_queue,
            result_queue=self.result_queue,
            feature_detector=feature_detector,
            config=self.config,
            metrics_tracker=self.metrics_tracker,
            tile_table=self.tile_table,
        )
        logger.info("Created poller worker")
        worker.start()
        logger.info("poller worker started")

def setup_async_tile_workers(
    region_request: RegionRequest,
    sensor_model: Optional[SensorModel] = None,
    elevation_model: Optional[ElevationModel] = None,
) -> Tuple[Queue, List[TileWorker]]:
    """
    Sets up a pool of tile-workers to process image tiles from a region request

    :param region_request: RegionRequest = the region request to update.
    :param sensor_model: Optional[SensorModel] = the sensor model for this raster dataset
    :param elevation_model: Optional[ElevationModel] = an elevation model used to fix the elevation of the image coordinate

    :return: Tuple[Queue, List[TileWorker] = a list of tile workers and the queue that manages them
    """
    try:
        model_invocation_credentials = None
        if region_request.model_invocation_role:
            model_invocation_credentials = get_credentials_for_assumed_role(region_request.model_invocation_role)

        # Set up a Queue to manage our tile workers
        tile_queue: Queue = Queue()
        tile_workers = []

        for i in range(int(ServiceConfig.workers)):
            # Set up our feature table to work with the region quest
            feature_table = FeatureTable(
                ServiceConfig.feature_table,
                region_request.tile_size,
                region_request.tile_overlap,
            )

            # Set up our feature table to work with the region quest
            region_request_table = RegionRequestTable(ServiceConfig.region_request_table)

            # Ignoring mypy error - if model_name was None the call to validate the region
            # request at the start of this function would have failed
            feature_detector = EnhancedFeatureDetectorFactory(
                endpoint=region_request.model_name,
                endpoint_mode=region_request.model_invoke_mode,
                assumed_credentials=model_invocation_credentials,
            ).build()

            worker = AsyncSubmissionWorker(
                worker_id=i,
                tile_queue=tile_queue,
                job_queue=self.job_queue,
                feature_detector=feature_detector,
                config=self.config,
                metrics_tracker=self.metrics_tracker,
                tile_table=self.tile_table,
            )


            # geolocator = None
            # if sensor_model is not None:
            #     geolocator = Geolocator(ImagedFeaturePropertyAccessor(), sensor_model, elevation_model=elevation_model)

            # worker = TileWorker(tile_queue, feature_detector, geolocator, feature_table, region_request_table)
            worker.start()
            tile_workers.append(worker)

        logger.debug(f"Setup pool of {len(tile_workers)} tile workers")

        return tile_queue, tile_workers
    except Exception as err:
        logger.exception(f"Failed to setup tile workers!: {err}")
        raise SetupTileWorkersException("Failed to setup tile workers!") from err
