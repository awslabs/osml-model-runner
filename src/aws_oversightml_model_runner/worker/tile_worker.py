import asyncio
import logging
from queue import Queue
from threading import Thread
from typing import Dict

from aws_embedded_metrics.logger.metrics_logger import MetricsLogger
from aws_embedded_metrics.unit import Unit

from aws_oversightml_model_runner.app_config import MetricLabels
from aws_oversightml_model_runner.ddb.feature_table import FeatureTable
from aws_oversightml_model_runner.worker.feature_detector import FeatureDetector


class TileWorker(Thread):
    def __init__(
        self,
        in_queue: Queue,
        feature_detector: FeatureDetector,
        feature_table: FeatureTable,
        event_loop: asyncio.AbstractEventLoop,
        metrics: MetricsLogger,
    ) -> None:
        super().__init__()
        self.in_queue = in_queue
        self.feature_detector = feature_detector
        self.feature_table = feature_table
        self.loop = event_loop
        self.metrics = metrics

    def run(self) -> None:
        asyncio.set_event_loop(self.loop)
        while True:
            image_info: Dict = self.in_queue.get()

            if image_info is None:
                logging.info("All images processed. Stopping tile worker.")
                logging.info(
                    "Feature Detector Stats: {} requests with {} errors".format(
                        self.feature_detector.request_count, self.feature_detector.error_count
                    )
                )
                break

            try:
                logging.info("Invoking SM Endpoint")
                with open(image_info["image_path"], mode="rb") as payload:
                    feature_collection = self.feature_detector.find_features(payload)

                # Convert the features to reference the full image
                features = []
                ulx = image_info["region"][0][1]
                uly = image_info["region"][0][0]
                if isinstance(feature_collection, dict) and "features" in feature_collection:
                    logging.info(
                        "SM Model returned {} features".format(len(feature_collection["features"]))
                    )
                    for feature in feature_collection["features"]:
                        tile_bbox = feature["properties"]["bounds_imcoords"]
                        full_image_bbox = [
                            tile_bbox[0] + ulx,
                            tile_bbox[1] + uly,
                            tile_bbox[2] + ulx,
                            tile_bbox[3] + uly,
                        ]

                        feature["properties"]["bounds_imcoords"] = full_image_bbox
                        feature["properties"]["image_id"] = image_info["image_id"]
                        features.append(feature)

                logging.info("# Features Created: {}".format(len(features)))
                if len(features) > 0:
                    self.feature_table.add_features(features)
            except Exception as e:
                logging.error("Failed to process region tile!")
                logging.exception(e)
                if self.metrics:
                    self.metrics.put_metric(MetricLabels.TILE_PROCESSING_ERROR, 1, Unit.COUNT.value)
                    self.metrics.put_metric(MetricLabels.TILE_CREATION_FAILURE, 1, Unit.COUNT.value)
            finally:
                self.in_queue.task_done()
