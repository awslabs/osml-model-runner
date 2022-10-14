import logging
from typing import Any, Dict, List

import shapely.geometry
import shapely.wkt
from shapely.geometry.base import BaseGeometry

from aws_oversightml_model_runner.sinks import Sink
from aws_oversightml_model_runner.sinks.kinesis_sink import KinesisSink
from aws_oversightml_model_runner.sinks.s3_sink import S3Sink
from aws_oversightml_model_runner.sqs.request_utils import (
    ModelHostingOptions,
    shared_properties_are_valid,
)
from aws_oversightml_model_runner.worker.image_utils import (
    ImageCompression,
    ImageDimensions,
    ImageFormats,
)

logger = logging.getLogger(__name__)


class ImageRequest(object):
    """
    Request for the Model Runner to process an image.

    This class contains the attributes that make up an image processing request along with
    constructors and factory methods used to create these requests from common constructs.
    """

    def __init__(self, *initial_data: Dict[str, Any], **kwargs: Any):
        """
        This constructor allows users to create these objects using a combination of dictionaries
        and keyword arguments.

        :param initial_data: dictionaries that contain attributes/values that map to this class's
                             attributes
        :param kwargs: keyword arguments provided on the constructor to set specific attributes
        """
        self.job_id: str = ""
        self.job_arn: str = ""
        self.image_id: str = ""
        self.image_url: str = ""
        self.image_read_role: str = ""
        self.outputs: List[Sink] = []
        self.model_name: str = ""
        self.model_hosting_type: ModelHostingOptions = ModelHostingOptions.NONE
        self.tile_size: ImageDimensions = (1024, 1024)
        self.tile_overlap: ImageDimensions = (50, 50)
        self.tile_format: ImageFormats = ImageFormats.NITF
        self.tile_compression: ImageCompression = ImageCompression.NONE
        self.model_invocation_role: str = ""
        self.roi: BaseGeometry = None

        for dictionary in initial_data:
            for key in dictionary:
                if key == "outputs":
                    setattr(self, key, ImageRequest.parse_destinations(dictionary[key]))
                else:
                    setattr(self, key, dictionary[key])
        for key in kwargs:
            if key == "outputs":
                setattr(self, key, ImageRequest.parse_destinations(kwargs[key]))
            else:
                setattr(self, key, kwargs[key])

    @staticmethod
    def from_external_message(image_request: Dict[str, Any]):
        """
        This method is used to construct an ImageRequest given a dictionary reconstructed from the
        JSON representation of a request that appears on the Image Job Queue. The structure of
        that message is generally governed by AWS API best practices and may evolve over time as
        the public APIs for this service mature.

        :param image_request: dictionary of values from the decoded JSON request
        :return: the ImageRequest
        """
        properties: Dict[str, Any] = {}
        if "imageProcessorTileSize" in image_request:
            tile_dimension = int(image_request["imageProcessorTileSize"])
            properties["tile_size"] = (tile_dimension, tile_dimension)

        if "imageProcessorTileOverlap" in image_request:
            overlap_dimension = int(image_request["imageProcessorTileOverlap"])
            properties["tile_overlap"] = (overlap_dimension, overlap_dimension)

        if "imageProcessorTileFormat" in image_request:
            properties["tile_format"] = image_request["imageProcessorTileFormat"]

        if "imageProcessorTileCompression" in image_request:
            properties["tile_compression"] = image_request["imageProcessorTileCompression"]

        properties["job_arn"] = image_request["jobArn"]
        properties["job_id"] = image_request["jobId"]

        # TODO: Consider possible support for multiple images in a single request. Some images
        #       are pre-tiled in S3 so customers may want to submit a single logical request
        #       for multiple images.
        properties["image_url"] = image_request["imageUrls"][0]
        properties["image_id"] = image_request["jobId"] + ":" + properties["image_url"]
        if "imageReadRole" in image_request:
            properties["image_read_role"] = image_request["imageReadRole"]

        properties["model_name"] = image_request["imageProcessor"]["name"]
        properties["model_hosting_type"] = image_request["imageProcessor"]["type"]
        if "assumedRole" in image_request["imageProcessor"]:
            properties["model_invocation_role"] = image_request["imageProcessor"]["assumedRole"]

        if "regionOfInterest" in image_request:
            properties["roi"] = shapely.wkt.loads(image_request["regionOfInterest"])

        # Support explicit outputs
        if image_request.get("outputs"):
            properties["outputs"] = image_request["outputs"]
        # Support leegacy image request
        elif image_request.get("outputBucket") and image_request.get("outputPrefix"):
            properties["outputs"] = [
                {
                    "type": S3Sink.name(),
                    "bucket": image_request["outputBucket"],
                    "prefix": image_request["outputPrefix"],
                }
            ]

        return ImageRequest(properties)

    @staticmethod
    def parse_destinations(destinations: List[Dict[str, Any]]) -> List[Sink]:
        outputs: List[Sink] = []
        for destination in destinations:
            sink_type = destination["type"]
            if sink_type == S3Sink.name():
                outputs.append(
                    S3Sink(
                        destination["bucket"], destination["prefix"], destination.get("assumedRole")
                    )
                )
            elif sink_type == KinesisSink.name():
                outputs.append(
                    KinesisSink(
                        destination["stream"],
                        destination["batchSize"],
                        destination.get("assumedRole"),
                    )
                )
            else:
                logger.error(
                    f"Invalid Image Request! Unrecognized output destination specified, '{sink_type}'"
                )
                raise ValueError("Invalid Image Request")
        return outputs

    def is_valid(self) -> bool:
        """
        Check to see if this request contains required attributes and meaningful values

        :return: True if the request contains all the mandatory attributes with acceptable values,
                 False otherwise
        """
        if not shared_properties_are_valid(self):
            return False

        if not self.job_arn or not self.job_id or not self.outputs:
            return False

        return True
