from typing import Any, Dict

import shapely.geometry
import shapely.wkt

from aws_oversightml_model_runner.utils.image_helper import ImageFormats
from aws_oversightml_model_runner.utils.request_helper import shared_properties_are_valid


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
        self.job_id = None
        self.job_arn = None
        self.image_id = None
        self.image_url = None
        self.output_bucket = None
        self.output_prefix = None
        self.model_name = None
        self.model_hosting_type = None
        self.tile_size = (1024, 1024)
        self.tile_overlap = (50, 50)
        self.tile_format = ImageFormats.NITF
        self.tile_compression = None
        self.execution_role = None
        self.roi = None

        for dictionary in initial_data:
            for key in dictionary:
                setattr(self, key, dictionary[key])
        for key in kwargs:
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
        properties = {}
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

        if "executionRole" in image_request:
            properties["execution_role"] = image_request["executionRole"]

        # TODO: Consider possible support multiple images in a single request. Some images are
        #       pre-tiled in S3 so customers may want to submit a single logical request for
        # multiple images.
        properties["image_url"] = image_request["imageUrls"][0]
        properties["image_id"] = image_request["jobId"] + ":" + properties["image_url"]
        properties["output_bucket"] = image_request["outputBucket"]
        properties["output_prefix"] = image_request["outputPrefix"]
        properties["model_name"] = image_request["imageProcessor"]["name"]
        properties["model_hosting_type"] = image_request["imageProcessor"]["type"]
        if "regionOfInterest" in image_request:
            properties["roi"] = shapely.wkt.loads(image_request["regionOfInterest"])

        return ImageRequest(properties)

    def is_valid(self) -> bool:
        """
        Check to see if this request contains required attributes and meaningful values

        :return: True if the request contains all the mandatory attributes with acceptable values,
                 False otherwise
        """
        if not shared_properties_are_valid(self):
            return False

        if not self.job_arn:
            return False

        if not self.job_id:
            return False

        return True
