from enum import Enum
from typing import Any, Dict, Union

import shapely.geometry
import shapely.wkt

from aws_oversightml_model_runner.image_utils import ImageDimensions


class ModelHostingOptions(str, Enum):
    """
    Enumeration defining the hosting options for CV models.
    """

    SM_ENDPOINT = "SM_ENDPOINT"


class TileFormats(str, Enum):
    """
    Enumeration defining supported encodings for tiles sent to CV models.
    """

    NITF = "NITF"
    JPEG = "JPEG"
    PNG = "PNG"
    GEOTIFF = "GEOTIFF"


class TileCompression(str, Enum):
    """
    Enumeration defining supported compression algorithms for tiles sent to CV models.
    """

    NONE = "NONE"
    JPEG = "JPEG"
    J2K = "J2K"


# These sets are constructed to facilitate easy checking of string values against the
# enumerations
VALID_MODEL_HOSTING_OPTIONS = set(item.value for item in ModelHostingOptions)
VALID_TILE_FORMATS = set(item.value for item in TileFormats)
VALID_TILE_COMPRESSION = set(item.value for item in TileCompression)


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
        self.tile_format = TileFormats.NITF
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


class RegionRequest(object):
    """
    Request for the Model Runner to process a region of an image.

    This class contains the attributes that make up a region processing request along with
    constructors used to create these requests from common constructs.
    """

    def __init__(self, *initial_data: Dict[str, Any], **kwargs: Any):
        """
        This constructor allows users to create these objects using a combination of dictionaries
        and keyword arguments.

        :param initial_data: dictionaries that contain attributes/values that map to this class's
                             attributes
        :param kwargs: keyword arguments provided on the constructor to set specific attributes
        """
        self.image_id = None
        self.image_url = None
        self.output_bucket = None
        self.output_prefix = None
        self.model_name = None
        self.model_hosting_type = None
        self.tile_size: ImageDimensions = (1024, 1024)
        self.tile_overlap: ImageDimensions = (50, 50)
        self.tile_format = TileFormats.NITF
        self.tile_compression = None
        # Bounds are: UL corner (row, column) , dimensions (w, h)
        self.region_bounds = None
        self.execution_role = None

        for dictionary in initial_data:
            for key in dictionary:
                setattr(self, key, dictionary[key])
        for key in kwargs:
            setattr(self, key, kwargs[key])

    def is_valid(self) -> bool:
        """
        Check to see if this request contains required attributes and meaningful values

        :return: True if the request contains all the mandatory attributes with acceptable values,
                 False otherwise
        """
        if not shared_properties_are_valid(self):
            return False

        if not self.region_bounds:
            return False

        return True


def shared_properties_are_valid(request: Union[ImageRequest, RegionRequest]) -> bool:
    """
    There are some attributes that are shared between ImageRequests and RegionRequests. This
    function exists to
    :param request:
    :return:
    """
    if not request.image_id or not request.image_url:
        return False

    if not request.output_bucket or not request.output_prefix:
        return False

    if not request.model_name:
        return False

    if (
        not request.model_hosting_type
        or request.model_hosting_type not in VALID_MODEL_HOSTING_OPTIONS
    ):
        return False

    if not request.tile_size or len(request.tile_size) != 2:
        return False

    if request.tile_size[0] <= 0 or request.tile_size[1] <= 0:
        return False

    if not request.tile_overlap or len(request.tile_overlap) != 2:
        return False

    if (
        request.tile_overlap[0] < 0
        or request.tile_overlap[0] >= request.tile_size[0]
        or request.tile_overlap[1] < 0
        or request.tile_overlap[1] >= request.tile_size[1]
    ):
        return False

    if not request.tile_format or request.tile_format not in VALID_TILE_FORMATS:
        return False

    if request.tile_compression and request.tile_compression not in VALID_TILE_COMPRESSION:
        return False

    if request.execution_role and not request.execution_role.startswith("arn:"):
        return False

    return True
