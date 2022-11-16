from enum import Enum
from typing import Tuple

# TODO: Define a Point type so there is no confusion over the meaning of BBox.
#       (i.e. a two corner box would be (Point, Point) while a UL width height box
#       would be (Point, w, h)

# Pixel coordinate (row, column)
ImageCoord = Tuple[int, int]
# 2D shape (w, h)
ImageDimensions = Tuple[int, int]
# UL corner (row, column) , dimensions (w, h)
ImageRegion = Tuple[ImageCoord, ImageDimensions]


class ImageCompression(str, Enum):
    """
    Enumeration defining compression algorithms for image.
    """

    NONE = "NONE"
    JPEG = "JPEG"
    J2K = "J2K"
    LZW = "LZW"


class ImageFormats(str, Enum):
    """
    Enumeration defining image encodings.
    """

    NITF = "NITF"
    JPEG = "JPEG"
    PNG = "PNG"
    GTIFF = "GTIFF"


# TODO - https://issues.amazon.com/issues/AnIMaL-24077
class ImageRequestStatus(str, Enum):
    STARTED = "STARTED"
    IN_PROGRESS = "IN_PROGRESS"
    PARTIAL = "PARTIAL"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"


class ModelHostingOptions(str, Enum):
    """
    Enumeration defining the hosting options for CV models.
    """

    NONE = "NONE"
    SM_ENDPOINT = "SM_ENDPOINT"


# These sets are constructed to facilitate easy checking of string values against the enumerations
VALID_IMAGE_COMPRESSION = [item.value for item in ImageCompression]
VALID_IMAGE_FORMATS = [item.value for item in ImageFormats]
VALID_MODEL_HOSTING_OPTIONS = [item.value for item in ModelHostingOptions]
