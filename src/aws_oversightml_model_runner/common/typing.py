from enum import auto
from typing import Tuple

from aws_oversightml_model_runner.common import AutoStringEnum

# TODO: Define a Point type so there is no confusion over the meaning of BBox.
#       (i.e. a two corner box would be (Point, Point) while a UL width height box
#       would be (Point, w, h)

# Pixel coordinate (row, column)
ImageCoord = Tuple[int, int]
# 2D shape (w, h)
ImageDimensions = Tuple[int, int]
# UL corner (row, column) , dimensions (w, h)
ImageRegion = Tuple[ImageCoord, ImageDimensions]


class ImageCompression(str, AutoStringEnum):
    """
    Enumeration defining compression algorithms for image.
    """

    NONE = auto()
    JPEG = auto()
    J2K = auto()
    LZW = auto()


class ImageFormats(str, AutoStringEnum):
    """
    Enumeration defining image encodings.
    """

    NITF = auto()
    JPEG = auto()
    PNG = auto()
    GTIFF = auto()


# TODO - https://issues.amazon.com/issues/AnIMaL-24077
class ImageRequestStatus(str, AutoStringEnum):
    STARTED = auto()
    IN_PROGRESS = auto()
    PARTIAL = auto()
    SUCCESS = auto()
    FAILED = auto()


class ModelHostingOptions(str, AutoStringEnum):
    """
    Enumeration defining the hosting options for CV models.
    """

    NONE = auto()
    SM_ENDPOINT = auto()


# These sets are constructed to facilitate easy checking of string values against the enumerations
VALID_IMAGE_COMPRESSION = [item.value for item in ImageCompression]
VALID_IMAGE_FORMATS = [item.value for item in ImageFormats]
VALID_MODEL_HOSTING_OPTIONS = [item.value for item in ModelHostingOptions]
