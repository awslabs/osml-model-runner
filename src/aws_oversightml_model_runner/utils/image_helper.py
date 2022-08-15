from enum import Enum
from typing import Any, Dict, Iterator, Tuple

from osgeo import gdal

from aws_oversightml_model_runner.utils.gdal_helper import get_type_and_scales

# TODO: Define a Point type so there is no confusion over the meaning of BBox.
#       (i.e. a two corner box would be (Point, Point) while a UL width height box
#       would be (Point, w, h)

# Pixel coordinate (row, column)
ImageCoord = Tuple[int, int]
# 2D shape (w, h)
ImageDimensions = Tuple[int, int]
# UL corner (row, column) , dimensions (w, h)
ImageRegion = Tuple[ImageCoord, ImageDimensions]
ImageKey = str


class ImageCompression(str, Enum):
    """
    Enumeration defining compression algorithms for image.
    """

    NONE = "NONE"
    JPEG = "JPEG"
    J2K = "J2K"


class ImageFormats(str, Enum):
    """
    Enumeration defining image encodings.
    """

    NITF = "NITF"
    JPEG = "JPEG"
    PNG = "PNG"
    GEOTIFF = "GEOTIFF"


# These sets are constructed to facilitate easy checking of string values against the enumerations
VALID_IMAGE_COMPRESSION = [item.value for item in ImageCompression]
VALID_IMAGE_FORMATS = [item.value for item in ImageFormats]


def ceildiv(a: int, b: int) -> int:
    """
    Integer ceiling division

    :param a: numerator
    :param b: denominator
    :return: ceil(a/b)
    """
    return -(-a // b)


def next_greater_multiple(n: int, m: int):
    """
    Return the minimum value that is greater than or equal to n that is evenly divisible by m.

    :param n: the input value
    :param m: the multiple
    :return: the minimum multiple of m greater than n
    """
    if n % m == 0:
        return n

    return n + (m - n % m)


def next_greater_power_of_two(n: int):
    """
    Returns the number that is both a power of 2 and greater than or equal to the input parameter.
    For example input 100 returns 128.

    :param n: the input integer
    :return: power of 2 greater than or equal to input
    """

    count = 0

    # First n in the below condition is for the case where n is 0
    # Second condition is only true if n is already a power of 2
    if n and not (n & (n - 1)):
        return n

    while n != 0:
        n >>= 1
        count += 1

    return 1 << count


def get_image_type(image_url) -> str:
    """
    Returns the type of image based on the file extension.

    :param image_url: the url of the image
    :return: image type
    """

    split = image_url.rsplit(".", 1)
    if len(split) == 2:
        upper_type = split[1].upper()
        if upper_type == "NTF" or upper_type == "NITF":
            upper_type = "NITF"
        elif upper_type == "TIF" or upper_type == "TIFF":
            upper_type = "TIFF"
        return upper_type
    return "UNKNOWN"


def generate_crops_for_region(
    region: ImageRegion, chip_size: ImageDimensions, overlap: ImageDimensions
) -> Iterator[ImageRegion]:
    """
    Yields a list of overlapping chip bounding boxes for the given region. Chips will start
    in the upper left corner of the region (i.e. region[0][0], region[0][1]) and will be spaced
    such that they have the specified horizontal and vertical overlap.

    :param region: a tuple for the bounding box of the region ((ul_r, ul_c), (width, height))
    :param chip_size: a tuple for the chip dimensions (width, height)
    :param overlap:  a tuple for the overlap (width, height)
    :return: an iterable list of tuples for the chip bounding boxes [((ul_r, ul_c), (w, h)), ...]
    """
    if overlap[0] >= chip_size[0] or overlap[1] >= chip_size[1]:
        raise ValueError(
            "Overlap must be less than chip size! chip_size = "
            + str(chip_size)
            + " overlap = "
            + str(overlap)
        )

    # Calculate the spacing for the chips taking into account the horizontal and vertical overlap
    # and how many are needed to cover the region
    stride_x = chip_size[0] - overlap[0]
    stride_y = chip_size[1] - overlap[1]
    num_x = ceildiv(region[1][0], stride_x)
    num_y = ceildiv(region[1][1], stride_y)

    for r in range(0, num_y):
        for c in range(0, num_x):
            # Calculate the bounds of the chip ensuring that the chip does not extend
            # beyond the edge of the requested region
            ul_x = region[0][1] + c * stride_x
            ul_y = region[0][0] + r * stride_y
            w = min(chip_size[0], (region[0][1] + region[1][0]) - ul_x)
            h = min(chip_size[1], (region[0][0] + region[1][1]) - ul_y)
            if w > overlap[0] and h > overlap[1]:
                yield ((ul_y, ul_x), (w, h))


def create_gdal_translate_kwargs(
    image_format: ImageFormats, image_compression: ImageCompression, raster_dataset: gdal.Dataset
) -> Dict[str, Any]:
    """
    This function creates a set of keyword arguments suitable for passing to the gdal.Translate
    function. The values for these options are derived from the region processing request and
    the raster dataset itself.

    See: https://gdal.org/python/osgeo.gdal-module.html#Translate
    See: https://gdal.org/python/osgeo.gdal-module.html#TranslateOptions

    :param image_format: the format of the input image
    :param image_compression: the compression used on the input image
    :param raster_dataset: the raster dataset to translate
    :return: the dictionary of translate keyword arguments
    """
    # Figure out what type of image this is and calculate a scale that does not force any range
    # remapping
    # TODO: Consider adding an option to have this driver perform the DRA. That option would change
    #       the scale_params output by this calculation
    output_type, scale_params = get_type_and_scales(raster_dataset)

    gdal_translate_kwargs = {
        "scaleParams": scale_params,
        "outputType": output_type,
        "format": image_format,
    }

    creation_options = ""
    if image_format == ImageFormats.NITF:
        # Creation options specific to the NITF raster driver.
        # See: https://gdal.org/drivers/raster/nitf.html
        if image_compression is None:
            # Default NITF tiles to JPEG2000 compression if not otherwise specified
            creation_options += "IC=C8"
        elif image_compression == ImageCompression.J2K:
            creation_options += "IC=C8"
        elif image_compression == ImageCompression.JPEG:
            creation_options += "IC=C3"
        elif image_compression == ImageCompression.NONE:
            creation_options += "IC=NC"

    # TODO: Expand this to offer support for compression using other file formats

    if len(creation_options) > 0:
        gdal_translate_kwargs["creationOptions"] = creation_options

    return gdal_translate_kwargs
