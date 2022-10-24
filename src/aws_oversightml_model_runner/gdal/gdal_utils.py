import logging
import re
from typing import Dict, List, Optional, Tuple
from xml.etree import ElementTree

from osgeo import gdal, gdalconst

from aws_oversightml_model_runner.photogrammetry import SensorModel

from .sensor_model_factory import SensorModelFactory

logger = logging.getLogger(__name__)


def load_gdal_dataset(image_path: str) -> Tuple[gdal.Dataset, Optional[SensorModel]]:
    """
    This function loads a GDAL raster dataset from the path provided and constructs a camera model
    abstraction used to georeference locations on this image.

    :param image_path: The path to the raster data, may be a local path or a virtual file system
                        (e.g. /vsis3/...)
    :return: the raster dataset and sensor model
    """
    ds = gdal.Open(image_path)
    logger.info("GDAL attempted to load image: %s", image_path)
    if ds is None:
        logger.info("Skipping: %s - GDAL Unable to Process", image_path)
        raise ValueError("GDAL Unable to Load: {}".format(image_path))

    # Get a GDAL Geo Transform or approximate it from any ground control points available
    geo_transform = ds.GetGeoTransform(can_return_null=True)
    if geo_transform is None:
        ground_control_points = ds.GetGCPs()
        if ground_control_points is not None:
            geo_transform = gdal.GCPsToGeoTransform(ground_control_points)

    # If this image has NITF TREs defined parse them
    parsed_tres = None
    xml_tres = ds.GetMetadata("xml:TRE")
    if xml_tres is not None and len(xml_tres) > 0:
        parsed_tres = ElementTree.fromstring(xml_tres[0])

    # Create the best sensor model available
    sensor_model = SensorModelFactory(parsed_tres, geo_transform).build()

    return ds, sensor_model


def get_type_and_scales(raster_dataset: gdal.Dataset) -> Tuple[int, List[List[int]]]:
    scale_params = []
    num_bands = raster_dataset.RasterCount
    output_type = gdalconst.GDT_Byte
    min = 0
    max = 255
    for band_num in range(1, num_bands + 1):
        band = raster_dataset.GetRasterBand(band_num)
        output_type = band.DataType
        if output_type == gdalconst.GDT_Byte:
            min = 0
            max = 255
        elif output_type == gdalconst.GDT_UInt16:
            min = 0
            max = 65535
        elif output_type == gdalconst.GDT_Int16:
            min = -32768
            max = 32767
        elif output_type == gdalconst.GDT_UInt32:
            min = 0
            max = 4294967295
        elif output_type == gdalconst.GDT_Int32:
            min = -2147483648
            max = 2147483647
        else:
            logger.warning(
                "Image uses unsupported GDAL datatype {}. Defaulting to [0,255] range".format(
                    output_type
                )
            )

        scale_params.append([min, max, min, max])

    return output_type, scale_params


def get_image_extension(image_path: str) -> str:
    possible_extensions = get_extensions_from_driver(image_path)
    selected_extension = select_extension(image_path, possible_extensions)
    image_extension = normalize_extension(selected_extension)
    logger.info("Image extension: {}".format(image_extension))
    return image_extension


def select_extension(image_path: str, possible_extensions: List[str]) -> str:
    selected_extension = "UNKNOWN"
    for i, possible_extension in enumerate(possible_extensions):
        if i == 0:
            selected_extension = possible_extension.upper()
        elif f".{possible_extension}".upper() in image_path.upper():
            selected_extension = possible_extension.upper()
    return selected_extension


def normalize_extension(unnormalized_extension: str) -> str:
    normalized_extension = unnormalized_extension.upper()
    if re.search(r"ni?tf", normalized_extension, re.IGNORECASE):
        normalized_extension = "NITF"
    elif re.search(r"tif{1,2}", normalized_extension, re.IGNORECASE):
        normalized_extension = "TIFF"
    elif re.search(r"jpe?g", normalized_extension, re.IGNORECASE):
        normalized_extension = "JPEG"
    return normalized_extension


def get_extensions_from_driver(image_path: str) -> List[str]:
    driver_extension_lookup = get_gdal_driver_extensions()
    info = gdal.Info(image_path, format="json")
    driver_long_name = info.get("driverLongName")
    return driver_extension_lookup.get(driver_long_name, [])


def get_gdal_driver_extensions() -> Dict[str, List]:
    driver_lookup = {}
    for i in range(gdal.GetDriverCount()):
        drv = gdal.GetDriver(i)
        driver_name = drv.GetMetadataItem(gdal.DMD_LONGNAME)
        driver_extensions = drv.GetMetadataItem(gdal.DMD_EXTENSIONS)
        if driver_extensions:
            extension_list = driver_extensions.strip().split(" ")
            driver_lookup[driver_name] = extension_list
    return driver_lookup
