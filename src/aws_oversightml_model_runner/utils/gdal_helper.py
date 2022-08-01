import logging
from typing import List, Optional, Tuple

from osgeo import gdal, gdalconst

from aws_oversightml_model_runner.classes.camera_model import CameraModel, GDALAffineCameraModel

logger = logging.getLogger(__name__)


def load_gdal_dataset(image_path: str) -> Tuple[gdal.Dataset, Optional[CameraModel]]:
    """
    This function loads a GDAL raster dataset from the path provided and constructs a camera model
    abstraction used to georeference locations on this image.

    :param image_path: The path to the raster data, may be a local path or a virtual file system
                        (e.g. /vsis3/...)
    :param metrics: Optional metrics instrumentation that will record time required to access
                    image and metadata
    :return: the raster dataset and camera model
    """
    ds = gdal.Open(image_path)
    if ds is None:
        logger.info("Skipping: %s - GDAL Unable to Process", image_path)
        raise ValueError("GDAL Unable to Load: {}".format(image_path))

    camera_model = None
    transform = ds.GetGeoTransform()
    if transform:
        camera_model = GDALAffineCameraModel(transform)

    logger.info("GDAL Parsed Image of size: %d x %d", ds.RasterXSize, ds.RasterYSize)

    return ds, camera_model


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
