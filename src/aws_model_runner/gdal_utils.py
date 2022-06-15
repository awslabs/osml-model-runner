import logging
from typing import Dict, List, Optional, Tuple

from osgeo import gdal, gdalconst

from .georeference import CameraModel, GDALAffineCameraModel
from .metrics import MetricsContext, now


def set_gdal_default_configuration() -> None:
    """
    This function sets GDAL configuration options to support efficient reading of large raster
    datasets using the /vsis3 virtual file system.
    TODO: Performance analysis to determine ideal values and possibly moving this to a
          non-hardcoded configuration
    """
    # This is the maximum size of a chunk we can fetch at one time from a remote file
    # I couldn't find the value anywhere in the documentation but it is enforced here:
    # https://github.com/OSGeo/gdal/blob/211e2430b8cda486d0e0e68446647f56cc0ca149/port/cpl_vsil_curl.cpp#L161
    max_curl_chunk_size = 10 * 1024 * 1024

    gdal_default_environment_options = {
        "GDAL_DISABLE_READDIR_ON_OPEN": "EMPTY_DIR",
        # This flag will setup verbose output for GDAL. In particular it will show you each range
        # read for the file if using the /vsis3 virtual file system.
        # 'CPL_DEBUG': 'ON',
        "CPL_VSIL_CURL_CHUNK_SIZE": str(max_curl_chunk_size),
        "CPL_VSIL_CURL_CACHE_SIZE": str(max_curl_chunk_size * 100),
    }
    for key, val in gdal_default_environment_options.items():
        gdal.SetConfigOption(key, str(val))
    logging.info("Set GDAL Configuration Options: {}".format(gdal_default_environment_options))


class GDALConfigEnv:
    """
    This class provides a way to setup a temporary GDAL environment using Python's "with"
    statement. GDAL configuration options will be set inside the scope of the with statement and
    then reverted to previously set values on exit. This will commonly be used to set AWS security
    credentials (e.g. AWS_SECRET_ACCESS_KEY) for use by other GDAL operations.

    See: https://gdal.org/user/configoptions.html#gdal-configuration-file for additional options.
    """

    def __init__(self, options=None):
        if options:
            self.options = options.copy()
        else:
            self.options = {}
        self.old_options = {}
        pass

    def with_aws_credentials(self, aws_credentials: Optional[Dict[str, str]]) -> "GDALConfigEnv":
        """
        This method sets the GDAL configuration options for the AWS credentials from the
        credentials object returned by a boto3 call to sts.assume_role(...).

        :param aws_credentials: The dictionary of values from the sts.assume_role()
                                response['Credentials']
        :return: self to facilitate a simple builder constructor pattern
        """
        if aws_credentials is not None:
            self.options.update(
                {
                    "AWS_SECRET_ACCESS_KEY": aws_credentials["SecretAccessKey"],
                    "AWS_ACCESS_KEY_ID": aws_credentials["AccessKeyId"],
                    "AWS_SESSION_TOKEN": aws_credentials["SessionToken"],
                }
            )
        return self

    def __enter__(self):
        for key, val in self.options.items():
            self.old_options[key] = gdal.GetConfigOption(key)
            gdal.SetConfigOption(key, str(val))

    def __exit__(self, exc_type, exc_val, exc_traceback):
        for key, val in self.options.items():
            gdal.SetConfigOption(key, self.old_options[key])


def load_gdal_dataset(
    image_path: str, metrics: MetricsContext = None
) -> Tuple[gdal.Dataset, Optional[CameraModel]]:
    """
    This function loads a GDAL raster dataset from the path provided and constructs a camera model
    abstraction used to georeference locations on this image.

    :param image_path: The path to the raster data, may be a local path or a virtual file system
                        (e.g. /vsis3/...)
    :param metrics: Optional metrics instrumentation that will record time required to access
                    image and metadata
    :return: the raster dataset and camera model
    """
    # Use GDAL to open the image object in S3. Note that we're using GDALs s3 driver to
    # read directly from the object store as needed to complete the image operations
    metadata_start_time = now()
    ds = gdal.Open(image_path)
    if ds is None:
        logging.info("Skipping: %s - GDAL Unable to Process", image_path)
        raise ValueError("GDAL Unable to Load: {}".format(image_path))

    camera_model = None
    transform = ds.GetGeoTransform()
    if transform:
        camera_model = GDALAffineCameraModel(transform)

    logging.info("GDAL Parsed Image of size: %d x %d", ds.RasterXSize, ds.RasterYSize)
    metadata_end_time = now()
    if metrics is not None:
        metrics.put_metric(
            "MetadataLatency", (metadata_end_time - metadata_start_time), "Microseconds"
        )

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
            logging.warning(
                "Image uses unsupported GDAL datatype {}. Defaulting to [0,255] range".format(
                    output_type
                )
            )

        scale_params.append([min, max, min, max])

    return output_type, scale_params
