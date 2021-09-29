
import pytest

import shapely.wkt
import shapely.geometry

from aws_oversightml_model_runner.georeference import GDALAffineCameraModel
from aws_oversightml_model_runner.model_runner import load_gdal_dataset, calculate_processing_bounds

@pytest.fixture
def test_dataset_and_camera():
    ds, camera_model = load_gdal_dataset('./test/data/GeogToWGS84GeoKey5.tif')
    return ds, camera_model


def test_gdal_load_success(test_dataset_and_camera):

    ds = test_dataset_and_camera[0]
    camera_model = test_dataset_and_camera[1]

    assert ds is not None
    assert ds.RasterXSize == 101
    assert ds.RasterYSize == 101

    assert camera_model is not None
    assert isinstance(camera_model, GDALAffineCameraModel)


def test_gdal_load_invalid():

    with pytest.raises(ValueError) as e_info:
        ds, camera_model = load_gdal_dataset('./test/data/does-not-exist.tif')


def test_calculate_processing_bounds_no_roi(test_dataset_and_camera):

    ds = test_dataset_and_camera[0]
    camera_model = test_dataset_and_camera[1]

    processing_bounds = calculate_processing_bounds(None, ds, camera_model)

    assert processing_bounds == ((0, 0), (101, 101))


def test_calculate_processing_bounds_full_image(test_dataset_and_camera):

    ds = test_dataset_and_camera[0]
    camera_model = test_dataset_and_camera[1]
    roi = shapely.wkt.loads("POLYGON ((8 50, 10 50, 10 60, 8 60, 8 50))")

    processing_bounds = calculate_processing_bounds(roi, ds, camera_model)

    assert processing_bounds == ((0, 0), (101, 101))


def test_calculate_processing_bounds_intersect(test_dataset_and_camera):

    ds = test_dataset_and_camera[0]
    camera_model = test_dataset_and_camera[1]
    roi = shapely.wkt.loads("POLYGON ((8 52, 9.001043490711101 52.0013898967889, 9 54, 8 54, 8 52))")

    # Manually verify the lon/lat coordinates of the image positions used in this test with these print statements
    # print(camera_model.image_to_world((0, 0)))
    # print(camera_model.image_to_world((50, 50)))
    # print(camera_model.image_to_world((101, 101)))
    processing_bounds = calculate_processing_bounds(roi, ds, camera_model)

    # Processing bounds is in ((r, c), (w, h))
    assert processing_bounds == ((0, 0), (50, 50))


def test_calculate_processing_bounds_chip(test_dataset_and_camera):

    ds = test_dataset_and_camera[0]
    camera_model = test_dataset_and_camera[1]
    roi = shapely.wkt.loads("POLYGON ((8.999932379599102 52.0023621190119, 8.999932379599102 52.0002787856769, 9.001599046267101 52.0002787856769, 9.001599046267101 52.0023621190119, 8.999932379599102 52.0023621190119))")

    # Manually verify the lon/lat coordinates of the image positions used in this test with these print statements
    # print(camera_model.image_to_world((10, 15)))
    # print(camera_model.image_to_world((70, 90)))
    processing_bounds = calculate_processing_bounds(roi, ds, camera_model)

    # Processing bounds is in ((r, c), (w, h))
    assert processing_bounds == ((15, 10), (60, 75))