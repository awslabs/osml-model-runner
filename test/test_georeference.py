import pytest

from aws_oversightml_model_runner.georeference import CameraModel, GDALAffineCameraModel

def test_gdal_cameramodel():

    # Test coordinate calculations using geotransform matrix from sample SpaceNet RIO image
    transform = (-43.681640625, 4.487879136029412e-06, 0.0, -22.939453125, 0.0, -4.487879136029412e-06)
    camera_model: CameraModel = GDALAffineCameraModel(transform)

    ul_corner_xy = (0,0)
    lr_corner_xy = (19584, 19584)
    ul_corner_lonlat = (-43.681640625, -22.939453125)
    lr_corner_lonlat = (-43.59375, -23.02734375)

    assert pytest.approx(ul_corner_lonlat, rel=1e-6, abs=1e-6) == camera_model.image_to_world(ul_corner_xy)
    assert pytest.approx(lr_corner_lonlat, rel=1e-6, abs=1e-6) == camera_model.image_to_world(lr_corner_xy)
    assert pytest.approx(ul_corner_xy, rel=1e-6, abs=1e-6) == camera_model.world_to_image(ul_corner_lonlat)
    assert pytest.approx(lr_corner_xy, rel=1e-6, abs=1e-6) == camera_model.world_to_image(lr_corner_lonlat)