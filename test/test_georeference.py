import pytest

from aws_oversightml_model_runner.georeference import CameraModel, GDALAffineCameraModel

def test_gdal_cameramodel():

    # Test coordinate calculations using geotransform matrix from sample SpaceNet RIO image
    transform = (-43.681640625, 4.487879136029412e-06, 0.0, -22.939453125, 0.0, -4.487879136029412e-06)
    camera_model: CameraModel = GDALAffineCameraModel(transform)

    ul_corner = camera_model.image_to_world((0,0))
    lr_corner = camera_model.image_to_workd(19584, 19584)

    assert ul_corner == (-22.939453125, -43.681640625)
    assert lr_corner == (-23.02734375, -43.59375)