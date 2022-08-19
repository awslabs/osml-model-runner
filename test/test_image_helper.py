import pytest

from aws_oversightml_model_runner.utils.image_helper import (
    ImageCompression,
    ImageFormats,
    create_gdal_translate_kwargs,
    generate_crops_for_region,
    get_image_type,
    next_greater_multiple,
    next_greater_power_of_two,
)


@pytest.fixture
def test_dataset_and_camera():
    from aws_oversightml_model_runner.utils.gdal_helper import load_gdal_dataset

    ds, camera_model = load_gdal_dataset("./test/data/GeogToWGS84GeoKey5.tif")
    return ds, camera_model


def test_chip_generator():
    chip_list = []
    for chip in generate_crops_for_region(((5, 10), (1024, 1024)), (300, 300), (44, 44)):
        chip_list.append(chip)

    assert len(chip_list) == 16
    assert chip_list[0] == ((5, 10), (300, 300))
    assert chip_list[1] == ((5, 266), (300, 300))
    assert chip_list[3] == ((5, 778), (256, 300))
    assert chip_list[12] == ((773, 10), (300, 256))
    assert chip_list[15] == ((773, 778), (256, 256))

    chip_list = []
    for chip in generate_crops_for_region(((0, 0), (5000, 2500)), (2048, 2048), (0, 0)):
        chip_list.append(chip)

    assert len(chip_list) == 6
    assert chip_list[0] == ((0, 0), (2048, 2048))
    assert chip_list[1] == ((0, 2048), (2048, 2048))
    assert chip_list[2] == ((0, 4096), (904, 2048))
    assert chip_list[3] == ((2048, 0), (2048, 452))
    assert chip_list[4] == ((2048, 2048), (2048, 452))
    assert chip_list[5] == ((2048, 4096), (904, 452))


def test_next_greater_multiple():
    assert 16 == next_greater_multiple(1, 16)
    assert 16 == next_greater_multiple(15, 16)
    assert 16 == next_greater_multiple(16, 16)
    assert 32 == next_greater_multiple(17, 16)
    assert 48 == next_greater_multiple(42, 16)
    assert 64 == next_greater_multiple(50, 16)
    assert 528 == next_greater_multiple(513, 16)


def test_next_greater_power_of_two():
    assert 1 == next_greater_power_of_two(1)
    assert 2 == next_greater_power_of_two(2)
    assert 4 == next_greater_power_of_two(3)
    assert 8 == next_greater_power_of_two(8)
    assert 64 == next_greater_power_of_two(42)
    assert 128 == next_greater_power_of_two(100)
    assert 256 == next_greater_power_of_two(255)
    assert 512 == next_greater_power_of_two(400)


# Test data here could be improved. We're reusing a nitf file for everything and just
# testing a single raster scale
def test_create_gdal_translate_kwargs(test_dataset_and_camera):
    ds, camera_model = test_dataset_and_camera

    format_compression_combinations = [
        (ImageFormats.NITF, ImageCompression.NONE, "IC=NC"),
        (ImageFormats.NITF, ImageCompression.JPEG, "IC=C3"),
        (ImageFormats.NITF, ImageCompression.J2K, "IC=C8"),
        (ImageFormats.NITF, "FAKE", ""),
        (ImageFormats.NITF, None, "IC=C8"),
        (ImageFormats.JPEG, ImageCompression.NONE, None),
        (ImageFormats.JPEG, ImageCompression.JPEG, None),
        (ImageFormats.JPEG, ImageCompression.J2K, None),
        (ImageFormats.JPEG, "FAKE", None),
        (ImageFormats.JPEG, None, None),
        (ImageFormats.PNG, ImageCompression.NONE, None),
        (ImageFormats.PNG, ImageCompression.JPEG, None),
        (ImageFormats.PNG, ImageCompression.J2K, None),
        (ImageFormats.PNG, "FAKE", None),
        (ImageFormats.PNG, None, None),
        (ImageFormats.GEOTIFF, ImageCompression.NONE, None),
        (ImageFormats.GEOTIFF, ImageCompression.JPEG, None),
        (ImageFormats.GEOTIFF, ImageCompression.J2K, None),
        (ImageFormats.GEOTIFF, "FAKE", None),
        (ImageFormats.GEOTIFF, None, None),
    ]

    for image_format, image_compression, expected_options in format_compression_combinations:
        gdal_translate_kwargs = create_gdal_translate_kwargs(image_format, image_compression, ds)

        assert gdal_translate_kwargs["format"] == image_format
        assert gdal_translate_kwargs["scaleParams"] == [[0, 255, 0, 255]]
        assert gdal_translate_kwargs["outputType"] == 1
        if expected_options:
            assert gdal_translate_kwargs["creationOptions"] == expected_options


def test_get_image_type():
    assert get_image_type("s3://my/test/buck/image.ntf") == "NITF"
    assert get_image_type("s3://my/test/buck/image.nTf") == "NITF"
    assert get_image_type("s3://my/test/buck/image.nitf") == "NITF"
    assert get_image_type("s3://my/test/buck/image.Tif") == "TIFF"
    assert get_image_type("s3://my/test/buck/image.tiff") == "TIFF"
    assert get_image_type("s3://my/test/buck/image.jpg") == "JPG"
    assert get_image_type("s3://my/test/buck/image") == "UNKNOWN"
