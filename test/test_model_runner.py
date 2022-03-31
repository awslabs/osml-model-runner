import multiprocessing

import mock
import pytest
import shapely.geometry
import shapely.wkt

from aws_oversightml_model_runner.model_runner_api import RegionRequest
from aws_oversightml_model_runner.georeference import GDALAffineCameraModel
from aws_oversightml_model_runner.job_table import JobTable
from aws_oversightml_model_runner.model_runner import load_gdal_dataset, calculate_processing_bounds, get_image_type, \
    process_region_request, create_gdal_translate_kwargs
from aws_oversightml_model_runner.metrics import configure_metrics

configure_metrics("test", "stdout")

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
    roi = shapely.wkt.loads(
        "POLYGON ((8.999932379599102 52.0023621190119, 8.999932379599102 52.0002787856769, 9.001599046267101 52.0002787856769, 9.001599046267101 52.0023621190119, 8.999932379599102 52.0023621190119))")

    # Manually verify the lon/lat coordinates of the image positions used in this test with these print statements
    # print(camera_model.image_to_world((10, 15)))
    # print(camera_model.image_to_world((70, 90)))
    processing_bounds = calculate_processing_bounds(roi, ds, camera_model)

    # Processing bounds is in ((r, c), (w, h))
    assert processing_bounds == ((15, 10), (60, 75))


def test_get_image_type():
    assert "NITF" == get_image_type("s3://random-bucket/image.ntf")
    assert "NITF" == get_image_type("https://foo.bar.com/random/prefix/complex-image.name.image.NITF")
    assert "TIFF" == get_image_type("s3://random-bucket/works-with-tiff-too.TIFF")
    assert "TIFF" == get_image_type("./foo.tif")


def test_create_gdal_translate_kwargs(test_dataset_and_camera):
    ds, camera_model = test_dataset_and_camera

    format_compression_combinations = [
        ('NITF', 'NONE', 'IC=NC'),
        ('NITF', 'JPEG', 'IC=C3'),
        ('NITF', 'J2K', 'IC=C8'),
        ('NITF', None, 'IC=C8')
    ]

    for tile_format, tile_compression, expected_options in format_compression_combinations:
        region_request = RegionRequest({'tile_format': tile_format,
                                        'tile_compression': tile_compression
                                        })

        gdal_translate_kwargs = create_gdal_translate_kwargs(region_request, ds)

        assert gdal_translate_kwargs['creationOptions'] == expected_options
        assert gdal_translate_kwargs['format'] == tile_format
        assert gdal_translate_kwargs['scaleParams'] == [[0, 255, 0, 255]]
        assert gdal_translate_kwargs['outputType'] == 1


class RegionRequestMatcher:
    def __init__(self, region_request):
        self.region_request = region_request

    def __eq__(self, other):
        if other is None:
            return self.region_request is None
        else:
            return other['region'] == self.region_request['region'] and \
                   other['image_id'] == self.region_request['image_id']


# Remember that with mutiple patch decorators the order of the mocks in the parameter list is reversed (i.e. the
# first mock parameter is the last decorator defined. Also note that the pytest fixtures must come at the end.
@mock.patch.dict("os.environ", {"AWS_DEFAULT_REGION": "us-east-1", "WORKERS_PER_CPU": "1"})
@mock.patch('aws_oversightml_model_runner.model_runner.FeatureDetector', autospec=True)
@mock.patch('aws_oversightml_model_runner.model_runner.FeatureTable', autospec=True)
@mock.patch('aws_oversightml_model_runner.model_runner.ImageTileWorker', autospec=True)
@mock.patch('aws_oversightml_model_runner.model_runner.Queue', autospec=True)
def test_process_region_request(mock_queue, mock_tile_worker, mock_feature_table, mock_feature_detector,
                                test_dataset_and_camera):
    region_request = RegionRequest({'tile_size': (10, 10),
                      'tile_overlap': (1, 1),
                      'tile_format': 'NITF',
                      'image_id': 'test-image-id',
                      'image_url': 'test-image-url',
                      'region_bounds': ((0, 0), (50, 50)),
                      'model_name': 'test-model-name'
                      })

    mock_job_table = mock.Mock(JobTable, autospec=True)

    raster_dataset = test_dataset_and_camera[0]

    process_region_request(region_request, mock_job_table, raster_dataset)

    region_queue_put_calls = [
        mock.call(RegionRequestMatcher({'region': ((0, 0), (10, 10)), 'image_id': 'test-image-id'})),
        mock.call(RegionRequestMatcher({'region': ((0, 9), (10, 10)), 'image_id': 'test-image-id'})),
        mock.call(RegionRequestMatcher({'region': ((0, 18), (10, 10)), 'image_id': 'test-image-id'})),
        mock.call(RegionRequestMatcher({'region': ((0, 27), (10, 10)), 'image_id': 'test-image-id'})),
        mock.call(RegionRequestMatcher({'region': ((0, 36), (10, 10)), 'image_id': 'test-image-id'})),
        mock.call(RegionRequestMatcher({'region': ((0, 45), (5, 10)), 'image_id': 'test-image-id'})),
        mock.call(RegionRequestMatcher({'region': ((9, 0), (10, 10)), 'image_id': 'test-image-id'})),
        mock.call(RegionRequestMatcher({'region': ((9, 9), (10, 10)), 'image_id': 'test-image-id'})),
        mock.call(RegionRequestMatcher({'region': ((9, 18), (10, 10)), 'image_id': 'test-image-id'})),
        mock.call(RegionRequestMatcher({'region': ((9, 27), (10, 10)), 'image_id': 'test-image-id'})),
        mock.call(RegionRequestMatcher({'region': ((9, 36), (10, 10)), 'image_id': 'test-image-id'})),
        mock.call(RegionRequestMatcher({'region': ((9, 45), (5, 10)), 'image_id': 'test-image-id'})),
        mock.call(RegionRequestMatcher({'region': ((18, 0), (10, 10)), 'image_id': 'test-image-id'})),
        mock.call(RegionRequestMatcher({'region': ((18, 9), (10, 10)), 'image_id': 'test-image-id'})),
        mock.call(RegionRequestMatcher({'region': ((18, 18), (10, 10)), 'image_id': 'test-image-id'})),
        mock.call(RegionRequestMatcher({'region': ((18, 27), (10, 10)), 'image_id': 'test-image-id'})),
        mock.call(RegionRequestMatcher({'region': ((18, 36), (10, 10)), 'image_id': 'test-image-id'})),
        mock.call(RegionRequestMatcher({'region': ((18, 45), (5, 10)), 'image_id': 'test-image-id'})),
        mock.call(RegionRequestMatcher({'region': ((27, 0), (10, 10)), 'image_id': 'test-image-id'})),
        mock.call(RegionRequestMatcher({'region': ((27, 9), (10, 10)), 'image_id': 'test-image-id'})),
        mock.call(RegionRequestMatcher({'region': ((27, 18), (10, 10)), 'image_id': 'test-image-id'})),
        mock.call(RegionRequestMatcher({'region': ((27, 27), (10, 10)), 'image_id': 'test-image-id'})),
        mock.call(RegionRequestMatcher({'region': ((27, 36), (10, 10)), 'image_id': 'test-image-id'})),
        mock.call(RegionRequestMatcher({'region': ((27, 45), (5, 10)), 'image_id': 'test-image-id'})),
        mock.call(RegionRequestMatcher({'region': ((36, 0), (10, 10)), 'image_id': 'test-image-id'})),
        mock.call(RegionRequestMatcher({'region': ((36, 9), (10, 10)), 'image_id': 'test-image-id'})),
        mock.call(RegionRequestMatcher({'region': ((36, 18), (10, 10)), 'image_id': 'test-image-id'})),
        mock.call(RegionRequestMatcher({'region': ((36, 27), (10, 10)), 'image_id': 'test-image-id'})),
        mock.call(RegionRequestMatcher({'region': ((36, 36), (10, 10)), 'image_id': 'test-image-id'})),
        mock.call(RegionRequestMatcher({'region': ((36, 45), (5, 10)), 'image_id': 'test-image-id'})),
        mock.call(RegionRequestMatcher({'region': ((45, 0), (10, 5)), 'image_id': 'test-image-id'})),
        mock.call(RegionRequestMatcher({'region': ((45, 9), (10, 5)), 'image_id': 'test-image-id'})),
        mock.call(RegionRequestMatcher({'region': ((45, 18), (10, 5)), 'image_id': 'test-image-id'})),
        mock.call(RegionRequestMatcher({'region': ((45, 27), (10, 5)), 'image_id': 'test-image-id'})),
        mock.call(RegionRequestMatcher({'region': ((45, 36), (10, 5)), 'image_id': 'test-image-id'})),
        mock.call(RegionRequestMatcher({'region': ((45, 45), (5, 5)), 'image_id': 'test-image-id'}))]

    num_workers = multiprocessing.cpu_count()
    for i in range(num_workers):
        region_queue_put_calls.append(mock.call(RegionRequestMatcher(None)))

    # Check to make sure the correct number of workers were created and setup with detectors and feature tables
    assert mock_tile_worker.call_count == num_workers
    assert mock_feature_detector.call_count == num_workers
    assert mock_feature_table.call_count == num_workers

    # Check to make sure a queue was created and populated with appropriate region requests
    mock_queue.assert_called_once()
    mock_queue.return_value.put.assert_has_calls(region_queue_put_calls)

    # Check to make sure the job was marked as complete
    mock_job_table.region_complete.assert_called_with(region_request.image_id)
