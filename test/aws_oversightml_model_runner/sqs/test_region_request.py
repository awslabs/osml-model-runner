from data.sample_request_data import SAMPLE_REGION_REQUEST_DATA


def test_invalid_data():
    from aws_oversightml_model_runner.sqs.region_request import RegionRequest

    rr = RegionRequest(
        SAMPLE_REGION_REQUEST_DATA,
        image_id="",
    )

    assert not rr.is_valid()


def test_invalid_region_bounds():
    from aws_oversightml_model_runner.sqs.region_request import RegionRequest

    rr = RegionRequest(
        SAMPLE_REGION_REQUEST_DATA,
        region_bounds=None,
    )

    assert not rr.is_valid()
