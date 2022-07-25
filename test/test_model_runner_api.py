import shapely.geometry

from aws_model_runner.model_runner_api import (
    ImageRequest,
    ModelHostingOptions,
    RegionRequest,
    TileFormats,
)


def test_region_request_constructor():
    region_request_template = {
        "model_name": "test-model-name",
        "model_hosting_type": "SM_ENDPOINT",
        "execution_role": "arn:aws:iam::010321660603:role/OversightMLBetaInvokeRole",
    }

    rr = RegionRequest(
        region_request_template,
        image_id="test-image-id",
        image_url="s3://fake-bucket/images/test-image-id",
        output_bucket="fake-bucket",
        output_prefix="images/outputs",
        region_bounds=[0, 1, 2, 3],
    )

    # Check to ensure we've created a valid request
    assert rr.is_valid()

    # Checks to ensure the dictionary provided values are set
    assert rr.model_name == "test-model-name"
    assert rr.model_hosting_type == ModelHostingOptions.SM_ENDPOINT
    assert rr.execution_role == "arn:aws:iam::010321660603:role/OversightMLBetaInvokeRole"

    # Checks to ensure the keyword arguments are set
    assert rr.image_id == "test-image-id"
    assert rr.image_url == "s3://fake-bucket/images/test-image-id"
    assert rr.output_bucket == "fake-bucket"
    assert rr.output_prefix == "images/outputs"
    assert rr.region_bounds == [0, 1, 2, 3]

    # Checks to ensure the defaults are set
    assert rr.tile_size == (1024, 1024)
    assert rr.tile_overlap == (50, 50)
    assert rr.tile_format == TileFormats.NITF
    assert rr.tile_compression is None


def test_image_request_constructor():
    image_request_template = {
        "model_name": "test-model-name",
        "model_hosting_type": "SM_ENDPOINT",
        "execution_role": "arn:aws:iam::010321660603:role/OversightMLBetaInvokeRole",
    }
    ir = ImageRequest(
        image_request_template,
        job_arn="arn:aws:oversightml:us-east-1:674401241798:ipj/test-job",
        job_name="test-job",
        job_id="5f4e8a55-95cf-4d96-95cd-9b037f767eff",
        image_id="5f4e8a55-95cf-4d96-95cd-9b037f767eff:s3://fake-bucket/images/test-image-id",
        image_url="s3://fake-bucket/images/test-image-id",
        output_bucket="fake-bucket",
        output_prefix="images/outputs",
    )

    assert ir.is_valid()
    assert ir.image_url == "s3://fake-bucket/images/test-image-id"
    assert (
        ir.image_id == "5f4e8a55-95cf-4d96-95cd-9b037f767eff:s3://fake-bucket/images/test-image-id"
    )
    assert ir.tile_size == (1024, 1024)
    assert ir.tile_overlap == (50, 50)
    assert ir.model_name == "test-model-name"
    assert ir.model_hosting_type == "SM_ENDPOINT"
    assert ir.execution_role == "arn:aws:iam::010321660603:role/OversightMLBetaInvokeRole"
    assert ir.tile_format == "NITF"
    assert ir.tile_compression is None
    assert ir.job_id == "5f4e8a55-95cf-4d96-95cd-9b037f767eff"
    assert ir.job_arn == "arn:aws:oversightml:us-east-1:674401241798:ipj/test-job"
    assert ir.output_bucket == "fake-bucket"
    assert ir.output_prefix == "images/outputs"
    assert ir.roi is None


def test_image_request_from_message():
    message_body = {
        "jobArn": "arn:aws:oversightml:us-east-1:674401241798:ipj/test-job",
        "jobName": "test-job",
        "jobId": "5f4e8a55-95cf-4d96-95cd-9b037f767eff",
        "jobStatus": "SUBMITTED",
        "processingSubmitted": "2021-09-14T00:18:32.130000+00:00",
        "imageUrls": ["s3://fake-bucket/images/test-image-id"],
        "outputBucket": "fake-bucket",
        "outputPrefix": "images/outputs",
        "imageProcessor": {"name": "test-model-name", "type": "SM_ENDPOINT"},
        "executionRole": "arn:aws:iam::010321660603:role/OversightMLBetaInvokeRole",
        "imageProcessorTileSize": 2048,
        "imageProcessorTileOverlap": 100,
        "imageProcessorTileFormat": "PNG",
        "regionOfInterest": "POLYGON((0.5 0.5,5 0,5 5,0 5,0.5 0.5), (1.5 1,4 3,4 1,1.5 1))",
    }

    ir = ImageRequest.from_external_message(message_body)

    assert ir.is_valid()
    assert ir.image_url == "s3://fake-bucket/images/test-image-id"
    assert (
        ir.image_id == "5f4e8a55-95cf-4d96-95cd-9b037f767eff:s3://fake-bucket/images/test-image-id"
    )
    assert ir.tile_size == (2048, 2048)
    assert ir.tile_overlap == (100, 100)
    assert ir.model_name == "test-model-name"
    assert ir.model_hosting_type == "SM_ENDPOINT"
    assert ir.execution_role == "arn:aws:iam::010321660603:role/OversightMLBetaInvokeRole"
    assert ir.tile_format == "PNG"
    assert ir.tile_compression is None
    assert ir.job_id == "5f4e8a55-95cf-4d96-95cd-9b037f767eff"
    assert ir.job_arn == "arn:aws:oversightml:us-east-1:674401241798:ipj/test-job"
    assert ir.output_bucket == "fake-bucket"
    assert ir.output_prefix == "images/outputs"
    assert isinstance(ir.roi, shapely.geometry.Polygon)


def test_image_request_from_minimal_message():
    message_body = {
        "jobArn": "arn:aws:oversightml:us-east-1:674401241798:ipj/test-job",
        "jobName": "test-job",
        "jobId": "5f4e8a55-95cf-4d96-95cd-9b037f767eff",
        "imageUrls": ["s3://fake-bucket/images/test-image-id"],
        "outputBucket": "fake-bucket",
        "outputPrefix": "images/outputs",
        "imageProcessor": {"name": "test-model-name", "type": "SM_ENDPOINT"},
    }

    ir = ImageRequest.from_external_message(message_body)

    assert ir.is_valid()
    assert ir.image_url == "s3://fake-bucket/images/test-image-id"
    assert (
        ir.image_id == "5f4e8a55-95cf-4d96-95cd-9b037f767eff:s3://fake-bucket/images/test-image-id"
    )
    assert ir.tile_size == (1024, 1024)
    assert ir.tile_overlap == (50, 50)
    assert ir.model_name == "test-model-name"
    assert ir.model_hosting_type == "SM_ENDPOINT"
    assert ir.execution_role is None
    assert ir.tile_format == "NITF"
    assert ir.tile_compression is None
    assert ir.job_id == "5f4e8a55-95cf-4d96-95cd-9b037f767eff"
    assert ir.job_arn == "arn:aws:oversightml:us-east-1:674401241798:ipj/test-job"
    assert ir.output_bucket == "fake-bucket"
    assert ir.output_prefix == "images/outputs"
    assert ir.roi is None
