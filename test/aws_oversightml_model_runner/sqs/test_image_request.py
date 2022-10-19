legacy_execution_role = "arn:aws:iam::010321660603:role/OversightMLBetaInvokeRole"


def test_request_data():
    return {
        "job_arn": "arn:aws:oversightml:us-east-1:674401241798:ipj/test-job",
        "job_id": "test-job",
        "image_id": "test-image-id",
        "image_url": "test-image-url",
        "image_read_role": "arn:aws:iam::010321661213:role/OversightMLS3ReadOnly",
        "output_bucket": "unit-test",
        "output_prefix": "region-request",
        "tile_size": (10, 10),
        "tile_overlap": (1, 1),
        "tile_format": "NITF",
        "model_name": "test-model-name",
        "model_hosting_type": "SM_ENDPOINT",
        "model_invocation_role": "arn:aws:iam::010321661213:role/OversightMLModelInvoker",
    }


def test_invalid_data():
    from aws_oversightml_model_runner.sqs.image_request import ImageRequest

    ir = ImageRequest(
        test_request_data(),
        image_id="",
    )

    assert not ir.is_valid()


def test_invalid_job_arn():
    from aws_oversightml_model_runner.sqs.image_request import ImageRequest

    ir = ImageRequest(
        test_request_data(),
        job_arn="",
    )

    assert not ir.is_valid()


def test_invalid_job_id():
    from aws_oversightml_model_runner.sqs.image_request import ImageRequest

    ir = ImageRequest(
        test_request_data(),
        job_id=None,
    )

    assert not ir.is_valid()
