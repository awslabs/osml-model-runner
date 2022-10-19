SAMPLE_REGION_REQUEST_DATA = {
    "tile_size": (10, 10),
    "tile_overlap": (1, 1),
    "tile_format": "NITF",
    "image_id": "test-image-id",
    "image_url": "test-image-url",
    "region_bounds": ((0, 0), (50, 50)),
    "model_name": "test-model-name",
    "model_hosting_type": "SM_ENDPOINT",
    "output_bucket": "unit-test",
    "output_prefix": "region-request",
    "execution_role": "arn:aws:iam::010321660603:role/OversightMLBetaInvokeRole",
}

SAMPLE_IMAGE_REQUEST_DATA = {
    "imageURL": "s3://spacenet-parrised-devaccount/AOI_1_Rio/srcData/rasterData/3-Band/013022223103.tif",
    "outputBucket": "spacenet-parrised-devaccount",
    "outputPrefix": "oversight/AOI_1_Rio",
    "modelName": "charon-xview-endpoint",
}
