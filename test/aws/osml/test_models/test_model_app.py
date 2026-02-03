#!/usr/bin/env python3
#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

import json
import os

import pytest


@pytest.fixture
def model_app_setup():
    """
    Set up test fixtures for model app tests.
    """
    from aws.osml.test_models.app import app

    ctx = app.app_context()
    ctx.push()
    client = app.test_client()

    # Preserve environment to avoid cross-test contamination
    saved_default_selection = os.environ.get("DEFAULT_MODEL_SELECTION")
    saved_model_selection = os.environ.get("MODEL_SELECTION")
    os.environ.pop("DEFAULT_MODEL_SELECTION", None)
    os.environ.pop("MODEL_SELECTION", None)

    yield app, client

    # Cleanup
    ctx.pop()
    if saved_default_selection is not None:
        os.environ["DEFAULT_MODEL_SELECTION"] = saved_default_selection
    else:
        os.environ.pop("DEFAULT_MODEL_SELECTION", None)
    if saved_model_selection is not None:
        os.environ["MODEL_SELECTION"] = saved_model_selection
    else:
        os.environ.pop("MODEL_SELECTION", None)


def test_ping(model_app_setup):
    """Test ping endpoint returns 200"""
    app, client = model_app_setup
    response = client.get("/ping")
    assert response.status_code == 200


def test_missing_model_selection(model_app_setup):
    """Test missing model selection returns 400"""
    app, client = model_app_setup
    response = client.post("/invocations", data=b"")
    assert response.status_code == 400


def test_centerpoint_routing(model_app_setup):
    """Test centerpoint model routing"""
    app, client = model_app_setup
    with open("test/data/test-model.tif", "rb") as data_binary:
        response = client.post(
            "/invocations",
            data=data_binary,
            headers={"X-Amzn-SageMaker-Custom-Attributes": "model_selection=centerpoint"},
        )
    assert response.status_code == 200
    actual_geojson_result = json.loads(response.data)
    assert actual_geojson_result["type"] == "FeatureCollection"
    assert "features" in actual_geojson_result


def test_flood_routing_with_volume(model_app_setup):
    """Test flood model routing with volume parameter"""
    app, client = model_app_setup
    with open("test/data/test-model.tif", "rb") as data_binary:
        response = client.post(
            "/invocations",
            data=data_binary,
            headers={"X-Amzn-SageMaker-Custom-Attributes": "model_selection=flood,flood_volume=5"},
        )
    assert response.status_code == 200
    actual_geojson_result = json.loads(response.data)
    assert actual_geojson_result["type"] == "FeatureCollection"
    assert len(actual_geojson_result["features"]) == 5


def test_failure_routing(model_app_setup):
    """Test failure model routing"""
    app, client = model_app_setup
    with open("test/data/test-model.tif", "rb") as data_binary:
        response = client.post(
            "/invocations",
            data=data_binary,
            headers={"X-Amzn-SageMaker-Custom-Attributes": "model_selection=failure"},
        )
    assert response.status_code == 200
    actual_geojson_result = json.loads(response.data)
    assert actual_geojson_result["type"] == "FeatureCollection"
    assert "features" in actual_geojson_result
