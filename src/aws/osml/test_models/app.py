#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

import os
from typing import Callable, Dict

from flask import Response, request

from aws.osml.test_models import build_flask_app, build_logger, setup_server
from aws.osml.test_models.centerpoint import app as centerpoint_app
from aws.osml.test_models.failure import app as failure_app
from aws.osml.test_models.flood import app as flood_app
from aws.osml.test_models.server_utils import parse_custom_attributes

SUPPORTED_MODELS = ("centerpoint", "flood", "failure")
MODEL_SELECTION_ENV = "DEFAULT_MODEL_SELECTION"
CUSTOM_ATTR_KEY = "model_selection"

# Create logger instance
logger = build_logger()

# Create our default flask app
app = build_flask_app(logger)

MODEL_HANDLERS: Dict[str, Callable[[bytes], Response]] = {
    "centerpoint": centerpoint_app.predict_from_bytes,
    "flood": flood_app.predict_from_bytes,
    "failure": failure_app.predict_from_bytes,
}


def _resolve_model_selection() -> str:
    """
    Resolve which test model to invoke for this request.

    Priority:
    1) CustomAttributes header key "model_selection"
    2) DEFAULT_MODEL_SELECTION env var
    3) MODEL_SELECTION env var (legacy fallback)
    """
    attributes = parse_custom_attributes()
    selection = attributes.get(CUSTOM_ATTR_KEY)
    if not selection:
        selection = os.environ.get(MODEL_SELECTION_ENV) or os.environ.get("MODEL_SELECTION")

    return selection.lower().strip() if selection else ""


@app.route("/ping", methods=["GET"])
def healthcheck() -> Response:
    """Health check endpoint."""
    app.logger.debug("Responding to health check")
    return Response(response="\n", status=200)


@app.route("/invocations", methods=["POST"])
def predict() -> Response:
    """
    Unified model invocation endpoint for all test models.
    """
    selection = _resolve_model_selection()
    if selection not in MODEL_HANDLERS:
        app.logger.warning(
            "Invalid or missing model selection: %s. Supported: %s",
            selection or "<empty>",
            ", ".join(SUPPORTED_MODELS),
        )
        return Response(
            response=("Invalid or missing model selection. " f"Supported values: {', '.join(SUPPORTED_MODELS)}"),
            status=400,
        )

    app.logger.debug("Routing request to test model: %s", selection)
    return MODEL_HANDLERS[selection](request.get_data())


if __name__ == "__main__":  # pragma: no cover
    setup_server(app)
