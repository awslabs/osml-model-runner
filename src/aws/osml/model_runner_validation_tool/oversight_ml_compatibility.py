#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

import json
import os
from datetime import datetime, timezone

import boto3

from aws.osml.model_runner_validation_tool.common.logging_config import configure_logging
from aws.osml.model_runner_validation_tool.common.s3_utils import S3Utils
from aws.osml.model_runner_validation_tool.common.validate_geo_json import ValidateGeoJSON

logger = configure_logging(__name__)

cloudwatch_client = boto3.client("cloudwatch")
s3_utils = S3Utils()

DEFAULT_ML_INSTANCE_TYPE = "ml.m5.xlarge"
SUPPORTED_IMAGE_EXTENSIONS = [".jpg", ".jpeg", ".png", ".tiff", ".tif", ".ntf"]


def run_oversight_ml_compatibility_tests(model_info, sm_compatibility_results):
    """
    Run Oversight Machine Learning compatibility tests on the provided model

    Args:
        model_info (dict): Information about the model to test
        sm_compatibility_results (dict): Sagemaker Compatibility test results, including information about the in-service
        inference endpoint

    Returns:
        dict: OSML Compatibility Test results
    """

    start_test_timestamp = datetime.now(timezone.utc)
    model_name = sm_compatibility_results.get("modelName")
    endpoint_name = sm_compatibility_results.get("endpointName")
    logger.info(
        f"Running Oversight ML compatibility tests for model: "
        f"{model_name}, using the inference endpoint: {endpoint_name}"
    )

    sagemaker_runtime = boto3.client("sagemaker-runtime")

    test_imagery_bucket = os.environ.get("TEST_IMAGERY_BUCKET")
    if not test_imagery_bucket:
        raise ValueError("TEST_IMAGERY_BUCKET environment variable is not set")

    test_images = s3_utils.list_objects(test_imagery_bucket)
    if not test_images:
        logger.warning("No test images found in bucket")

    # TODO: Rename *_validations to something like inference_succeeded and inference_failed
    # Track validation results for all images
    all_validations = []
    successful_validations = 0
    failed_validations = 0

    for object_key in test_images:
        logger.info(f"Processing test image: {object_key}")
        file_bytes = None
        validation_result = {"imageKey": object_key, "isValid": False, "message": "", "error": None}
        if not any(object_key.lower().endswith(ext) for ext in SUPPORTED_IMAGE_EXTENSIONS):
            error_msg = f"Skipping image with unsupported extension: {object_key}"
            logger.warning(error_msg)
            validation_result["error"] = error_msg
            all_validations.append(validation_result)
            failed_validations += 1
            continue

        try:
            file_bytes, error = s3_utils.get_object(test_imagery_bucket, object_key)
            if error:
                validation_result["error"] = error
                all_validations.append(validation_result)
                failed_validations += 1
                continue

            # Skip if file_bytes is None or empty
            if not file_bytes:
                error_msg = f"Empty file or failed to download: {object_key}"
                logger.warning(error_msg)
                validation_result["error"] = error_msg
                all_validations.append(validation_result)
                failed_validations += 1
                continue

            content_type = determine_content_type_from_extension(object_key)

            logger.info(f"Invoking endpoint {endpoint_name} with image {object_key}")
            response = sagemaker_runtime.invoke_endpoint(
                EndpointName=endpoint_name, ContentType=content_type, Body=file_bytes
            )

            raw_response = response["Body"].read().decode("utf-8")
            logger.info(f"Inference response: {raw_response}")
            is_valid, message = ValidateGeoJSON.validate(raw_response)
            validation_result["isValid"] = is_valid
            validation_result["message"] = message

            if is_valid:
                logger.info(f"GeoJSON is valid for image: {object_key}")
                successful_validations += 1
            else:
                logger.warning(f"GeoJSON validation failed for image {object_key}: {message}")
                failed_validations += 1

        except Exception as e:
            error_msg = f"Error processing image {object_key}: {str(e)}"
            logger.warning(error_msg)
            validation_result["error"] = error_msg
            failed_validations += 1

        all_validations.append(validation_result)

    # Assume the model is compatible with OSML if inference is successful for at least one image tile
    is_valid = successful_validations > 0
    message = f"Processed {len(all_validations)} images: {successful_validations} successful, {failed_validations} failed"

    logger.info(f"Validation summary: {message}")
    for result in all_validations:
        status = "✓ Valid" if result["isValid"] else "✗ Invalid"
        if result["error"]:
            logger.info(f"{status} - {result['imageKey']}: Error - {result['error']}")
        else:
            logger.info(f"{status} - {result['imageKey']}: {result['message']}")

    # If no images were successfully processed, mark as invalid
    if len(all_validations) == 0:
        is_valid = False
        message = "No test images were successfully processed"

    logger.info(
        f"Oversight ML compatibility tests completed for model: {model_name}, Model "
        f"{'is' if is_valid else 'is not'} compatible with Oversight ML"
    )

    test_results = {
        "modelName": model_name,
        "oversightMLCompatible": is_valid,
        "testDurationSeconds": (datetime.now(timezone.utc) - start_test_timestamp).total_seconds(),
        "message": message,
        "detailedResults": all_validations,
        "summary": {
            "totalImages": len(all_validations),
            "successfulValidations": successful_validations,
            "failedValidations": failed_validations,
        },
    }

    log_metrics_to_cloudwatch(model_name, test_results)
    save_test_results_to_s3(model_name, test_results, "oversight-ml-compatibility")

    return test_results


def log_metrics_to_cloudwatch(model_name, test_results):
    """
    Log test metrics to CloudWatch

    Args:
        model_name (str): Name of the model
        test_results (dict): Test results to log
    """
    logger.info(f"Logging metrics to CloudWatch for model: {model_name}")

    successful_inference_count = test_results.get("summary").get("successfulValidations")
    failed_inference_count = test_results.get("summary").get("failedValidations")

    logger.info(f"Successful inferences: {successful_inference_count}, Failed inferences: {failed_inference_count}")
    logger.info(f"Type of successful_inference_count: {type(successful_inference_count)}")
    logger.info(f"Type of failed_inference_count: {type(failed_inference_count)}")
    logger.info(f"Type of test_duration_seconds: {type(test_results.get('summary').get('testDurationSeconds'))}")

    # Example of logging metrics to CloudWatch
    cloudwatch_client.put_metric_data(
        Namespace="OSML/ModelValidation/OSMLCompatibility",
        MetricData=[
            {
                "MetricName": "IsCompatible",
                "Dimensions": [{"Name": "ModelName", "Value": model_name}],
                "Value": 1 if test_results.get("oversightMLCompatible") else 0,
                "Unit": "Count",
            },
            {
                "MetricName": "SuccessfulInferences",
                "Dimensions": [{"Name": "ModelName", "Value": model_name}],
                "Value": successful_inference_count,
                "Unit": "Count",
            },
            {
                "MetricName": "FailedInferences",
                "Dimensions": [{"Name": "ModelName", "Value": model_name}],
                "Value": failed_inference_count,
                "Unit": "Count",
            },
            {
                "MetricName": "TestDuration",
                "Dimensions": [{"Name": "ModelName", "Value": model_name}],
                "Value": test_results.get("testDurationSeconds"),
                "Unit": "Seconds",
            },
        ],
    )


def save_test_results_to_s3(model_name, test_results, test_type):
    """
    Save test results to S3

    Args:
        model_name (str): Name of the model
        test_results (dict): Test results to save
        test_type (str): Type of test
    """

    report_bucket = os.environ.get("REPORT_BUCKET")
    if not report_bucket:
        logger.warning("REPORT_BUCKET environment variable not set, skipping S3 upload")
        return

    s3_utils.save_test_results(report_bucket, model_name, test_results, test_type)


def handler(event, context):
    """
    Lambda handler for Oversight ML compatibility tests

    Args:
        event (dict): Lambda event
        context (object): Lambda context

    Returns:
        dict: Test results
    """
    logger.info(f"OSML compatibility handler - received event: {json.dumps(event)}")

    try:
        model_info = event.get("modelInfo", {})
        if not model_info:
            raise ValueError("Model information not provided in the event")

        # TODO: Clean this up
        smc_results_key = "sageMakerCompatibilityResults"
        sm_compatibility_results = event.get(smc_results_key, {}).get("Payload", {}).get(smc_results_key)

        if not sm_compatibility_results:
            raise ValueError(
                "SageMaker compatibility results not provided in the event and are required"
                "for the Oversight ML compatibility tests"
            )

        test_results = run_oversight_ml_compatibility_tests(model_info, sm_compatibility_results)

        return {
            "statusCode": 200,
            "oversightMLCompatibilityResults": test_results,
        }
    except Exception as e:
        logger.error(f"Error running Oversight ML compatibility tests: {str(e)}", exc_info=True)
        return {"statusCode": 500, "error": str(e), "oversightMLCompatible": False}


def determine_content_type_from_extension(object_key):
    content_type = "application/octet-stream"  # Unknown, treat the payload as a stream of bytes
    if object_key.lower().endswith(".jpg") or object_key.lower().endswith(".jpeg"):
        content_type = "image/jpeg"
    if object_key.lower().endswith(".tif") or object_key.lower().endswith(".tiff"):
        content_type = "image/tiff"
    elif object_key.lower().endswith(".png"):
        content_type = "image/png"
    elif object_key.lower().endswith(".ntf"):
        content_type = "image/ntf"

    return content_type
