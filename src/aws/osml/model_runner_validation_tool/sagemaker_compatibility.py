#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

import json
import os
from datetime import datetime, timezone

import boto3

from aws.osml.model_runner_validation_tool.common import S3Utils
from aws.osml.model_runner_validation_tool.common.logging_config import configure_logging
from aws.osml.model_runner_validation_tool.common.sagemaker_utils import SageMakerHelper

logger = configure_logging(__name__)

cloudwatch_client = boto3.client("cloudwatch")
cfn_client = boto3.client("cloudformation")
s3_utils = S3Utils()

DEFAULT_ML_INSTANCE_TYPE = "ml.m5.xlarge"


def run_sagemaker_compatibility_tests(model_info, execution_role_arn, preserve_endpoint=False):
    """
    Run SageMaker compatibility tests on the provided model

    Args:
        model_info (dict): Information about the model to test
        execution_role_arn (string): SageMaker execution role ARN
        preserve_endpoint (boolean): If True, don't delete the endpoint at the end of the test or step function workflow

    Returns:
        dict: Test results
    """

    start_test_timestamp = datetime.now(timezone.utc)
    try:
        ecs_image_uri = model_info.get("ecsImageUri")
        s3_model_data_uri = model_info.get("s3ModelDataUri", None)  # weights, etc. e.g. my_model.tar.gz
        environment_vars = model_info.get("environmentVars")
        basic_instance_type_preference = model_info.get("basicInstanceTypePreference", DEFAULT_ML_INSTANCE_TYPE)
    except Exception as e:
        logger.error(f"Error retrieving model info from StepFunction payload: {str(e)}")
        raise

    sm_helper = SageMakerHelper(ecs_image_uri, execution_role_arn=execution_role_arn)
    logger.info(f"Running SageMaker compatibility tests for model: {ecs_image_uri}")
    model_name = sm_helper.create_model(model_data_uri=s3_model_data_uri, environment_vars=environment_vars)

    endpoint_config_name = sm_helper.create_endpoint_config(instance_type=basic_instance_type_preference)

    endpoint_info = sm_helper.create_endpoint(
        endpoint_config_name=endpoint_config_name, wait_for_completion=True, max_wait_time_minutes=10
    )
    endpoint_time_to_in_service = endpoint_info.get("timeToInService", 0)

    end_test_timestamp = datetime.now(timezone.utc)

    test_results = {
        "sageMakerCompatible": True,  # If we got this far, the model is compatible with SageMaker
        "ecsImageUri": ecs_image_uri,
        "s3ModelDataUri": s3_model_data_uri,
        "modelName": model_name,
        "endpointConfigName": endpoint_config_name,
        "endpointName": endpoint_info.get("endpointName"),
        "testDurationSeconds": (end_test_timestamp - start_test_timestamp).total_seconds(),
        "endpointTimeToInServiceSeconds": endpoint_time_to_in_service,
        "existingEndpointName": None,
        "preserveEndpoint": preserve_endpoint,
    }

    log_metrics_to_cloudwatch(test_results.get("modelName"), test_results)
    save_test_results_to_s3(test_results.get("modelName"), test_results, "SageMaker-compatibility")

    return test_results


def log_metrics_to_cloudwatch(model_name, test_results):
    """
    Log test metrics to CloudWatch

    Args:
        model_name (str): Name of the model
        test_results (dict): Test results to log
    """
    logger.info(f"Logging metrics to CloudWatch for model: {model_name}")

    cloudwatch_client.put_metric_data(
        Namespace="OSML/ModelValidation/SageMakerCompatibility",
        MetricData=[
            {
                "MetricName": "IsCompatible",
                "Dimensions": [{"Name": "ModelName", "Value": test_results.get("modelName", "unknown")}],
                "Value": 1 if test_results.get("sageMakerCompatible") else 0,
                "Unit": "Count",
            },
            {
                "MetricName": "EndpointTimeToInService",
                "Dimensions": [{"Name": "ModelName", "Value": test_results.get("modelName", "unknown")}],
                "Value": test_results.get("endpointTimeToInService", 0),
                "Unit": "Seconds",
            },
            {
                "MetricName": "TestDuration",
                "Dimensions": [{"Name": "ModelName", "Value": test_results.get("modelName", "unknown")}],
                "Value": test_results.get("testDurationSeconds", 0),
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

    timestamp = datetime.now(timezone.utc).isoformat()
    key = f"{model_name}/{test_type}/{timestamp}.json"

    logger.info(f"Saving test results to S3: {report_bucket}/{key}")

    s3_utils.put_object(report_bucket, key, json.dumps(test_results, indent=2), "application/json")


def handler(event, context):
    """
    Lambda handler for SageMaker compatibility tests

    Args:
        event (dict): Lambda event
        context (object): Lambda context

    Returns:
        dict: Test results
    """
    logger.info(f"Sagemaker compatibility handler - received event: {json.dumps(event)}")

    execution_role_arn = os.environ.get("SAGEMAKER_EXECUTION_ROLE_ARN")
    if not execution_role_arn:
        logger.error("SAGEMAKER_EXECUTION_ROLE_ARN environment variable not set")
        raise Exception("SAGEMAKER_EXECUTION_ROLE_ARN environment variable not set")

    try:
        preserve_endpoint = event.get("preserveEndpoint", False)
        existing_endpoint_name = event.get("existingEndpointName", None)
        if not existing_endpoint_name:
            model_info = event.get("modelInfo", {})

            if not model_info:
                raise ValueError("Model information not provided in the event")

            test_results = run_sagemaker_compatibility_tests(model_info, execution_role_arn, preserve_endpoint)
        else:
            logger.info(f"Skipping SageMaker compatibility tests using existing endpoint: {existing_endpoint_name}")
            test_results = {
                "sageMakerCompatible": True,
                "ecsImageUri": "n/a - used existing endpoint",
                "s3ModelDataUri": "n/a - used existing endpoint",
                "modelName": "n/a - used existing endpoint",
                "endpointConfigName": "n/a - used existing endpoint",
                "endpointName": existing_endpoint_name,
                "testDurationSeconds": 0,
                "endpointTimeToInServiceSeconds": 0,
                "existingEndpointName": existing_endpoint_name,
                "preserveEndpoint": preserve_endpoint,
            }

        return {
            "statusCode": 200,
            "sageMakerCompatibilityResults": test_results,
        }
    except Exception as e:
        logger.error(f"Error running SageMaker compatibility tests: {str(e)}", exc_info=True)

        # Throw any exceptions back to the state machine so the task will fail
        raise e
