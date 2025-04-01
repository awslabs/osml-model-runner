#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

import json
import os

import boto3

from aws.osml.model_runner_validation_tool.common import SageMakerHelper
from aws.osml.model_runner_validation_tool.common.logging_config import configure_logging

logger = configure_logging(__name__)
sagemaker_client = boto3.client("sagemaker")


def cleanup_sagemaker_resources(model_name: str = None, endpoint_config_name: str = None, endpoint_name: str = None):
    """
    Clean up SageMaker resources created during validation tests

    Args:
        model_name (str): Sagemaker model name
        endpoint_config_name (str): Endpoint config name
        endpoint_name (str): Endpoint name

    Returns:
        dict: Results of cleanup operations
    """

    execution_role_arn = os.environ.get("SAGEMAKER_EXECUTION_ROLE_ARN")

    if not execution_role_arn:
        logger.error("SAGEMAKER_EXECUTION_ROLE_ARN environment variable not set")
        raise Exception("SAGEMAKER_EXECUTION_ROLE_ARN environment variable not set")

    try:
        sm_helper = SageMakerHelper(execution_role_arn=execution_role_arn)
        logger.info(f"Cleaning up resources for model: {model_name}")
        cleanup_results = sm_helper.delete_resources(
            model_name=model_name,
            endpoint_config_name=endpoint_config_name,
            endpoint_name=endpoint_name,
        )
    except Exception as e:
        logger.error(f"Error during SageMaker resource cleanup: {str(e)}", exc_info=True)

    return cleanup_results


def handler(event, context):
    """
    Lambda handler for cleaning up resources created during validation tests

    Args:
        event (dict): Lambda event
        context (object): Lambda context

    Returns:
        dict: Results of cleanup operations
    """
    logger.info(f"Cleanup resources handler - received event: {json.dumps(event)}")

    try:
        sm_results_key = "sageMakerCompatibilityResults"
        sm_compatibility_results = event.get(sm_results_key, {}).get("Payload", {}).get(sm_results_key)
        if not sm_compatibility_results:
            raise ValueError("Payload missing in the event")

        use_existing_endpoint = sm_compatibility_results.get("existingEndpointName", None)
        if use_existing_endpoint:
            logger.info("Using existing endpoint, no cleanup required")
            sagemaker_cleanup_results = {"cleanupResults": "No cleanup required"}
        else:
            preserve_endpoint = sm_compatibility_results.get("preserveEndpoint", False)
            logger.info(f"Preserve endpoint: {preserve_endpoint}")
            if not preserve_endpoint:
                model_name = sm_compatibility_results.get("modelName")
                endpoint_config_name = sm_compatibility_results.get("endpointConfigName")
                endpoint_name = sm_compatibility_results.get("endpointName")
                sagemaker_cleanup_results = cleanup_sagemaker_resources(model_name, endpoint_config_name, endpoint_name)
            else:
                logger.info("Preserving endpoint, no cleanup required")
                sagemaker_cleanup_results = {"cleanupResults": "No cleanup required"}

        return {
            "statusCode": 200,
            "cleanupResults": sagemaker_cleanup_results,
        }

    except Exception as e:
        logger.error(f"Error cleaning up resources: {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "error": str(e),
        }
