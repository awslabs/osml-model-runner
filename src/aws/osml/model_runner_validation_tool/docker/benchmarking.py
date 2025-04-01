#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

import argparse
import json
import os
from datetime import datetime, timezone

import boto3

from aws.osml.model_runner_validation_tool.common import S3Utils
from aws.osml.model_runner_validation_tool.common.logging_config import configure_logging

logger = configure_logging(__name__)

cloudwatch_client = boto3.client("cloudwatch", region_name=os.environ.get("AWS_REGION"))
sagemaker_client = boto3.client("sagemaker", region_name=os.environ.get("AWS_REGION"))
s3_utils = S3Utils()

REPORT_BUCKET = os.environ.get("REPORT_BUCKET")
ECS_IMAGE_URI = os.environ.get("ECS_IMAGE_URI")
S3_MODEL_DATA_URI = os.environ.get("S3_MODEL_DATA_URI")
MODEL_NAME = os.environ.get("MODEL_NAME")
EXISTING_ENDPOINT_NAME = os.environ.get("EXISTING_ENDPOINT_NAME")


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Run model benchmarking tests")
    parser.add_argument("--ecs-image-uri", help="ECS Image URI to benchmark", default=ECS_IMAGE_URI)
    parser.add_argument("--s3-model-data-uri", help="S3 URI of the model data", default=S3_MODEL_DATA_URI)
    parser.add_argument("--model-name", help="Name of the model generated in the SM compatibility test", default=MODEL_NAME)
    parser.add_argument(
        "--existing-endpoint-name",
        help="Name of the existing endpoint used for the prior tests",
        default=EXISTING_ENDPOINT_NAME,
    )
    return parser.parse_args()


def run_benchmarking_tests(ecs_image_uri: str, s3_model_data_uri: str, model_name: str, existing_endpoint_name: str):
    """
    Run benchmarking tests on the provided model

    Args:
        ecs_image_uri (str): ECS Image URI to benchmark
        s3_model_data_uri (str): S3 URI of model data archive or None
        model_name (str): Name of the model_name generated in the SM compatibility test
        existing_endpoint_name (str): Name of the existing endpoint used for the prior tests or None

    Returns:
        dict: Benchmarking results
    """
    message = "Running benchmarking tests"
    if existing_endpoint_name != "None":
        message += f" using existing endpoint: {existing_endpoint_name}"
    elif model_name:
        message += f" using model: {model_name}"
    logger.info(message)

    try:
        # TODO - This is a placeholder for actual benchmarking logic

        benchmarking_results = {
            "ecsImageUri": ecs_image_uri,
            "s3ModelDataUri": s3_model_data_uri,
            "modelName": model_name,
            "existingEndpointName": existing_endpoint_name,
            "testTimestamp": datetime.now(timezone.utc).isoformat(),
            "benchmarkingSuccessful": True,
            "results": {"TODO": True},
        }

        save_results_to_s3(model_name, benchmarking_results)

        return benchmarking_results

    finally:
        # Clean up resources
        try:
            # TODO
            pass
        except Exception as e:
            logger.error(f"Error cleaning up resources: {str(e)}")


def save_results_to_s3(model_name, results):
    """
    Save benchmarking results to S3

    Args:
        model_name (str): Name of the model
        results (dict): Benchmarking results
    """
    if not REPORT_BUCKET:
        logger.warning("REPORT_BUCKET environment variable not set, skipping S3 upload")
        return

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    key = f"{model_name}/benchmarking/{timestamp}.json"

    logger.info(f"Saving benchmarking results to S3: {REPORT_BUCKET}/{key}")

    s3_utils.put_object(REPORT_BUCKET, key, json.dumps(results, indent=2), "application/json")


def main():
    """Main function"""
    args = parse_args()

    try:
        results = run_benchmarking_tests(
            args.ecs_image_uri,
            args.s3_model_data_uri,
            args.model_name,
            args.existing_endpoint_name,
        )

        logger.info(f"Benchmarking results: {json.dumps(results, indent=2)}")

        # Emit the results to stdout for the state machine to consume
        print(json.dumps(results))

        exit(0)
    except Exception as e:
        logger.error(f"Error running benchmarking tests: {str(e)}", exc_info=True)

        if REPORT_BUCKET:
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
            key = f"{args.model_name}/benchmarking/{timestamp}-error.json"

            error_report = {
                "ecsImageUri": args.ecs_image_uri,
                "s3ModelDataUri": args.s3_model_data_uri,
                "modelName": args.model_name,
                "existingEndpointName": args.existing_endpoint_name,
                "testTimestamp": datetime.now(timezone.utc).isoformat(),
                "benchmarkingSuccessful": False,
                "error": str(e),
            }

            s3_utils.put_object(REPORT_BUCKET, key, json.dumps(error_report, indent=2), "application/json")

        exit(1)


if __name__ == "__main__":
    main()
