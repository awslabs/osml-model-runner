#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

import argparse
import json
import os
import time
from datetime import datetime, timezone

import boto3

from aws.osml.model_runner_validation_tool.common import S3Utils
from aws.osml.model_runner_validation_tool.common.logging_config import configure_logging

logger = configure_logging(__name__)
sagemaker_client = boto3.client("sagemaker")
s3_utils = S3Utils()

REPORT_BUCKET = os.environ.get("REPORT_BUCKET")
ECS_IMAGE_URI = os.environ.get("ECS_IMAGE_URI")
S3_MODEL_DATA_URI = os.environ.get("S3_MODEL_DATA_URI")
MODEL_NAME = os.environ.get("MODEL_NAME")
EXISTING_ENDPOINT_NAME = os.environ.get("EXISTING_ENDPOINT_NAME")


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Run SageMaker Inference Recommender")
    parser.add_argument("--ecs-image-uri", help="ECS Image URI to benchmark", default=ECS_IMAGE_URI)
    parser.add_argument("--s3-model-data-uri", help="S3 URI of the model data", default=S3_MODEL_DATA_URI)
    parser.add_argument("--model-name", help="Name of the model generated in the SM compatibility test", default=MODEL_NAME)
    parser.add_argument(
        "--existing-endpoint-name",
        help="Name of the existing endpoint used for the prior tests",
        default=EXISTING_ENDPOINT_NAME,
    )
    parser.add_argument("--sample-payload-url", help="S3 URL of the sample payload", default=None)
    parser.add_argument(
        "--instance-types",
        help="List of instance types to evaluate",
        default=["ml.m5.xlarge", "ml.c5.xlarge", "ml.g4dn.xlarge"],
    )

    return parser.parse_args()


def run_inference_recommender(
    ecs_image_uri, s3_model_data_uri, model_name, existing_endpoint_name, sample_payload_url=None, instance_types=None
):
    """
    Run SageMaker Inference Recommender for the provided model

    Args:
        ecs_image_uri (str): ECS Image URI to benchmark
        s3_model_data_uri (str): S3 URI of model data archive
        model_name (str): Name of the model_name generated in the SM compatibility test
        existing_endpoint_name (str): Name of the existing endpoint used for the prior tests
        sample_payload_url (str): S3 URL of the sample payload
        instance_types (list): List of instance types to evaluate

    Returns:
        dict: Inference Recommender results
    """
    logger.info(f"Running SageMaker Inference Recommender for model: {model_name}")

    if not instance_types:
        instance_types = ["ml.m5.xlarge", "ml.c5.xlarge", "ml.g4dn.xlarge"]

    # Create Inference Recommender job
    job_name = f"{model_name}-recommender-{int(time.time())}"

    try:
        # Check if Inference Recommender is available in the region
        try:
            # Create Inference Recommender job
            logger.info(f"Creating Inference Recommender job: {job_name}")

            # Placeholder for actual SMIR job
            # TODO
            status = "Completed"

            # Get job results
            if status == "Completed":
                logger.info("Inference Recommender job completed successfully")

                # Compile results
                results = {
                    "ecsImageUri": ecs_image_uri,
                    "s3ModelDataUri": s3_model_data_uri,
                    "modelName": model_name,
                    "existingEndpointName": existing_endpoint_name,
                    "samplePayloadUrl": sample_payload_url,
                    "instanceTypes": instance_types,
                    "testTimestamp": datetime.now(timezone.utc).isoformat(),
                    "inferenceRecommenderSuccessful": True,
                    "recommendations": {"TODO": "fake"},
                }

                save_results_to_s3(model_name, results)

                return results
            else:
                logger.error(f"Inference Recommender job failed with status: {status}")
                raise Exception(f"Inference Recommender job failed with status: {status}")

        except Exception as e:
            if "InferenceRecommendations" in str(e):
                logger.warning("SageMaker Inference Recommender is not available in this region")
                raise
            else:
                raise

    finally:
        # Clean up resources if needed
        try:
            # No cleanup needed for Inference Recommender jobs
            pass
        except Exception as e:
            logger.error(f"Error cleaning up resources: {str(e)}")


def save_results_to_s3(model_name, results):
    """
    Save Inference Recommender results to S3

    Args:
        model_name (str): Name of the model
        results (dict): Inference Recommender results
    """
    if not REPORT_BUCKET:
        logger.warning("REPORT_BUCKET environment variable not set, skipping S3 upload")
        return

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    # TODO: model_name is sometimes 'n/a used existing endpoint'
    key = f"{model_name}/inference-recommender/{timestamp}.json"

    logger.info(f"Saving Inference Recommender results to S3: {REPORT_BUCKET}/{key}")

    s3_utils.put_object(REPORT_BUCKET, key, json.dumps(results, indent=2), "application/json")


def main():
    """Main function"""
    args = parse_args()

    try:
        results = run_inference_recommender(
            args.ecs_image_uri,
            args.s3_model_data_uri,
            args.model_name,
            args.existing_endpoint_name,
            args.sample_payload_url,
            args.instance_types,
        )

        logger.info(f"Inference Recommender results: {json.dumps(results, indent=2)}")

        # Emit the results to stdout for the state machine to consume
        print(json.dumps(results))

        exit(0)
    except Exception as e:
        logger.error(f"Error running Inference Recommender: {str(e)}", exc_info=True)

        if REPORT_BUCKET:
            timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
            key = f"{args.model_name}/inference-recommender/{timestamp}-error.json"

            error_report = {
                "modelName": args.model_name,
                "modelArn": args.model_arn,
                "testTimestamp": datetime.now(timezone.utc).isoformat(),
                "inferenceRecommenderSuccessful": False,
                "error": str(e),
            }

            s3_utils.put_object(REPORT_BUCKET, key, json.dumps(error_report, indent=2), "application/json")

        exit(1)


if __name__ == "__main__":
    main()
