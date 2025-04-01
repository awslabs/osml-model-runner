#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

import json
import os
from datetime import datetime, timezone

import boto3

from aws.osml.model_runner_validation_tool.common import S3Utils
from aws.osml.model_runner_validation_tool.common.logging_config import configure_logging

logger = configure_logging(__name__)

cloudwatch_client = boto3.client("cloudwatch")
s3_utils = S3Utils()


def compile_validation_report(event):
    """
    Compile validation test results into a comprehensive report

    Args:
        event (dict): Event containing test results

    Returns:
        dict: Compiled report
    """
    logger.info("Compiling model validation report")

    # TODO: 403   "oversightMLCompatibilityResults": "OSML Compatibility Test Skipped"
    # Gather all test results
    smc_results_key = "sageMakerCompatibilityResults"
    sagemaker_compatibility_results = event.get(smc_results_key, {}).get("Payload", {}).get(smc_results_key)

    omlc_results_key = "oversightMLCompatibilityResults"
    osml_compatibility_results = event.get(omlc_results_key)
    if osml_compatibility_results == "OSML Compatibility Test Skipped":
        osml_compatibility_results = None
    else:
        osml_compatibility_results = osml_compatibility_results.get("Payload", {}).get(omlc_results_key)

    # TODO: Still not getting results sent back from Fargate
    benchmarking_results = event.get("benchmarkingResults", {})
    inference_recommender_results = event.get("inferenceRecommenderResults", {})

    # Create comprehensive report
    report = {
        "modelInfo": event.get("modelInfo", {}),
        smc_results_key: sagemaker_compatibility_results,
        # TODO: Move test execution params into a nested testConfig key so it's easier to pass around
        "validationTestParams": {
            "runOversightMLCompatibilityTest": event.get("runOversightMLCompatibilityTest"),
            "runBenchmarkingTask": event.get("runBenchmarkingTask"),
            "runInferenceRecommender": event.get("runInferenceRecommender"),
            "preserveEndpoint": event.get("preserveEndpoint"),
            "existingEndpointName": sagemaker_compatibility_results.get("existingEndpointName"),
        },
        "reportTimestamp": datetime.now(timezone.utc).isoformat(),
    }

    # Add Oversight ML compatibility results if available
    if osml_compatibility_results:
        report[omlc_results_key] = osml_compatibility_results

    # Add benchmarking results if available
    if benchmarking_results:
        report["benchmarkingResults"] = benchmarking_results

    # Add inference recommender results if available
    if inference_recommender_results:
        report["inferenceRecommenderResults"] = inference_recommender_results

    # TODO: Compile combined summary report

    # Save report to S3
    model_name = sagemaker_compatibility_results.get("modelName")
    save_report_to_s3(model_name, report)

    # Create CloudWatch dashboard if configured
    create_cloudwatch_dashboard(model_name, report)

    return report


def save_report_to_s3(model_name, report):
    """
    Save compiled report to S3

    Args:
        model_name (str): Name of the model
        report (dict): Compiled report
    """
    report_bucket = os.environ.get("REPORT_BUCKET")

    if not report_bucket:
        logger.warning("REPORT_BUCKET environment variable not set, skipping S3 upload")
        return

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
    key = f"{model_name}/reports/{timestamp}-validation-report.json"

    logger.info(f"Saving validation report to S3: {report_bucket}/{key}")

    s3_utils.put_object(report_bucket, key, json.dumps(report, indent=2), "application/json")

    # Also save a copy as latest report
    latest_key = f"{model_name}/reports/latest-validation-report.json"
    s3_utils.put_object(report_bucket, latest_key, json.dumps(report, indent=2), "application/json")


def create_cloudwatch_dashboard(model_name, report):
    """
    Create CloudWatch dashboard for the model validation results

    Args:
        model_name (str): Name of the model
        report (dict): Compiled report
    """
    # This is a placeholder for creating a CloudWatch dashboard
    # In a real implementation, this would create a dashboard with widgets
    # showing the validation test results and metrics
    # TODO
    logger.info(f"Creating CloudWatch dashboard for model: {model_name}")

    # Example dashboard creation (not implemented)
    # dashboard_name = f"ModelValidation-{model_name}"
    # dashboard_body = create_dashboard_body(model_name, report)
    # cloudwatch_client.put_dashboard(
    #     DashboardName=dashboard_name,
    #     DashboardBody=dashboard_body
    # )


def handler(event, context):
    """
    Lambda handler for compiling validation report

    Args:
        event (dict): Lambda event
        context (object): Lambda context

    Returns:
        dict: Compiled report
    """
    logger.info(f"Compile report handler - received event: {json.dumps(event)}")

    try:
        report = compile_validation_report(event)

        return {
            "statusCode": 200,
            "validationTestReport": report,
        }
    except Exception as e:
        logger.error(f"Error compiling validation report: {str(e)}", exc_info=True)
        return {"statusCode": 500, "error": str(e), "validationSuccessful": False}
