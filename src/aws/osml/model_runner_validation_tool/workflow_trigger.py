#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

import json
import logging
import os

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize boto3 client for Step Functions
sfn_client = boto3.client("stepfunctions")
STATE_MACHINE_ARN = os.environ["STATE_MACHINE_ARN"]


def handler(event, context):
    """
    Lambda function handler that processes SNS messages and starts a Step Functions workflow.

    Args:
        event (dict): The event dict containing the SNS message
        context (object): Lambda context object

    Returns:
        dict: Response containing execution details
    """
    try:
        logger.info(f"Workflow trigger handler - received event: {json.dumps(event)}")

        # Extract the message from the SNS event
        if "Records" in event and len(event["Records"]) > 0:
            sns_message = event["Records"][0]["Sns"]["Message"]

            # If the message is a string containing JSON, parse it
            if isinstance(sns_message, str):
                try:
                    payload = json.loads(sns_message)
                except json.JSONDecodeError:
                    logger.error("Failed to parse SNS message as JSON")
                    payload = {"message": sns_message}
            else:
                payload = sns_message

            logger.info(f"Extracted payload: {json.dumps(payload)}")

            # Start the Step Functions execution
            response = sfn_client.start_execution(stateMachineArn=STATE_MACHINE_ARN, input=json.dumps(payload))

            execution_arn = response["executionArn"]
            logger.info(f"Started Step Functions execution: {execution_arn}")

            return {
                "statusCode": 200,
                "body": json.dumps(
                    {"message": "Successfully started Step Functions execution", "executionArn": execution_arn}
                ),
            }
        else:
            logger.error("No SNS records found in the event")
            return {"statusCode": 400, "body": json.dumps({"message": "No SNS records found in the event"})}

    except Exception as e:
        logger.error(f"Error processing SNS message: {str(e)}")
        return {"statusCode": 500, "body": json.dumps({"message": f"Error processing SNS message: {str(e)}"})}
