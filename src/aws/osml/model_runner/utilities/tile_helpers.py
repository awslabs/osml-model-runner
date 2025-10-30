import json
import logging

from aws.osml.model_runner.exceptions import InvocationFailure

# Set up logging configuration
logger = logging.getLogger(__name__)


def parse_s3_event_for_output_location(s3_event_message: dict) -> str:
    """
    Parse S3 event notification to extract the output location (S3 URI).

    :param s3_event_message: S3 event notification message from SQS
    :return: S3 URI of the output location, or empty string if parsing fails

    messages:
        S3 message
            {
                'Records': [{
                    'eventVersion': '2.1',
                    'eventSource': 'aws:s3',
                    'awsRegion': 'us-west-2',
                    'eventTime': '2025-09-28T07:28:42.606Z',
                    'eventName': 'ObjectCreated:Put',
                    'userIdentity': {
                        'principalId': 'AWS:AROAZI2LIXEZ7G5XQA3QP:SageMaker'
                    },
                    'requestParameters': {
                        'sourceIPAddress': '10.2.11.121'
                    },
                    'responseElements': {
                        'x-amz-request-id': 'JT4J929BNCAZ6MZS',
                        'x-amz-id-2': 'oRE6zlp4M1...AHiOjvHDGG5'
                    },
                    's3': {
                        's3SchemaVersion': '1.0',
                        'configurationId': 'NzI1NzcwNDEtNTU0Mi00NzQ3LTk0YzktY2NiMjZjNTljYzJl',
                        'bucket': {
                            'name': 'modelrunner-infra-mrartifactbucketf483353e-x0nfaecdvfrr',
                            'ownerIdentity': {
                                'principalId': 'A244AJ6LIN4SSK'
                            },
                            'arn': 'arn:aws:s3:::modelrunner-infra-mrartifactbucketf483353e-x0nfaecdvfrr'
                        },
                        'object': {
                            'key': 'async-inference/output/e52dabe9-938b-4135-8baa-34af002548f6.out',
                            'size': 873,
                            'eTag': '52c8bd24d8391a30f3f87b609c973e31',
                            'sequencer': '0068D8E3AA906D1928'
                        }
                    }
                }]
            }

        sns message from sagemaker
            {
                'awsRegion': 'us-west-2',
                'eventTime': '2025-09-28T07:28:43.333Z',
                'receivedTime': '2025-09-28T07:28:43.203Z',
                'invocationStatus': 'Completed',
                'requestParameters': {
                    'accept': 'application/json',
                    'contentType': 'application/json',
                    'customAttributes': '{}',
                    'endpointName': 'Endpoint-control-model-3-dice-async',
                    'inputLocation': 's3://....0928_072843_8af55b3b'
                },
                'responseParameters': {
                    'contentType': 'text/html; charset=utf-8',
                    'outputLocation': 's3://....-8e93-758efa240cdf.out'
                },
                'inferenceId': '1d2071a5-65c5-40f8-ae06-f61367050695',
                'eventVersion': '1.0',
                'eventSource': 'aws:sagemaker',
                'eventName': 'InferenceResult'
            }

    """
    try:
        # Handle both direct S3 events and SNS-wrapped S3 events
        if "Records" in s3_event_message:
            # Direct S3 event
            records = []  # Turn this off to not get double messages s3_event_message["Records"]
            logger.debug(f"Processing event from S3: {s3_event_message}")
        elif "Message" in s3_event_message:
            # SNS-wrapped S3 event
            message = json.loads(s3_event_message["Message"])
            records = message.get("Records", [])
            logger.debug(f"Processing event from S3->SNS: {s3_event_message}")
        elif "responseParameters" in s3_event_message:
            if s3_event_message.get("invocationStatus") == "Failed":
                logger.error(f"Invocation failed for {s3_event_message}")
                raise InvocationFailure(s3_event_message.get("failureReason"))
            logger.debug(f"Processing event from SageMaker->SNS: {s3_event_message}")
            return s3_event_message["responseParameters"]["outputLocation"]
        else:
            logger.error(f"Unrecognized S3 event format: {s3_event_message}")
            return ""

        for record in records:
            if "s3" in record:
                s3_info = record["s3"]
                bucket_name = s3_info["bucket"]["name"]
                object_key = s3_info["object"]["key"]

                # Construct S3 URI
                output_location = f"s3://{bucket_name}/{object_key}"
                logger.debug(f"Extracted output location: {output_location}")
                return output_location

        logger.warning(f"No S3 records found in event: {s3_event_message}")
        return ""

    except Exception as e:
        logger.error(f"Error parsing S3 event: {e}")
        logger.debug(f"S3 event content: {s3_event_message}")
        return ""
