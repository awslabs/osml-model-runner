
import traceback
import logging
from typing import Dict, Optional
import time
import boto3

from aws.osml.model_runner.app_config import ServiceConfig
from aws.osml.model_runner.inference.detector import Detector
from aws.osml.model_runner.inference.sm_detector import SMDetector
from aws.osml.model_runner.utilities import S3Manager, S3OperationError

logger = logging.getLogger(__name__)

S3_MANAGER = S3Manager()


class BatchSMDetector(SMDetector):
    def __init__(
        self,
        endpoint: str,
        assumed_credentials: Optional[Dict[str, str]] = None,
    ) -> None:
        """
        Initializes the AsyncSMDetector with async endpoint capabilities.

        :param endpoint: str = The name of the SageMaker async endpoint to invoke.
        :param assumed_credentials: Optional[Dict[str, str]] = Optional credentials for invoking the SageMaker model.
        """

        super().__init__(endpoint, assumed_credentials)  # type: ignore

        if assumed_credentials is not None:
            # Use the provided credentials to invoke SageMaker endpoints in another AWS account.
            self.sagemaker_client = boto3.client(
                "sagemaker",
                config=BotoConfig.sagemaker,
                aws_access_key_id=assumed_credentials.get("AccessKeyId"),
                aws_secret_access_key=assumed_credentials.get("SecretAccessKey"),
                aws_session_token=assumed_credentials.get("SessionToken"),
            )
        else:
            # Use the default role for this container if no specific credentials are provided.
            self.sagemaker_client = boto3.client("sagemaker", config=BotoConfig.sagemaker)

        # Initialize async configuration
        self.async_config = ServiceConfig.async_endpoint_config

        logger.debug(f"AsyncSMDetector initialized for endpoint: {endpoint}")

        # Validate S3 bucket access during initialization
        try:
            S3_MANAGER.validate_bucket_access()
        except S3OperationError as e:
            logger.warning(f"S3 bucket validation failed during initialization: {e}")
            # Don't fail initialization, but log the warning

    def _submit_batch_job(self, transform_job_name: str, input_s3_uri: str, output_s3_uri: str):

        transform_input = {
            "DataSource": {
                "S3DataSource": {
                    "S3DataType": "S3Prefix",
                    "S3Uri": input_s3_uri,
                }
            },
            "CompressionType": "None",
            "ContentType": "image/jpeg", # Adjust based on your image type (e.g., image/png)
            "SplitType": "None", # For individual image files, no splitting is typically needed
        }

        transform_output = {
            "S3OutputPath": output_s3_uri,
            "AssembleWith": "None", # If each image produces a separate output file
            "Accept": "application/json", # Or the content type your model outputs (e.g., application/json)
        }

        transform_resources = {
            "InstanceType": "ml.m4.xlarge", # Choose an appropriate instance type
            "InstanceCount": 1,
        }

        # Optional: Add environment variables if your inference script requires them
        environment_variables = {
            "SAGEMAKER_PROGRAM": "inference.py", # Name of your inference script if using a custom image
        }

        create_inputs = dict(
            TransformJobName=transform_job_name,
            ModelName=self.endpoint,
            TransformInput=transform_input,
            TransformOutput=transform_output,
            TransformResources=transform_resources,
            Environment=environment_variables, # Include if you have environment variables
        )


        response = sagemaker_client.create_transform_job(**create_inputs)
        logger.info(f"Transform Job created: {transform_job_name}")

class BatchSMDetectorBuilder:
    def __init__(self, endpoint: str, assumed_credentials: Optional[Dict[str, str]] = None): 
        self.endpoint = endpoint
        self.assumed_credentials = assumed_credentials or {}

    def build(self) -> Optional[Detector]:
        try:
            detector = BatchSMDetector(endpoint=self.endpoint, assumed_credentials=self.assumed_credentials)
            return detector
        except ExtensionConfigurationError:
            # Re-raise configuration errors
            raise
        except Exception as e:
            logger.error(f"Failed to create BatchSMDetector: {e}")
            logger.error(f"Traceback: {traceback.format_exc()}")
            # Return None to allow fallback handling
            return None
