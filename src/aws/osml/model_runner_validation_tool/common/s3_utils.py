#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import boto3

# Get logger from the calling module
logger = logging.getLogger(__name__)


class S3Utils:
    """
    Utility class for common S3 operations used in the OSML Model Runner Validation Tool
    """

    def __init__(self, region_name: Optional[str] = None):
        """
        Initialize the S3Utils class with optional region

        Args:
            region_name (str, optional): AWS region name
        """
        self.s3_client = boto3.client("s3", region_name=region_name) if region_name else boto3.client("s3")

    def list_objects(self, bucket: str, prefix: str = "") -> List[str]:
        """
        List objects in an S3 bucket with optional prefix

        Args:
            bucket (str): S3 bucket name
            prefix (str, optional): Prefix to filter objects

        Returns:
            List[str]: List of object keys
        """
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket, Prefix=prefix)
            object_keys = [obj["Key"] for obj in response.get("Contents", [])]
            logger.info(f"Found {len(object_keys)} objects in bucket {bucket} with prefix '{prefix}'")
            return object_keys
        except Exception as e:
            logger.error(f"Error listing objects in bucket {bucket} with prefix '{prefix}': {e}")
            return []

    def get_object(self, bucket: str, key: str) -> Tuple[Optional[bytes], Optional[str]]:
        """
        Get an object from S3

        Args:
            bucket (str): S3 bucket name
            key (str): Object key

        Returns:
            Tuple[Optional[bytes], Optional[str]]: Tuple containing (file_bytes, error_message)
                If successful, error_message will be None
                If failed, file_bytes will be None and error_message will contain the error
        """
        try:
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            file_bytes = response["Body"].read()
            logger.info(f"Successfully downloaded object: {key} from bucket: {bucket}")
            return file_bytes, None
        except Exception as e:
            error_msg = f"Error downloading file {key} from bucket {bucket}: {e}"
            logger.warning(error_msg)
            return None, error_msg

    def put_object(self, bucket: str, key: str, data: Any, content_type: str = "application/json") -> bool:
        """
        Put an object in S3

        Args:
            bucket (str): S3 bucket name
            key (str): Object key
            data (Any): Data to store
            content_type (str, optional): Content type of the data

        Returns:
            bool: True if successful, False otherwise
        """
        try:
            # Convert data to string if it's a dict or other JSON-serializable object
            if isinstance(data, (dict, list)):
                data = json.dumps(data, indent=2)

            self.s3_client.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)
            logger.info(f"Successfully uploaded object to {bucket}/{key}")
            return True
        except Exception as e:
            logger.error(f"Error uploading object to {bucket}/{key}: {e}")
            return False

    def save_test_results(self, bucket: str, model_name: str, test_results: Dict[str, Any], test_type: str) -> bool:
        """
        Save test results to S3 with a standardized path format

        Args:
            bucket (str): S3 bucket name
            model_name (str): Name of the model
            test_results (Dict[str, Any]): Test results to save
            test_type (str): Type of test (e.g., 'oversight-ml-compatibility')

        Returns:
            bool: True if successful, False otherwise
        """
        if not bucket:
            logger.warning("Bucket name not provided, skipping S3 upload")
            return False

        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")
        key = f"{model_name}/{test_type}/{timestamp}.json"

        logger.info(f"Saving test results to S3: {bucket}/{key}")

        return self.put_object(bucket, key, test_results)
