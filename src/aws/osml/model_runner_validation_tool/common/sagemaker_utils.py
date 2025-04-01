#  Copyright 2023-2025 Amazon.com, Inc. or its affiliates.

"""
SageMaker utility functions for OSML Model Runner Validation Tool.
"""

import os
import time
from datetime import datetime, timezone

import boto3

from aws.osml.model_runner_validation_tool.common.logging_config import configure_logging

logger = configure_logging(__name__)


class SageMakerHelper:
    """
    Helper class for SageMaker operations like creating models, endpoint configs, and endpoints.
    """

    def __init__(self, ecs_image_uri=None, execution_role_arn=None, region_name=None):
        """
        Initialize the SageMaker helper.

        Args:
            execution_role_arn (str, optional): SageMaker execution role ARN. If not provided,
                                               will try to get from environment variable.
            region_name (str, optional): AWS region name. If not provided, boto3 will use default.
        """
        self.ecs_image_uri = ecs_image_uri
        if ecs_image_uri:
            self.model_name = self.generate_unique_name(ecs_image_uri.split("/")[-1])

        self.sagemaker_client = boto3.client("sagemaker", region_name=region_name)

        self.execution_role_arn = execution_role_arn or os.environ.get("SAGEMAKER_EXECUTION_ROLE_ARN")
        if not self.execution_role_arn:
            logger.warning("SageMaker execution role ARN not provided and not found in environment variables")

    def create_model(self, model_data_uri=None, environment_vars=None):
        """
        Create a SageMaker model.

        Args:
            model_data_uri (str, optional): S3 URI of the model artifacts
            environment_vars (dict, optional): Environment variables for the model container

        Returns:
            str: The name of the created model
        """
        if not self.execution_role_arn:
            raise ValueError("SageMaker execution role ARN is required to create a model")

        if not self.ecs_image_uri:
            raise ValueError("ECS image URI is required to create a model")

        primary_container = {
            "Image": self.ecs_image_uri,
        }
        if model_data_uri:
            primary_container["ModelDataUrl"] = model_data_uri
        if environment_vars:
            primary_container["Environment"] = environment_vars

        logger.info(f"Creating SageMaker model: {self.model_name}")
        self.sagemaker_client.create_model(
            ModelName=self.model_name, ExecutionRoleArn=self.execution_role_arn, PrimaryContainer=primary_container
        )

        return self.model_name

    def create_endpoint_config(self, instance_type="ml.m5.xlarge", instance_count=1, variant_name="AllTraffic"):
        """
        Create a SageMaker endpoint configuration.

        Args:
            instance_type (str, optional): ML instance type. Defaults to 'ml.m5.xlarge'
            instance_count (int, optional): Number of instances. Defaults to 1
            variant_name (str, optional): Name of the production variant. Defaults to 'AllTraffic'

        Returns:
            str: The name of the created endpoint configuration
        """

        if not self.ecs_image_uri:
            raise ValueError("ECS image URI is required to create an endpoint configuration")

        endpoint_config_name = f"{self.model_name}-sm-compat-endpoint-cfg"
        logger.info(f"Creating SageMaker endpoint configuration: {endpoint_config_name}")
        self.sagemaker_client.create_endpoint_config(
            EndpointConfigName=endpoint_config_name,
            ProductionVariants=[
                {
                    "VariantName": variant_name,
                    "ModelName": self.model_name,
                    "InitialInstanceCount": instance_count,
                    "InstanceType": instance_type,
                }
            ],
        )

        return endpoint_config_name

    def create_endpoint(self, endpoint_config_name, wait_for_completion=True, max_wait_time_minutes=10):
        """
        Create a SageMaker endpoint and optionally wait for it to be in service.

        Args:
            endpoint_config_name (str): Name of the endpoint configuration to use
            wait_for_completion (bool, optional): Whether to wait for the endpoint to be in service.
                                                Defaults to True
            max_wait_time_minutes (int, optional): Maximum time to wait in minutes. Defaults to 10

        Returns:
            dict: Information about the endpoint creation including timing data
        """

        if not self.ecs_image_uri:
            raise ValueError("ECS image URI is required to create an endpoint")

        start_endpoint_timestamp = datetime.now(timezone.utc)

        endpoint_name = f"{self.model_name}-sm-compat-endpoint"
        logger.info(f"Creating SageMaker endpoint: {endpoint_name}")
        self.sagemaker_client.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=endpoint_config_name)

        endpoint_info = {
            "endpointName": endpoint_name,
            "endpointConfigName": endpoint_config_name,
            "creationStartTime": start_endpoint_timestamp.isoformat(),
        }

        if wait_for_completion:
            logger.info(f"Waiting for endpoint '{endpoint_name}' to be in service...")
            waiter = self.sagemaker_client.get_waiter("endpoint_in_service")

            waiter_config = {"Delay": 10, "MaxAttempts": max_wait_time_minutes * 6}  # 6 attempts per minute

            waiter.wait(EndpointName=endpoint_name, WaiterConfig=waiter_config)
            logger.info(f"Endpoint '{endpoint_name}' is now in service.")

            end_endpoint_timestamp = datetime.now(timezone.utc)
            time_to_in_service = (end_endpoint_timestamp - start_endpoint_timestamp).total_seconds()

            endpoint_info.update(
                {"inServiceTime": end_endpoint_timestamp.isoformat(), "timeToInService": time_to_in_service}
            )

        return endpoint_info

    def delete_resources(self, model_name=None, endpoint_config_name=None, endpoint_name=None):
        """
        Delete SageMaker resources.

        Args:
            model_name (str, optional): Name of the model to delete
            endpoint_config_name (str, optional): Name of the endpoint configuration to delete
            endpoint_name (str, optional): Name of the endpoint to delete
        """
        cleanup_results = {
            "model": None,
            "endpointConfig": None,
            "endpoint": None,
        }

        if endpoint_name:
            logger.info(f"Deleting SageMaker endpoint: {endpoint_name}")
            try:
                self.sagemaker_client.delete_endpoint(EndpointName=endpoint_name)
                cleanup_results["endpoint"] = f"{endpoint_name} deleted"
            except Exception as e:
                logger.error(f"Error deleting endpoint {endpoint_name}: {str(e)}")

        if endpoint_config_name:
            logger.info(f"Deleting SageMaker endpoint configuration: {endpoint_config_name}")
            try:
                self.sagemaker_client.delete_endpoint_config(EndpointConfigName=endpoint_config_name)
                cleanup_results["endpointConfig"] = f"{endpoint_config_name} deleted"
            except Exception as e:
                logger.error(f"Error deleting endpoint config {endpoint_config_name}: {str(e)}")

        if model_name:
            logger.info(f"Deleting SageMaker model: {model_name}")
            cleanup_results["model"] = f"{model_name} deleted"
            try:
                self.sagemaker_client.delete_model(ModelName=model_name)
            except Exception as e:
                logger.error(f"Error deleting model {model_name}: {str(e)}")

        return cleanup_results

    @staticmethod
    def generate_unique_name(base_name):
        """
        Generate a unique name with timestamp suffix. The base_name will be truncated at 24 characters,
        so the longest name returned will be 35 characters.

        Args:
            base_name (str): Base name to use

        Returns:
            str: Unique name with timestamp suffix.
        """
        return f"{base_name[:24]}-{int(time.time())}"
