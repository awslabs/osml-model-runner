import logging
import math
import operator
from typing import Optional

import boto3
from cachetools import TTLCache, cachedmethod

from aws_oversightml_model_runner.app_config import BotoConfig, ServiceConfig
from aws_oversightml_model_runner.common.credentials_utils import get_credentials_for_assumed_role

logger = logging.getLogger(__name__)


class EndpointUtils:
    def __init__(self) -> None:
        # We want an accurate estimation of max regions but we also don't want
        # to do this caclulation every single time we process a region request
        # (especially if the requests are processing very quickly) so the region
        # calculation is done at most every 60 second per container per endpoint
        self.max_region_cache: TTLCache = TTLCache(maxsize=10, ttl=60)
        self.ec2_client = boto3.client("ec2", config=BotoConfig.sagemaker)

    @cachedmethod(operator.attrgetter("max_region_cache"))
    def calculate_max_regions(
        self, endpoint_name: str, model_invocation_role: Optional[str] = None
    ) -> int:
        """
        Calculates the max number of in-process regions for a given model endpoint. In
        order to calculate the max regions we get the count of instances currently
        running for the endpoint as well as the instance type. We then get the number
        of vcpus for the instance type and calculate the max regions as:

            math.floor(10 * VCPUs * Number of Instances) / Workers Per CPU

        :param endpoint_name: The name of the endpoint
        :param model_invocation_credentials: Role used to invoke the model which
            will be used here to describe the endpoint and config
        :returns: max endpoint count for the endpoint
        """
        assumed_credentials = None
        if model_invocation_role:
            assumed_credentials = get_credentials_for_assumed_role(model_invocation_role)
        if assumed_credentials is not None:
            sm_client = boto3.client(
                "sagemaker",
                config=BotoConfig.sagemaker,
                aws_access_key_id=assumed_credentials.get("AccessKeyId"),
                aws_secret_access_key=assumed_credentials.get("SecretAccessKey"),
                aws_session_token=assumed_credentials.get("SessionToken"),
            )
        else:
            sm_client = boto3.client("sagemaker", config=BotoConfig.sagemaker)

        # This works for single model endpoints. Multimodel endpoints will need to
        # account for each variant and the weight of those variants
        endpoint_desc = sm_client.describe_endpoint(EndpointName=endpoint_name)
        variant = endpoint_desc["ProductionVariants"][0]
        current_instance_count = variant["CurrentInstanceCount"]

        endpoint_config_desc = sm_client.describe_endpoint_config(
            EndpointConfigName=endpoint_desc["EndpointConfigName"]
        )
        instance_type = endpoint_config_desc["ProductionVariants"][0]["InstanceType"].replace(
            "ml.", ""
        )
        instance_desc = self.ec2_client.describe_instance_types(InstanceTypes=[instance_type])
        vcpus = instance_desc["InstanceTypes"][0]["VCpuInfo"]["DefaultVCpus"]
        max_regions = math.floor(
            (int(ServiceConfig.throttling_vcpu_scale_factor) * vcpus * current_instance_count)
            / int(ServiceConfig.workers_per_cpu)
        )
        logger.info(
            f"Max regions for endpoint: {endpoint_name} calculated to be {max_regions}."
            f"(ScaleFactor: {ServiceConfig.throttling_vcpu_scale_factor}, InstanceType:{instance_type},"
            f"VCPU Count:{vcpus}, WorkersPerCPU: {ServiceConfig.workers_per_cpu}"
        )

        return max_regions
