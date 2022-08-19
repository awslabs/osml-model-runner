from typing import Dict

import boto3

from aws_oversightml_model_runner.utils.constants import BOTO_CONFIG

sts_client = boto3.client("sts", config=BOTO_CONFIG)


def get_credentials_for_assumed_role(execution_role: str) -> Dict[str, str]:
    assumed_invocation_role = sts_client.assume_role(
        RoleArn=execution_role, RoleSessionName="AWSOversightMLModelRunner"
    )
    return assumed_invocation_role["Credentials"]
