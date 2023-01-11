from typing import Dict

import boto3

from aws_oversightml_model_runner.app_config import BotoConfig

from .exceptions import InvalidAssumedRoleException

sts_client = boto3.client("sts", config=BotoConfig.default)


def get_credentials_for_assumed_role(assumed_role: str) -> Dict[str, str]:
    try:
        assumed_invocation_role = sts_client.assume_role(
            RoleArn=assumed_role, RoleSessionName="AWSOversightMLModelRunner"
        )
        return assumed_invocation_role["Credentials"]
    except Exception as err:
        raise InvalidAssumedRoleException(
            f"Cannot assume role based on provided ARN {assumed_role}"
        ) from err
