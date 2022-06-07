from typing import Dict

import boto3

sts_client = boto3.client("sts")


def get_credentials_for_assumed_role(execution_role: str) -> Dict[str, str]:
    assumed_invocation_role = sts_client.assume_role(
        RoleArn=execution_role, RoleSessionName="AWSOversightMLModelRunner"
    )
    return assumed_invocation_role["Credentials"]
