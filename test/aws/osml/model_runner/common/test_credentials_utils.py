#  Copyright 2023-2026 Amazon.com, Inc. or its affiliates.

import pytest
from botocore.stub import Stubber


def test_fetch_credentials_for_assumed_role_failure():
    from aws.osml.model_runner.common import credentials_utils
    from aws.osml.model_runner.common.exceptions import InvalidAssumedRoleException

    assume_role = "arn:aws:iam::012345678910:role/OversightMLBetaInvokeRole"
    sts_client_stub = Stubber(credentials_utils.sts_client)
    sts_client_stub.activate()
    sts_client_stub.add_client_error(
        "assume_role",
        service_error_code="InvalidIdentityTokenException",
        service_message="InvalidIdentityTokenException",
        expected_params={},
    )
    with pytest.raises(InvalidAssumedRoleException):
        credentials_utils.get_credentials_for_assumed_role(assume_role)
    sts_client_stub.deactivate()
