import unittest
from unittest import mock

import pytest
from botocore.stub import Stubber
from configuration import TEST_ENV_CONFIG


@mock.patch.dict("os.environ", TEST_ENV_CONFIG, clear=True)
class TestTimer(unittest.TestCase):
    def test_fetch_credentials_for_assumed_role_failure(self):
        from aws_oversightml_model_runner.common import credentials_utils
        from aws_oversightml_model_runner.common.exceptions import InvalidAssumedRoleException

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


if __name__ == "__main__":
    unittest.main()
