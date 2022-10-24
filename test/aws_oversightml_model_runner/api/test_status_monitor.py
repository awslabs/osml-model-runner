from unittest import mock

from configuration import TEST_ENV_CONFIG


@mock.patch.dict("os.environ", TEST_ENV_CONFIG, clear=True)
@mock.patch("botocore.session")
def test_status_monitor(mock_session):
    from aws_oversightml_model_runner.api.status_monitor import StatusMonitor

    mock_client = mock.MagicMock()
    mock_session.return_value = mock_client
    status_monitor = StatusMonitor("TEST-ENDPOINT")
    status_monitor.processing_event("test-job-id", "test-job-status", "test-description")

    status_monitor = StatusMonitor(None)  # if cp.client is null
    status_monitor.processing_event("test-job-id", "test-job-status", "test-description")
