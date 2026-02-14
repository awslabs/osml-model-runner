#  Copyright 2026 Amazon.com, Inc. or its affiliates.

import tempfile
from queue import Queue
from unittest.mock import Mock

import pytest

from aws.osml.model_runner.common import TileState
from aws.osml.model_runner.tile_worker.tile_worker import TileWorker


@pytest.fixture
def worker_setup():
    feature_detector = Mock()
    feature_table = Mock()
    region_request_table = Mock()
    worker = TileWorker(Queue(), feature_detector, None, feature_table, region_request_table)
    return worker, feature_detector, feature_table, region_request_table


def test_flush_tile_updates_batches_by_region_and_state(worker_setup):
    worker, _, _, region_request_table = worker_setup

    worker.buffer_tile_update(
        {"image_id": "img-1", "region_id": "reg-1", "region": ((0, 0), (256, 256))},
        TileState.SUCCEEDED,
    )
    worker.buffer_tile_update(
        {"image_id": "img-1", "region_id": "reg-1", "region": ((256, 0), (512, 256))},
        TileState.SUCCEEDED,
    )
    worker.buffer_tile_update(
        {"image_id": "img-1", "region_id": "reg-1", "region": ((512, 0), (768, 256))},
        TileState.FAILED,
    )

    worker.flush_tile_updates()

    assert region_request_table.add_tiles.call_count == 2
    region_request_table.add_tile.assert_not_called()
    assert len(worker._buffered_tile_updates) == 0


def test_flush_tile_updates_raises_when_batch_write_fails(worker_setup):
    worker, _, _, region_request_table = worker_setup
    region_request_table.add_tiles.side_effect = Exception("batch failed")

    worker.buffer_tile_update(
        {"image_id": "img-1", "region_id": "reg-1", "region": ((0, 0), (256, 256))},
        TileState.SUCCEEDED,
    )

    with pytest.raises(Exception):
        worker.flush_tile_updates()

    region_request_table.add_tiles.assert_called_once()
    region_request_table.add_tile.assert_not_called()


def test_process_tile_buffers_status_and_does_not_write_immediately(worker_setup):
    worker, feature_detector, _, region_request_table = worker_setup
    feature_detector.endpoint = "test-endpoint"
    feature_detector.find_features.return_value = {"features": []}

    with tempfile.NamedTemporaryFile() as temp_file:
        image_info = {
            "image_path": temp_file.name,
            "image_id": "img-1",
            "region_id": "reg-1",
            "region": ((0, 0), (256, 256)),
        }
        worker.process_tile(image_info)

    region_request_table.add_tiles.assert_not_called()
    region_request_table.add_tile.assert_not_called()
    assert len(worker._buffered_tile_updates) == 1


def test_run_handles_flush_failure_and_exits_cleanly(mocker):
    mock_set_context = mocker.patch("aws.osml.model_runner.tile_worker.tile_worker.ThreadingLocalContextFilter.set_context")
    work_queue = Queue()
    work_queue.put(None)
    feature_detector = Mock()
    feature_detector.request_count = 0
    worker = TileWorker(work_queue, feature_detector, None, Mock(), Mock())
    worker.flush_tile_updates = Mock(side_effect=Exception("batch failed"))

    worker.run()

    worker.flush_tile_updates.assert_called_once()
    mock_set_context.assert_called_once_with(None)
