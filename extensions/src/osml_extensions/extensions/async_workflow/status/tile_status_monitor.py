from aws.osml.model_runner.status import BaseStatusMonitor

class TileStatusMonitor(BaseStatusMonitor):
    def __init__(self, tile_status_topic: str):
        super().__init__(tile_status_topic)
        # todo