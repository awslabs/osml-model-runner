import unittest
from math import radians

from aws_oversightml_model_runner.photogrammetry import GeodeticWorldCoordinate, SRTMTileSet


class TestSRTMDEMTileSet(unittest.TestCase):
    def test_ne_location(self):
        tile_set = SRTMTileSet()
        tile_path = tile_set.find_tile_id(GeodeticWorldCoordinate([radians(142), radians(3), 0.0]))
        assert "n03_e142_1arc_v3.tif" == tile_path

    def test_sw_location(self):
        tile_set = SRTMTileSet()
        tile_path = tile_set.find_tile_id(GeodeticWorldCoordinate([radians(-2), radians(-11), 0.0]))
        assert "s11_w002_1arc_v3.tif" == tile_path

    def test_zeros_and_overrides(self):
        tile_set = SRTMTileSet(prefix="CustomPrefix_", version="?", format_extension=".foo")
        tile_path = tile_set.find_tile_id(
            GeodeticWorldCoordinate([radians(0.0), radians(0.0), 0.0])
        )
        assert "CustomPrefix_n00_e000_?.foo" == tile_path


if __name__ == "__main__":
    unittest.main()
