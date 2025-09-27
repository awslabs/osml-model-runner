import logging
from dataclasses import dataclass
from typing import List
from pathlib import Path

logger = logging.getLogger(__name__)


@dataclass
class TileRequest:
    tile_id: str
    region_id: str
    image_id: str
    job_id: str
    image_path: str  # path to tile image
    image_url: str  # path to full image in S3
    tile_bounds: List

    def is_valid(self) -> bool:
        """
        Check if this tile request contains required attributes and meaningful values.

        :return: True if the request contains all mandatory attributes with acceptable values, False otherwise.
        """
        # Check for required string fields
        if not self.tile_id or not isinstance(self.tile_id, str):
            logger.error("Invalid tile_id in TileRequest: must be a non-empty string")
            return False

        if not self.region_id or not isinstance(self.region_id, str):
            logger.error("Invalid region_id in TileRequest: must be a non-empty string")
            return False

        if not self.image_id or not isinstance(self.image_id, str):
            logger.error("Invalid image_id in TileRequest: must be a non-empty string")
            return False

        if not self.job_id or not isinstance(self.job_id, str):
            logger.error("Invalid job_id in TileRequest: must be a non-empty string")
            return False

        if not self.image_path or not isinstance(self.image_path, str):
            logger.error("Invalid image_path in TileRequest: must be a non-empty string")
            return False

        if not self.image_url.startswith("s3://"):
            logger.error(f"Invalid image_url format in TileRequest: {self.image_url}")

        # Basic validation for image path (check if it's a reasonable path format)
        try:
            # Accept both S3 paths and local file paths
            if self.image_url.startswith("s3://") or Path(self.image_path).is_absolute() or "/" in self.image_path:
                logger.error(f"Invalid image_url format in TileRequest: {self.image_path}")
                return False
        except Exception as e:
            logger.error(f"Error validating image_path in TileRequest: {e}")
            return False

        # Check region bounds
        if not isinstance(self.region, list):
            logger.error("Invalid region in TileRequest: must be a list")
            return False

        if len(self.region) != 2:
            logger.error("Invalid region in TileRequest: must contain exactly 2 coordinate pairs")
            return False

        # Validate region bounds format: [[x1, y1], [x2, y2]]
        try:
            for i, coord_pair in enumerate(self.region):
                if not isinstance(coord_pair, list) or len(coord_pair) != 2:
                    logger.error(f"Invalid region coordinate pair {i} in TileRequest: must be [x, y]")
                    return False

                if not all(isinstance(coord, (int, float)) for coord in coord_pair):
                    logger.error(f"Invalid region coordinates in pair {i}: must be numeric")
                    return False

            # Basic sanity check: ensure bounds make sense (x2 > x1, y2 > y1)
            x1, y1 = self.region[0]
            x2, y2 = self.region[1]

            if x2 <= x1 or y2 <= y1:
                logger.error("Invalid region bounds in TileRequest: second coordinate must be greater than first")
                return False

        except (IndexError, TypeError, ValueError) as e:
            logger.error(f"Error validating region bounds in TileRequest: {e}")
            return False

        return True

    @classmethod
    def from_tile_request_dict(cls, data):
        """
        Create a TileRequest from a dictionary (typically from TileRequestItem).

        :param data: Dictionary containing tile request data
        :return: TileRequest instance
        """
        return cls(
            tile_id=data.get("tile_id", ""),
            region_id=data.get("region_id", ""),
            image_id=data.get("image_id", ""),
            job_id=data.get("job_id", ""),
            image_path=data.get("image_path", ""),
            image_url=data.get("image_url", ""),
            tile_bounds=data.get("tile_bounds", []),
        )
