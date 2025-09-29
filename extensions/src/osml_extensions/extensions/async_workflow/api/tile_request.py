import logging
from dataclasses import dataclass
from typing import List, Optional

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
    inference_id: str = ""  # SageMaker async inference ID
    output_location: str = "UNKNOWN"  # S3 output location for results
    model_invocation_role: str = ""
    tile_size: Optional[List[int]] = None
    tile_overlap: Optional[List[int]] = None
    model_invoke_mode: Optional[str] = None
    model_name: str = ""
    image_read_role: Optional[str] = None

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

        # Check tile bounds
        if not isinstance(self.tile_bounds, (list, tuple)):
            logger.error(
                f"Invalid tile_bounds in TileRequest: must be a list, got: {type(self.tile_bounds)}, {self.tile_bounds}"
            )
            return False

        if len(self.tile_bounds) != 2:
            logger.error("Invalid tile_bounds in TileRequest: must contain exactly 2 coordinate pairs")
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
            inference_id=data.get("inference_id", ""),
            output_location=data.get("output_location", "UNKNWON"),
            model_invocation_role=data.get("model_invocation_role"),
            tile_size=data.get("tile_size"),
            tile_overlap=data.get("tile_overlap"),
            model_invoke_mode=data.get("model_invoke_mode"),
            model_name=data.get("model_name"),
            image_read_role=data.get("image_read_role"),
        )

    @classmethod
    def from_tile_request_item(cls, tile_request_item):

        tile_request = cls.from_tile_request_dict(
            {
                "tile_id": tile_request_item.tile_id,
                "region_id": tile_request_item.region_id,
                "image_id": tile_request_item.image_id,
                "job_id": tile_request_item.job_id,
                "image_path": tile_request_item.image_path,
                "image_url": tile_request_item.image_url,
                "tile_bounds": tile_request_item.tile_bounds,
                "inference_id": tile_request_item.inference_id,
                "output_location": tile_request_item.output_location,
                "model_invocation_role": tile_request_item.model_invocation_role,
                "tile_size": tile_request_item.tile_size,
                "tile_overlap": tile_request_item.tile_overlap,
                "model_invoke_mode": str(tile_request_item.model_invoke_mode),
                "model_name": tile_request_item.model_name,
            }
        )
        return tile_request
