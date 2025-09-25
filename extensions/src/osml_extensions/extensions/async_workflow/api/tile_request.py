from dataclasses import dataclass
from typing import List

@dataclass
class TileRequest:
    tile_id: str
    region_id: str
    image_id: str
    job_id: str
    image_path: str
    region: List
