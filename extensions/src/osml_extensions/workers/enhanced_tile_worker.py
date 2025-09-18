#  Copyright 2023-2024 Amazon.com, Inc. or its affiliates.

import logging
from typing import Dict

from aws.osml.model_runner.tile_worker import TileWorker

logger = logging.getLogger(__name__)


class EnhancedTileWorker(TileWorker):
    """
    Example enhanced TileWorker showing extension patterns.
    
    This minimal example demonstrates how to extend the base TileWorker
    with additional functionality while maintaining compatibility.
    """

    def process_tile(self, image_info: Dict, metrics=None) -> None:
        """
        Example enhanced tile processing with additional logging.
        """
        logger.debug(f"Enhanced tile processing for: {image_info.get('image_path', 'unknown')}")
        
        # Call parent implementation
        super().process_tile(image_info, metrics)