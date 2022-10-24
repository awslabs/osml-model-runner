from enum import Enum

from aws_oversightml_model_runner.worker.image_utils import (
    VALID_IMAGE_COMPRESSION,
    VALID_IMAGE_FORMATS,
)


class ModelHostingOptions(str, Enum):
    """
    Enumeration defining the hosting options for CV models.
    """

    NONE = "NONE"
    SM_ENDPOINT = "SM_ENDPOINT"


VALID_MODEL_HOSTING_OPTIONS = [item.value for item in ModelHostingOptions]


def shared_properties_are_valid(request) -> bool:
    """
    There are some attributes that are shared between ImageRequests and RegionRequests. This
    function exists to
    :param request:
    :return:
    """
    if not request.image_id or not request.image_url:
        return False

    if not request.model_name:
        return False

    if (
        not request.model_hosting_type
        or request.model_hosting_type not in VALID_MODEL_HOSTING_OPTIONS
    ):
        return False

    if not request.tile_size or len(request.tile_size) != 2:
        return False

    if request.tile_size[0] <= 0 or request.tile_size[1] <= 0:
        return False

    if not request.tile_overlap or len(request.tile_overlap) != 2:
        return False

    if (
        request.tile_overlap[0] < 0
        or request.tile_overlap[0] >= request.tile_size[0]
        or request.tile_overlap[1] < 0
        or request.tile_overlap[1] >= request.tile_size[1]
    ):
        return False

    if not request.tile_format or request.tile_format not in VALID_IMAGE_FORMATS:
        return False

    if request.tile_compression and request.tile_compression not in VALID_IMAGE_COMPRESSION:
        return False

    if request.image_read_role and not request.image_read_role.startswith("arn:"):
        return False

    if request.model_invocation_role and not request.model_invocation_role.startswith("arn:"):
        return False

    return True
