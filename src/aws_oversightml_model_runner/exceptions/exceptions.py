# JobTable Exceptions
class RetryableJobException(Exception):
    pass


class CompleteRegionException(Exception):
    pass


class IsImageCompleteException(Exception):
    pass


class StartImageException(Exception):
    pass


class EndImageException(Exception):
    pass


class GetImageRequestItemException(Exception):
    pass


# DDBHelper Exceptions
class DDBUpdateException(Exception):
    pass


# Status Monitor Exceptions
class StatusMonitorException(Exception):
    pass


# ModelRunner Exceptions
class AggregateFeaturesException(Exception):
    pass


class ProcessRegionException(Exception):
    pass


class LoadImageException(Exception):
    pass


class AddFeaturesException(Exception):
    pass


class ProcessImageException(Exception):
    pass


class SetupTileWorkersException(Exception):
    pass


class ProcessTilesException(Exception):
    pass


class UnsupportedModelException(Exception):
    pass


class InvalidImageURLException(Exception):
    pass


class InvalidImageRequestException(Exception):
    pass


class SNSPublishException(Exception):
    pass


class SelfThrottledRegionException(Exception):
    pass


class InvalidRegionRequestException(Exception):
    pass


class StartRegionException(Exception):
    pass
