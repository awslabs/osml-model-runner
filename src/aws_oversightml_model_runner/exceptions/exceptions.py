# JobTable Exceptions
class RetryableJobException(Exception):
    pass


class CompleteRegionFailed(Exception):
    pass


class IsImageCompleteFailed(Exception):
    pass


class StartImageFailed(Exception):
    pass


class ImageStatsFailed(Exception):
    pass


class EndImageFailed(Exception):
    pass


class GetJobItemFailed(Exception):
    pass


# DDBHelper Exceptions
class DDBUpdateFailed(Exception):
    pass


# ControlPlane Exceptions
class CPUpdateFailed(Exception):
    pass
