import argparse
import logging
import os

from codeguru_profiler_agent import Profiler

from aws_model_runner import model_runner
from aws_model_runner.metrics import configure_metrics, start_metrics, stop_metrics

CODEGURU_PROFILING_GROUP = os.environ.get("CODEGURU_PROFILING_GROUP")


def configure_logging(verbose: bool):
    """
    Setup logging for this application
    """
    logging_level = logging.INFO
    if verbose:
        logging_level = logging.DEBUG

    root_logger = logging.getLogger()
    root_logger.setLevel(logging_level)

    ch = logging.StreamHandler()
    ch.setLevel(logging_level)
    formatter = logging.Formatter("%(levelname)-8s %(message)s")
    ch.setFormatter(formatter)

    root_logger.addHandler(ch)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args()

    configure_logging(args.verbose)

    configure_metrics("AIP/ModelRunner", "cw")
    if CODEGURU_PROFILING_GROUP:
        Profiler(profiling_group_name=CODEGURU_PROFILING_GROUP).start()
    start_metrics()
    model_runner.monitor_work_queues()
    stop_metrics()
