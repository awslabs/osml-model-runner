import argparse
import logging
import os


from aws_oversightml_model_runner import app


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

    app.monitor_work_queues()