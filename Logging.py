import logging
import sys


def open_log(log_path):
    sys.stdout.write(f"Logging to {log_path}\n")
    logger = logging.getLogger(__name__)
    logging.basicConfig(filename=log_path,
                        level=logging.INFO,
                        format="%(asctime)s %(levelname)-8s %(message)s",
                        datefmt="%Y-%m-%d %H:%M:%S")