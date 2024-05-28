"""Reads In All required Configuration files"""

import logging.config
import logging
import pathlib
import atexit
import json


def load_logging_config():
    config_file = pathlib.Path("configs/log.json")
    with open(config_file) as f:
        config = json.load(f)

    return config


def setup_logging():
    log_config = load_logging_config()
    logging.config.dictConfig(log_config)
    queue_handler = logging.getHandlerByName("queue_handler")
    if queue_handler is not None:
        queue_handler.listener.start()
        atexit.register(queue_handler.listener.stop)


setup_logging()
logger = logging.getLogger("somelogger")


def main():
    logger.debug("debug message")
    logger.info("info message")
    logger.warning("warning message")
    logger.error("error message")
    logger.critical("critical message")
    try:
        raise Exception("Exception message")
    except Exception as e:
        logger.exception("exception message")


if __name__ == "__main__":
    main()
