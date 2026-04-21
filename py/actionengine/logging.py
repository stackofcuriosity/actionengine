import logging


def get_logger():
    if not hasattr(get_logger, "logger"):
        get_logger.logger = logging.getLogger("actionengine")
    return get_logger.logger
