import logging
import os
from logging.handlers import RotatingFileHandler


def get_logger():
    """
     Creates a rotating log
     """
    _logger_ = logging.getLogger('')
    # setting the logger level
    _logger_.setLevel(os.environ.get('LOG_LEVEL', default="INFO"))
    # creating the format for the log
    log_formatter = "%(asctime)s-%(levelname)-s-[%(funcName)5s():%(lineno)s]-%(message)s"
    time_format = "%Y-%m-%d %H:%M:%S"
    # getting the path for the logger
    file_path = "logs"
    # setting the format
    formatter = logging.Formatter(log_formatter, time_format)
    # creating the folder if not exist
    if not os.path.exists(file_path):
        os.makedirs(file_path)
    # joining the path
    log_file = os.path.join(f"{file_path}/application.log")
    # creating rotating file handler with max byte as 1000000000
    temp_handler = RotatingFileHandler(log_file, maxBytes=1000000000, backupCount=5)
    # setting the formatter
    temp_handler.setFormatter(formatter)
    # setting the handler
    _logger_.addHandler(temp_handler)
        # added stream handler
    temp_handler = logging.StreamHandler()
        # setting the handler
    _logger_.addHandler(temp_handler)
    return _logger_


logger = get_logger()
