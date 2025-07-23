"""
logger.py

Utility module for configuring and retrieving a consistent logger instance.
Primarily used across Airflow tasks or other services to standardize logging format.
"""

import logging

def get_logger(name="airflow.task", level: int = logging.INFO) -> logging.Logger:
    """
    Returns a configured logger instance.

    If the logger with the given name has not been initialized before,
    this function will attach a stream handler with a standard formatter.
    The log level is set to INFO by default.

    Args:
        name (str): The name of the logger to retrieve. Defaults to "airflow.task".

    Returns:
        logging.Logger: A logger instance with the specified name.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("[%(asctime)s] %(levelname)s - %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(level)
    return logger
