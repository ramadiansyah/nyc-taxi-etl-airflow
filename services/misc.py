"""
date_handler.py

Provides utility function for determining the target year and month
for ETL operations based on DAG execution date and config settings.
"""

from datetime import timedelta
from utils.logger import get_logger

logger = get_logger()

def decide_year_month(config: dict, execution_date) -> tuple:
    """
    Determines the target year and month for data extraction based on configuration.

    The logic supports two modes:
    - "auto": calculates the target date by subtracting `n_months_back` from the DAG's execution date.
    - "manual": directly uses the year and month provided in the config.

    Args:
        config (dict): Configuration dictionary that must contain:
            - mode (str): Either "auto" or "manual".
            - n_months_back (int, str): Number of months to go back if in auto mode.
            - year (str): Comma-separated list of years (used in manual mode).
            - month (str): Comma-separated list of months (used in manual mode).
        execution_date (datetime.datetime): The logical execution date of the DAG run.

    Returns:
        tuple: A tuple of integers (year, month).

    Raises:
        ValueError: If the config mode is unsupported.
    """
    mode = config["mode"]
    if mode == "auto":
    #     n_back = int(config["n_months_back"])
    #     logger.info(f"execution_date : {execution_date}") 
    #     logger.info(f"n_months_back : {n_back}")
    #     target_date = execution_date - timedelta(days=30 * n_back)
    #     return target_date.year, target_date.month
                
        logger.info(f"execution_date : {execution_date}")
        n_back_month = int(config["n_back_month"])
    
        year = execution_date.year
        month = execution_date.month

        # Go back n months
        month -= n_back_month
        while month <= 0:
            month += 12
            year -= 1

        logger.info(f"Target year-month (n_back_month={n_back_month}): {year}-{month:02d}")
        
        return year, month
    elif mode == "manual":
        years = [int(y) for y in config["year"].split(",")]
        months = [int(m) for m in config["month"].split(",")]
        return years[0], months[0]
    raise ValueError("Unsupported mode in config.yaml")

