from utils.logger import get_logger

logger = get_logger()

def fail_task():
    """
    Simulates a task failure by logging an error and raising a ValueError.

    This function is commonly used in Airflow DAGs to intentionally fail
    a task for testing failure callbacks, alerting systems, or retry behavior.

    Logs an error message before raising the exception.

    Raises:
        ValueError: Always raised to simulate a task failure.
    """
    logger.error("❌ fail_task triggered: Simulating failure...")
    raise ValueError("Simulasi error")
