"""
tasks.py

This module defines closure-based Python callables (task factories)
used in Airflow PythonOperator for decision-making logic. These tasks
include resolving the target year/month for the ETL and determining
the expected GCS file path for transformed data.

Functions:
    - decide_date_task
    - decide_gcs_url_file_task
"""

from airflow.models import TaskInstance
from airflow.models.taskinstance import TaskInstance
from airflow.utils.context import Context
from typing import Callable, Tuple, Dict

from services.misc import decide_year_month
from utils.logger import get_logger

logger = get_logger()

def decide_date_task(config: Dict) -> Callable[[Context], Tuple[int, int]]:
    """
    Factory function to generate a PythonOperator-compatible task function
    that determines the expected GCS URL of the transformed file.

    This function first attempts to retrieve the GCS path from XCom pushed
    by the transformation task. If not found (e.g., due to task rerun or skipped step),
    it reconstructs the GCS URL based on config and date context.

    The final GCS path is pushed back to XCom under the key `green_taxi_processed_filepath`.

    Args:
        config (Dict): Dictionary loaded from YAML config, containing:
            - gcs_bucket (str): GCS bucket name.
            - gcs_folder (str): Path prefix to data folder.

    Returns:
        Callable[[Context], str]: A closure function compatible with Airflow PythonOperator.
    """
    def _task(**kwargs) -> Tuple[int, int]:
        ti: TaskInstance = kwargs["ti"]
        execution_date = kwargs["logical_date"]
        
        # Assuming decide_year_month is defined elsewhere
        year, month = decide_year_month(config, execution_date)

        ti.xcom_push(key="year", value=year)
        ti.xcom_push(key="month", value=month)

        logger.info(f"year: {year}, month: {month}")

        return year, month  # useful for logging/debugging
    return _task



def decide_gcs_url_file_task(config: Dict) -> Callable[[Context], str]:
    def _task(**kwargs) -> str:
        ti: TaskInstance = kwargs["ti"]

        xcom_task_id = "transform_green_taxi"         
        xcom_key = "green_taxi_processed_filepath"

        gcs_url_file = ti.xcom_pull(task_ids=xcom_task_id, key=xcom_key)
        logger.info(f"GCS URL file: {gcs_url_file}")
        
        # ✅ Correct null check
        if not gcs_url_file:

            year = ti.xcom_pull(task_ids='decide_date', key='year')
            month = ti.xcom_pull(task_ids='decide_date', key='month')

            filename = f"green_tripdata_{year}-{int(month):02d}_transformed.parquet"
            gcs_filepath = f"{config['gcs_folder']}processed/{filename}"
            gcs_url_file = f"gs://{config['gcs_bucket']}/{gcs_filepath}"

            # Returned value was: gs://jcdeol004-bucket/processed/capstone3_rr/green_tripdata_2024-09_transformed.parquet

            logger.info(f"Created GCS URL file: {gcs_url_file}")

        ti.xcom_push(key=xcom_key, value=gcs_url_file)
        logger.info(f"pushed GCS URL file: {gcs_url_file}")

        return gcs_url_file  # useful for logging/debugging
    return _task

