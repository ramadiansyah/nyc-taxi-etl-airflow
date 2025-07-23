"""
transform_operator.py

This module defines a custom Airflow operator that reads raw NYC Green Taxi data
from GCS, transforms it using predefined business logic, and writes the cleaned
data back to GCS in a processed folder.
"""

import os
from airflow.models import BaseOperator
from airflow.utils.context import Context

from datetime import datetime, timezone

from services.loader import df_to_parquet_gcs_direct 
from services.transformer import read_parquet_from_gcs, transform_taxi_data

class TransformGreenTaxiOperator(BaseOperator):
    """
    Custom Airflow Operator to read, transform, and store NYC Green Taxi data.

    This operator retrieves a raw Parquet file from GCS, performs data cleaning and transformation,
    and writes the resulting dataset back to a separate GCS path. It also pushes the processed file
    path to XCom for downstream tasks to consume.

    Attributes:
        config (dict): Configuration dictionary containing GCS bucket, folder paths, etc.

    Methods:
        execute(context): Executes the data transformation and stores the processed data.
    """
    def __init__(self, config, *args, **kwargs):
        """
        Initializes the TransformGreenTaxiOperator with the provided configuration.

        Args:
            config (dict): A dictionary containing:
                - gcs_bucket (str): Name of the GCS bucket.
                - gcs_folder (str): Base GCS folder path for raw and processed data.
            *args: Additional positional arguments for BaseOperator.
            **kwargs: Additional keyword arguments for BaseOperator.
        """
        super().__init__(*args, **kwargs)
        self.config = config

    def execute(self, context: Context):
        """
        Executes the transformation task.

        Steps:
        1. Pulls `year` and `month` from XCom (output of `decide_date` task).
        2. Builds the raw GCS file path and loads it into a DataFrame.
        3. Transforms the raw data using `transform_taxi_data`.
        4. Saves the transformed DataFrame as a Parquet file to the processed GCS folder.
        5. Pushes the final GCS URI to XCom for later use (e.g., by the load task).

        Args:
            context (Context): The execution context provided by Airflow.

        Raises:
            FileNotFoundError: If the raw Parquet file does not exist in GCS.
            Exception: For any errors during transformation or file writing.

        Returns:
            None
        """
        run_date = context.get("logical_date") or datetime.now(timezone.utc)

        ti = context["ti"]
        year = ti.xcom_pull(task_ids='decide_date', key='year')
        month = ti.xcom_pull(task_ids='decide_date', key='month')
        
        self.log.info(f"[TransformGreenTaxiOperator] Running transformation for year={year}, month={month}")

        gcs_bucket = self.config['gcs_bucket']
        gcs_folder = f"{self.config['gcs_folder']}raw/"
        filename = f"green_tripdata_{year}-{month:02d}.parquet"        
        credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

        df = read_parquet_from_gcs(gcs_bucket, gcs_folder, filename, credentials_path)        
        df_cleaned = transform_taxi_data(df, run_date)  

        gcs_folder = f"{self.config['gcs_folder']}processed/"
        filename = f"green_tripdata_{year}-{month:02d}_transformed.parquet"
        xcom_key = "green_taxi_processed_filepath"

        df_to_parquet_gcs_direct(df_cleaned, gcs_bucket, gcs_folder, filename, ti, xcom_key)
        # ti.xcom_push(key=xcom_key, value=gcs_uri)
        
        self.log.info("✅ Data uploaded and XCom pushed.")
