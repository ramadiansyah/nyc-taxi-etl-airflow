"""
load_operator.py

This module defines a custom Airflow operator to load transformed NYC Green Taxi data
from Google Cloud Storage (GCS) into a BigQuery table using an upsert (merge or append) strategy.
"""

import os
from airflow.models import BaseOperator
from airflow.utils.context import Context

from datetime import datetime, timezone
from services.loader import load_parquet_from_gcs_to_bq_upsert
from services.schema import nyc_green_transformed_schema

class LoadGreenTaxiOperator(BaseOperator):
    """
    Custom Airflow Operator to load Parquet files from GCS into a BigQuery table.

    This operator pulls the GCS file path from a previous task via XCom, and loads
    the file into BigQuery using specified partitioning and clustering settings.

    Attributes:
        config (dict): Configuration dictionary containing BQ dataset, table, project ID, etc.

    Methods:
        execute(context): Executes the load task by calling a utility function to handle GCS → BQ transfer.
    """
    def __init__(self, config, *args, **kwargs):
        """
        Initializes the LoadGreenTaxiOperator with the provided configuration.

        Args:
            config (dict): A dictionary containing configuration values, including:
                - bq_dataset (str): BigQuery dataset ID.
                - bq_table (str): BigQuery table name.
                - gcp_project_id (str): Google Cloud project ID.
                - gcs_bucket (str): (Optional) GCS bucket name (if needed in future).
            *args: Additional positional arguments for BaseOperator.
            **kwargs: Additional keyword arguments for BaseOperator.
        """
        super().__init__(*args, **kwargs)
        self.config = config

    def execute(self, context: Context):
        """
        Executes the load operation from GCS to BigQuery.

        This method:
        - Pulls the processed Parquet file GCS URL from XCom.
        - Reads configuration for BQ target.
        - Calls `load_parquet_from_gcs_to_bq_upsert()` with appropriate parameters.

        Args:
            context (Context): Airflow execution context dictionary.

        Raises:
            Exception: If the load process fails due to missing XCom values or GCS/BQ issues.

        Returns:
            None
        """
        # run_date = context.get("logical_date") or datetime.now(timezone.utc)

        xcom_task_id = "decide_gcs_filepath"
        xcom_key = "green_taxi_processed_filepath"

        ti = context["ti"]
        gcs_url = ti.xcom_pull(task_ids=xcom_task_id, key=xcom_key)

        dataset_id = self.config['bq_dataset']
        table_id = self.config['bq_table']
        project_id = self.config['gcp_project_id']
        credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

        # gcs_filepath = f"{gcs_folder}{filename}"
        # gcs_url = f"gs://{gcs_bucket}/{gcs_filepath}"
        
        # df_to_parquet_gcs_direct: ['vendorid', 'lpep_pickup_datetime', 'lpep_dropoff_datetime', 'store_and_fwd_flag', 'ratecodeid', 'pulocationid', 'dolocationid', 'passenger_count', 'trip_distance', 'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'ehail_fee', 'improvement_surcharge', 'total_amount', 'payment_type', 'trip_type', 'congestion_surcharge', 'run_date']

        write_disposition: str = "WRITE_APPEND",  # or WRITE_TRUNCATE, WRITE_EMPTY
        schema = nyc_green_transformed_schema
        partition_field = "lpep_pickup_datetime" # must be a TIMESTAMP or DATE columnx
        cluster_fields = ["vendorid", "pulocationid", "dolocationid"] # supported Types STRING, BOOLEAN, INT64 (aka INTEGER), DATE, TIMESTAMP
        
        # def load_parquet_from_gcs_to_bq_upsert(
        #     gcs_url: str,
        #     dataset_id: str,
        #     table_id: str,
        #     project_id: str,
        #     credentials_path: str,
        #     schema: dict,
        #     write_disposition: str = "WRITE_APPEND",
        #     partition_field: str = None,
        #     cluster_fields: list = None
        # ):

        load_parquet_from_gcs_to_bq_upsert(
            gcs_url,
            dataset_id,
            table_id,
            project_id,
            credentials_path,
            schema, 
            write_disposition,
            partition_field, 
            cluster_fields
        )

        self.log.info("✅ LoadGreenTaxiOperator completed.")
        

