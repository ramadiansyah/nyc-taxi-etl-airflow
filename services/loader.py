"""
loader.py

This module provides utility functions for writing transformed DataFrames to Google Cloud Storage (GCS)
in Parquet format, and for loading these files into Google BigQuery with support for partitioning,
clustering, and upsert (merge) operations.

Functions:
    - df_to_parquet_gcs_direct
    - load_parquet_from_gcs_to_bq
    - load_parquet_from_gcs_to_bq_upsert
"""

import os
import uuid
import pandas as pd
from io import BytesIO

from google.oauth2 import service_account
from google.cloud import bigquery, storage

from utils.logger import get_logger

logger = get_logger()

def df_to_parquet_gcs_direct(df: pd.DataFrame, gcs_bucket: str, gcs_folder: str, filename: str, ti, xcom_key: str):
    """
    Saves a Pandas DataFrame as a Parquet file and uploads it to Google Cloud Storage (GCS),
    then pushes the GCS URI to XCom.

    Args:
        df (pd.DataFrame): The DataFrame to be saved and uploaded.
        gcs_bucket (str): The target GCS bucket name.
        gcs_folder (str): The folder path within the GCS bucket.
        filename (str): The filename to use for the Parquet file.
        ti (TaskInstance): The Airflow TaskInstance to enable XCom push.
        xcom_key (str): The key to store the GCS URI in XCom.

    Returns:
        None
    """
    logger.info(f"df_to_parquet_gcs_direct: {df.head()}")
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    client = storage.Client()
    bucket = client.bucket(gcs_bucket)
    gcs_filepath = f"{gcs_folder}{filename}"
    blob = bucket.blob(gcs_filepath)
    blob.upload_from_file(buffer, content_type='application/octet-stream')

    gcs_url = f"gs://{gcs_bucket}/{gcs_filepath}"
    ti.xcom_push(key=xcom_key, value=gcs_url)


# direct
def load_parquet_from_gcs_to_bq(
    gcs_url: str,
    dataset_id: str,
    table_id: str,
    project_id: str,
    credentials_path: str,
    write_disposition: str = "WRITE_APPEND",  # or WRITE_TRUNCATE, WRITE_EMPTY
    partition_field: str = None,  # Optional partition field
    cluster_fields: list = None   # Optional clustering fields

):
    """
    Loads a Parquet file from GCS into a BigQuery table.

    Args:
        gcs_url (str): The full GCS URI of the file (e.g., gs://bucket/path/file.parquet).
        dataset_id (str): The BigQuery dataset ID.
        table_id (str): The BigQuery table ID.
        project_id (str): The GCP project ID.
        credentials_path (str): Path to the GCP service account JSON credentials.
        write_disposition (str): Mode for writing: WRITE_APPEND, WRITE_TRUNCATE, WRITE_EMPTY.
        partition_field (str): Column name for time-based partitioning (optional).
        cluster_fields (list): List of columns to use for clustering (optional).

    Returns:
        None
    """

    logger.info(f"processed URL: {gcs_url}")
    # Setup credentials
    credentials = service_account.Credentials.from_service_account_file(credentials_path)

    # Initialize BigQuery client
    client = bigquery.Client(credentials=credentials, project=project_id)
    logger.info(f"Partition field: {partition_field} ({type(partition_field)})")

    # Configure the job
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        source_format=bigquery.SourceFormat.PARQUET,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field,
        ) if partition_field else None,
        clustering_fields=cluster_fields if cluster_fields else [],
        autodetect=True
    )

    # Load the Parquet file from GCS
    load_job = client.load_table_from_uri(
        gcs_url, table_ref, job_config=job_config
    )
    logger.info(f"Starting job: {load_job.job_id}")

    load_job.result()  # Waits for job to complete
    logger.info("Job finished.")

    destination_table = client.get_table(table_ref)
    logger.info(f"Loaded {destination_table.num_rows} rows into {table_ref}.")



def load_parquet_from_gcs_to_bq_upsert(
    gcs_url: str,
    dataset_id: str,
    table_id: str,
    project_id: str,
    credentials_path: str,
    schema: dict,
    write_disposition: str = "WRITE_APPEND",
    partition_field: str = None,
    cluster_fields: list = None
):
    """
    Loads a Parquet file from GCS into a staging table in BigQuery and performs an upsert (MERGE)
    into the target table using a fingerprint-based deduplication strategy.

    If the target table doesn't exist, it will be created with the same schema, partitioning,
    and clustering as the staging table.

    Args:
        gcs_url (str): GCS URI of the Parquet file.
        dataset_id (str): BigQuery dataset ID.
        table_id (str): BigQuery target table ID (without staging suffix).
        project_id (str): GCP project ID.
        credentials_path (str): Path to the GCP service account credentials file.
        write_disposition (str): BigQuery write mode. Default is WRITE_APPEND.
        partition_field (str): Field to use for time-based partitioning.
        cluster_fields (list): List of columns to use for clustering.

    Returns:
        None
    """
    logger.info(f"Processing GCS URL: {gcs_url}")
    logger.info(f"Partition field: {partition_field} ({type(partition_field)})")

    credentials = service_account.Credentials.from_service_account_file(credentials_path)
    client = bigquery.Client(credentials=credentials, project=project_id)

    staging_table = f"{project_id}.{dataset_id}.{table_id}_staging_{uuid.uuid4().hex[:8]}"
    target_table = f"{project_id}.{dataset_id}.{table_id}"

    logger.info(f"schema: {schema}")

    # Load data into staging table
    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        schema=schema,
        source_format=bigquery.SourceFormat.PARQUET,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field
        ) if partition_field else None,
        clustering_fields=cluster_fields or [],
        autodetect=True
    )

    load_job = client.load_table_from_uri(gcs_url, staging_table, job_config=job_config)
    load_job.result()
    logger.info(f"Loaded to staging table: {staging_table}")

    # Confirm load
    destination_table = client.get_table(staging_table)
    logger.info(f"Loaded {destination_table.num_rows} rows into {staging_table}.")


    columns = [
        'vendorid', 'lpep_pickup_datetime', 'lpep_dropoff_datetime', 'store_and_fwd_flag',
        'ratecodeid', 'pulocationid', 'dolocationid', 'passenger_count', 'trip_distance',
        'fare_amount', 'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'ehail_fee',
        'improvement_surcharge', 'total_amount', 'payment_type', 'trip_type',
        'congestion_surcharge', 'raw_src', 'run_date', 'row_no', 'row_hash'
    ]

    update_clause = ",\n        ".join([f"T.{col} = S.{col}" for col in columns])
    insert_columns = ", ".join(columns)
    insert_values = ", ".join([f"S.{col}" for col in columns])

    row_hash_expr = """
        FARM_FINGERPRINT(
            CONCAT(
                CAST(raw_src AS STRING), '|',
                CAST(row_no AS STRING)
            )
        )
    """

    # Check if target table exists or create it
    try:
        table = client.get_table(target_table)
        logger.info("Target table exists.")

        if not any(field.name == 'row_hash' for field in table.schema):
            logger.info("Adding 'row_hash' column to target table.")
            client.query(f"ALTER TABLE `{target_table}` ADD COLUMN row_hash INT64").result()

    except Exception as e:
        logger.warning("Target table not found. Creating from staging.")

        # Fetch staging table metadata
        staging_metadata = client.get_table(staging_table)

        # Define new table (object) with same schema
        new_table = bigquery.Table(target_table, schema=staging_metadata.schema)

        # Copy partitioning
        new_table.time_partitioning = staging_metadata.time_partitioning
        new_table.range_partitioning = staging_metadata.range_partitioning

        # Copy clustering fields
        new_table.clustering_fields = staging_metadata.clustering_fields

        # Create empty target table first
        client.create_table(new_table, exists_ok=True)
        logger.info(f"Created target table `{target_table}` with partitioning and clustering.")

        # Add row_hash column
        client.query(f"ALTER TABLE `{target_table}` ADD COLUMN row_hash INT64").result()

        # Fill it with data (including row_hash)
        query = f"""
            INSERT INTO `{target_table}`
            SELECT
                *,
                {row_hash_expr} AS row_hash
            FROM `{staging_table}`
        """
        client.query(query).result()

         # 6. Clean up
        client.delete_table(staging_table, not_found_ok=True)
        logger.info(f"Staging table: {staging_table} deleted.")

        return

    # Merge deduplicated data into target
    merge_query = f"""
    MERGE `{target_table}` T
    USING (
        SELECT *
        FROM (
            SELECT
                *,
                {row_hash_expr} AS row_hash,
                ROW_NUMBER() OVER (
                    PARTITION BY {row_hash_expr}
                    ORDER BY run_date DESC
                ) AS rn
            FROM `{staging_table}`
        )
        WHERE rn = 1
    ) S
    ON T.row_hash = S.row_hash
    WHEN MATCHED THEN UPDATE SET
        {update_clause}
    WHEN NOT MATCHED THEN INSERT (
        {insert_columns}
    ) VALUES (
        {insert_values}
    )
    """

    logger.info("Running MERGE query...")
    # logger.info({merge_query})
    # MERGE `purwadika.jcdeol004_capstone3_rr_nyc.green_taxi_trip` T
    #     USING (
    #         SELECT *
    #         FROM (
    #             SELECT
    #                 *,
    #                 -- Generate unique fingerprint hash for deduplication
    #                 FARM_FINGERPRINT(
    #                     CONCAT(
    #                         CAST(raw_src AS STRING), '|',
    #                         CAST(row_no AS STRING)
    #                     )
    #                 ) AS row_hash,

    #                 -- Deduplicate: keep the latest entry by run_date
    #                 ROW_NUMBER() OVER (
    #                     PARTITION BY FARM_FINGERPRINT(
    #                         CONCAT(
    #                             CAST(raw_src AS STRING), '|',
    #                             CAST(row_no AS STRING)
    #                         )
    #                     )
    #                     ORDER BY run_date DESC
    #                 ) AS rn

    #             FROM `purwadika.jcdeol004_capstone3_rr_nyc.green_taxi_trip_staging_373e95d1`
    #         )
    #         WHERE rn = 1
    #     ) S
    #     ON T.row_hash = S.row_hash

    #     -- If match found, update all relevant fields
    #     WHEN MATCHED THEN UPDATE SET
    #         T.vendorid = S.vendorid,
    #         T.lpep_pickup_datetime = S.lpep_pickup_datetime,
    #         T.lpep_dropoff_datetime = S.lpep_dropoff_datetime,
    #         T.store_and_fwd_flag = S.store_and_fwd_flag,
    #         T.ratecodeid = S.ratecodeid,
    #         T.pulocationid = S.pulocationid,
    #         T.dolocationid = S.dolocationid,
    #         T.passenger_count = S.passenger_count,
    #         T.trip_distance = S.trip_distance,
    #         T.fare_amount = S.fare_amount,
    #         T.extra = S.extra,
    #         T.mta_tax = S.mta_tax,
    #         T.tip_amount = S.tip_amount,
    #         T.tolls_amount = S.tolls_amount,
    #         T.ehail_fee = S.ehail_fee,
    #         T.improvement_surcharge = S.improvement_surcharge,
    #         T.total_amount = S.total_amount,
    #         T.payment_type = S.payment_type,
    #         T.trip_type = S.trip_type,
    #         T.congestion_surcharge = S.congestion_surcharge,
    #         T.raw_src = S.raw_src,
    #         T.run_date = S.run_date,
    #         T.row_no = S.row_no,
    #         T.row_hash = S.row_hash

    #     -- If no match found, insert as a new row
    #     WHEN NOT MATCHED THEN INSERT (
    #         vendorid, lpep_pickup_datetime, lpep_dropoff_datetime, store_and_fwd_flag,
    #         ratecodeid, pulocationid, dolocationid, passenger_count, trip_distance,
    #         fare_amount, extra, mta_tax, tip_amount, tolls_amount, ehail_fee,
    #         improvement_surcharge, total_amount, payment_type, trip_type,
    #         congestion_surcharge, raw_src, run_date, row_no, row_hash
    #     )
    #     VALUES (
    #         S.vendorid, S.lpep_pickup_datetime, S.lpep_dropoff_datetime, S.store_and_fwd_flag,
    #         S.ratecodeid, S.pulocationid, S.dolocationid, S.passenger_count, S.trip_distance,
    #         S.fare_amount, S.extra, S.mta_tax, S.tip_amount, S.tolls_amount, S.ehail_fee,
    #         S.improvement_surcharge, S.total_amount, S.payment_type, S.trip_type,
    #         S.congestion_surcharge, S.raw_src, S.run_date, S.row_no, S.row_hash
    #     );
    client.query(merge_query).result()

    # Clean up
    client.delete_table(staging_table, not_found_ok=True)
    logger.info("Upsert complete. Staging table deleted.")

    destination_table = client.get_table(target_table)
    logger.info(f"{target_table} now has {destination_table.num_rows} rows.")



