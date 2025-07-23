"""
transformer.py

This module provides utilities for:
1. Reading Parquet files from Google Cloud Storage (GCS) into Pandas DataFrames.
2. Cleaning and transforming NYC taxi data before loading.

Functions:
    - read_parquet_from_gcs
    - transform_taxi_data
"""

import pandas as pd
import os
from io import BytesIO

from google.cloud import storage
from datetime import datetime

from utils.logger import get_logger

logger = get_logger()
    
def read_parquet_from_gcs(gcs_bucket: str, gcs_folder: str, filename: str, credentials_path: str) -> pd.DataFrame:
    try:
        # GCS path
        blob_path = f"{gcs_folder}{filename}"
        logger.info(f"blob_path: {blob_path}")

        # Create client
        storage_client = storage.Client.from_service_account_json(credentials_path)
        bucket = storage_client.bucket(gcs_bucket)
        blob = bucket.blob(blob_path)

        # Download blob as bytes
        byte_stream = BytesIO()
        blob.download_to_file(byte_stream)
        byte_stream.seek(0)

        # Load into dataframe
        df = pd.read_parquet(byte_stream, engine="pyarrow")
        return df

    except Exception as e:
        raise RuntimeError(f"Error reading parquet from GCS: {e}")


def transform_taxi_data(df: pd.DataFrame, run_date: datetime) -> pd.DataFrame:
    """
    Cleans and transforms raw NYC taxi trip data.

    Steps include:
    - Standardizing column names to lowercase and snake_case.
    - Converting pickup and dropoff columns to datetime.
    - Dropping rows with null values in essential datetime fields.
    - Adding a `run_date` column to indicate transformation time.

    Args:
        df (pd.DataFrame): The raw input DataFrame.
        run_date (datetime): The timestamp associated with this ETL run.

    Returns:
        pd.DataFrame: A cleaned and transformed version of the input DataFrame.

    Raises:
        RuntimeError: If any transformation step fails.
    """
    try:
        logger.info("Starting data transformation...")
        logger.info(f"Initial shape: {df.shape}")
        logger.info(f"Null count per column before dropna:\n{df.isnull().sum()}")

        # Standardisasi nama kolom
        df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]

        # Konversi datetime
        for col in ["lpep_pickup_datetime", "lpep_dropoff_datetime"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')

        # Drop hanya kolom penting
        df.dropna(subset=["lpep_pickup_datetime", "lpep_dropoff_datetime"], inplace=True)

        logger.info(f"Shape after dropna: {df.shape}")

        df['ehail_fee'] = df['ehail_fee'].astype(float)

        # Tambahkan run_date
        df["run_date"] = run_date
        logger.info(f"Transformation complete. Run Date; {run_date}")
        return df

    except Exception as e:
        logger.error(f"Error in transformation: {e}")
        raise
