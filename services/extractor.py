"""
extractor.py

This module provides functionality to download NYC taxi data from a given URL,
enhance it with metadata, and upload it to Google Cloud Storage (GCS) as a Parquet file.
"""

import pandas as pd
from io import BytesIO
from datetime import datetime
from google.cloud import storage

from utils.logger import get_logger
logger = get_logger()

def download_nyc_and_upload_to_gcs(url: str, filename: str, gcs_bucket: str, gcs_folder: str, run_date: datetime) -> None:
    """
    Downloads NYC taxi data from a URL, adds metadata columns, and uploads it as a Parquet file to GCS.

    The function performs the following steps:
    1. Reads a remote Parquet file into a Pandas DataFrame.
    2. Appends metadata columns:
        - `raw_src`: original filename.
        - `run_date`: Airflow DAG run date.
        - `row_no`: sequential row number for tracking.
    3. Converts the DataFrame to a Parquet file in-memory.
    4. Uploads the file to the specified GCS bucket and folder.

    Args:
        url (str): The URL to the Parquet file (typically hosted on a public dataset repository).
        filename (str): The name to assign to the uploaded Parquet file.
        gcs_bucket (str): The name of the destination Google Cloud Storage bucket.
        gcs_folder (str): The folder path within the GCS bucket to store the file.
        run_date (datetime): The logical execution date or DAG run timestamp to include in the metadata.

    Raises:
        Exception: If any error occurs during reading, transforming, or uploading the data.
    """
    try:
        logger.info(f"🌐 Start reading file from URL: {url} to DataFrame")
        df = pd.read_parquet(url)
        df["raw_src"] = filename
        df["run_date"] = run_date
        df["row_no"] = range(1, len(df) + 1)  # Adds row number starting from 1
        logger.info(f"df.head: {df.head()}")

        # Convert DataFrame to Parquet in memory
        buffer = BytesIO()
        df.to_parquet(buffer, engine="pyarrow", index=False)
        buffer.seek(0)

        # Upload to GCS
        destination_blob_path = f"{gcs_folder}{filename}"
        
        logger.info(f"📦 Uploading to GCS: gs://{gcs_bucket}/{destination_blob_path}")
        client = storage.Client()
        bucket = client.bucket(gcs_bucket)
        blob = bucket.blob(destination_blob_path) 
        blob.upload_from_file(buffer, content_type='application/octet-stream')

        logger.info(f"✅ Successfully uploaded to gs://{gcs_bucket}/{destination_blob_path}")

    except Exception as e:
        logger.error(f"❌ Failed to upload DataFrame to GCS: {e}")
        raise




