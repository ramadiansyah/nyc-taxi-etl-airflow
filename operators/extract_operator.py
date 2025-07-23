"""
extract_operator.py
Custom Airflow operator for extracting NYC Green Taxi data and uploading it to Google Cloud Storage (GCS).
"""

from airflow.models import BaseOperator
from airflow.utils.context import Context

from datetime import datetime, timezone
from services.extractor import download_nyc_and_upload_to_gcs

class ExtractGreenTaxiOperator(BaseOperator):
    """
    Custom Airflow Operator to extract NYC Green Taxi data from a public URL and upload it to GCS.

    This operator pulls the year and month from a previous task via XCom, constructs the download URL,
    and uses a helper function to upload the data to the specified GCS bucket and folder.

    Attributes:
        config (dict): Configuration dictionary containing GCS bucket name, folder paths, and data URL.

    Methods:
        execute(context): Executes the extraction and upload process for the specified year and month.
    """
    def __init__(self, config, *args, **kwargs):
        """
        Initialize the ExtractGreenTaxiOperator.

        Args:
            config (dict): A dictionary containing configuration values, including:
                - gcs_bucket (str): Name of the GCS bucket.
                - gcs_folder (str): GCS folder path where files will be stored.
                - nyc_data_url (str): Base URL for the NYC Green Taxi dataset.
            *args: Variable length argument list for BaseOperator.
            **kwargs: Arbitrary keyword arguments for BaseOperator.
        """
        super().__init__(*args, **kwargs)
        self.config = config

    def execute(self, context: Context):
        """
        Executes the extraction task.

        Pulls the year and month from the XCom pushed by the `decide_date` task, constructs the file
        download URL, and uploads the file to GCS using the `download_nyc_and_upload_to_gcs()` utility.

        Args:
            context (Context): Airflow context dictionary that provides information about the execution environment.

        Raises:
            Exception: If the download or upload process fails.

        Returns:
            None
        """
        ti = context["ti"]
        year = ti.xcom_pull(task_ids="decide_date", key="year")
        month = ti.xcom_pull(task_ids="decide_date", key="month")

        self.log.info(f"[ExtractGreenTaxiOperator] Running extractor for year: {year}, month: {month}")
        
        gcs_bucket = self.config['gcs_bucket']
        gcs_folder = f"{self.config['gcs_folder']}raw/"
        run_date = context.get("logical_date") or datetime.now(timezone.utc)
        filename = f"green_tripdata_{year}-{month:02d}.parquet"
        url = f"{self.config['nyc_data_url']}/{filename}"

        download_nyc_and_upload_to_gcs(url, filename, gcs_bucket, gcs_folder, run_date)
