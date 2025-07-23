"""
green_taxi_etl_dag.py

This DAG orchestrates a monthly ETL pipeline for New York City Green Taxi data. It extracts raw data,
transforms it, and loads it into a target destination using custom Airflow operators.

Modules:
    - os: Provides functions for interacting with the operating system.
    - airflow: Core Airflow package for DAG definitions.
    - datetime: Used for managing dates and time intervals.
    - airflow.operators.python: Used to define tasks using Python callables.
    - operators.extract_operator: Contains custom ExtractGreenTaxiOperator.
    - operators.transform_operator: Contains custom TransformGreenTaxiOperator.
    - operators.load_operator: Contains custom LoadGreenTaxiOperator.
    - services.tasks: Contains auxiliary Python callable tasks (e.g., decide_date_task).
    - utils.config_loader: Loads DAG configuration from a YAML file.
    - utils.notifier: Contains callback functions for DAG success/failure notifications.

Attributes:
    CONFIG_PATH (str): Path to the configuration YAML file.
    config (dict): Dictionary containing configuration loaded from YAML file.
    default_args (dict): Default arguments for the DAG and all its tasks.
    dag (DAG): The Airflow DAG object defined for this ETL pipeline.

DAG Schedule:
    - DAG ID: "green_taxi_etl_dag"
    - Description: "Monthly ETL DAG for Green Taxi Data"
    - Schedule: Runs every 5th day of each month at 05:00 AM
    - Start Date: January 1, 2023
    - Catchup: Disabled
    - Tags: ["capstone3", "green-taxi", "etl"]

Tasks:
    - decide_date: Determines the processing date using configuration.
    - extract_green_taxi: Extracts Green Taxi data from the source.
    - transform_green_taxi: Cleans and transforms the extracted data.
    - decide_gcs_filepath: Determines the GCS filepath for loading.
    - load_green_taxi: Loads the transformed data to GCS or final destination.

Usage:
    Place this file in the Airflow DAGs folder. Ensure that custom operators and utility modules
    are available in the Airflow context and Docker image (if using Docker-based deployment).
"""

import os
from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from operators.extract_operator import ExtractGreenTaxiOperator
from operators.transform_operator import TransformGreenTaxiOperator
from operators.load_operator import LoadGreenTaxiOperator
from services.tasks import decide_date_task, decide_gcs_url_file_task

from utils.config_loader import load_config
# from utils.notifier import failure_callback, success_callback
from utils.notifier import create_failure_callback, create_success_callback

CONFIG_PATH = "/opt/airflow/config/config.yaml"
config = load_config(CONFIG_PATH)

WEBHOOK_URL = config['discord_webhook_url']


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": create_failure_callback(WEBHOOK_URL),
    "on_success_callback": create_success_callback(WEBHOOK_URL),
}


dag = DAG(
    dag_id="green_taxi_etl_dag",
    default_args=default_args,
    description="Monthly ETL DAG for Green Taxi Data",
    schedule_interval="0 5 5 * *",  # Every 5th day of month at 05:00
    start_date=datetime(2023, 2, 1),
    catchup=True,
    max_active_runs=1,
    tags=["capstone3", "green-taxi", "etl"]
)

with dag:

    # Task 1: PythonOperator with function factory (closure-based)
    t0 = PythonOperator(
        task_id="decide_date",
        python_callable=decide_date_task(config),  # injects config via closure
        dag=dag
    )

    # #Task 2: Custom Operator with config injected via __init__
    extract_task = ExtractGreenTaxiOperator(
        task_id="extract_green_taxi",
        config=config,  # passed to __init__ of the custom operator
        dag=dag
    )

    transform_task = TransformGreenTaxiOperator(
        task_id="transform_green_taxi",
        config=config,  
        dag=dag
    )

    t1 = PythonOperator(
        task_id="decide_gcs_filepath",
        python_callable=decide_gcs_url_file_task(config),  
        dag=dag
    )

    load_task = LoadGreenTaxiOperator(
        task_id="load_green_taxi",
        config=config,  
        dag=dag
    )

    t0 >> extract_task >> transform_task >> t1 >> load_task

    # for testing simulation for incremental load
    # t0 >> t1 >> load_task

    

