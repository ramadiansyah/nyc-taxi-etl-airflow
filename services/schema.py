from typing import Dict, Any
from typing import List
from google.cloud import bigquery

# Schema for the raw incoming purchase stream data
nyc_green_transformed_schema_dict: Dict[str, Any] = {
    "fields": [
        {'name': 'vendorid', 'field_type': 'INT64', 'mode': 'NULLABLE'},
        {'name': 'lpep_pickup_datetime', 'field_type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'lpep_dropoff_datetime', 'field_type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'store_and_fwd_flag', 'field_type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'ratecodeid', 'field_type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'pulocationid', 'field_type': 'INT64', 'mode': 'NULLABLE'},
        {'name': 'dolocationid', 'field_type': 'INT64', 'mode': 'NULLABLE'},
        {'name': 'passenger_count', 'field_type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'trip_distance', 'field_type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'fare_amount', 'field_type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'extra', 'field_type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'mta_tax', 'field_type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'tip_amount', 'field_type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'tolls_amount', 'field_type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'ehail_fee', 'field_type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'improvement_surcharge', 'field_type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'total_amount', 'field_type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'payment_type', 'field_type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'trip_type', 'field_type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'congestion_surcharge', 'field_type': 'FLOAT64', 'mode': 'NULLABLE'},
        {'name': 'raw_src', 'field_type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'run_date', 'field_type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'row_no', 'field_type': 'INT64', 'mode': 'NULLABLE'}
    ]
}


nyc_green_transformed_schema: List[bigquery.SchemaField] = [
    bigquery.SchemaField(**field) for field in nyc_green_transformed_schema_dict['fields']
]