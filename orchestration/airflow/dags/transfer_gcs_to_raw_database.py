"""
DAG: Load CSV files from GCS to BigQuery raw dataset
Schedule: Monthly (or however often your data updates)
"""

from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'gcs_to_bigquery_raw',
    default_args=default_args,
    schedule_interval='0 2 1 * *',  # 2am on the 1st of each month
    catchup=False,
    tags=['ingestion', 'bigquery'],
) as dag:

    # Define your CSV files
    csv_files = [
        'clicks',
        'impressions',
        'items',
        'lifecycle_events',
        'notifications',
        'purchases',
        'search_events',
        'users'
    ]

    # Create tasks for each CSV
    for table_name in csv_files:
        
        load_csv = GCSToBigQueryOperator(
            task_id=f'load_{table_name}',
            bucket='marketplace-analytics-485915-data-lake',
            # This will resolve to: raw/2025-02/customers.csv
            source_objects=['raw/{{ execution_date.strftime("%Y-%m") }}/' + f'{table_name}.csv'],
            destination_project_dataset_table=f'marketplace-analytics-485915.raw.{table_name}',
            source_format='CSV',
            skip_leading_rows=1,
            autodetect=True,
            write_disposition='WRITE_TRUNCATE',
            create_disposition='CREATE_IF_NEEDED',
            gcp_conn_id='google_cloud_default',
        )

        load_csv