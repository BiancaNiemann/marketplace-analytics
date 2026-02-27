"""
DAG: Load monthly CSV files from GCS to BigQuery raw dataset
- Loads new users incrementally
- Appends activity data for all users
- Handles schema properly
"""

from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from google.cloud import bigquery

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['your-email@example.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Table schemas (explicit to avoid autodetect issues)
SCHEMAS = {
    'users': [
        {'name': 'user_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'email', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'signup_date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'signup_ts', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'country', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'city', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'age_group', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'is_seller', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'account_type', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'signup_channel', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'device_type', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'marketing_opt_in', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'is_verified', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'status', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    'items': [
        {'name': 'item_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'seller_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'title', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'category', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'price', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'currency', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'condition', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'listed_date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'created_ts', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'status', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    'search_events': [
        {'name': 'search_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'user_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'query', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'search_date', 'type': 'DATE', 'mode': 'NULLABLE'},
    ],
    'impressions': [
        {'name': 'impression_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'search_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'user_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'item_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'position', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'impression_date', 'type': 'DATE', 'mode': 'NULLABLE'},
    ],
    'clicks': [
        {'name': 'click_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'impression_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'search_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'user_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'item_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'position', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'click_ts', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'click_date', 'type': 'DATE', 'mode': 'NULLABLE'},
    ],
    'purchases': [
        {'name': 'purchase_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'item_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'buyer_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'seller_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'purchase_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'platform_fee', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'payment_method', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'currency', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'purchase_timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'purchase_date', 'type': 'DATE', 'mode': 'NULLABLE'},
    ],
    'notifications': [
        {'name': 'notification_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'user_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'type', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'sent_ts', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'sent_date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'opened', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'channel', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
    'lifecycle_events': [
        {'name': 'event_id', 'type': 'INTEGER', 'mode': 'REQUIRED'},
        {'name': 'user_id', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'event_type', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'timestamp', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
        {'name': 'event_date', 'type': 'DATE', 'mode': 'NULLABLE'},
        {'name': 'properties', 'type': 'STRING', 'mode': 'NULLABLE'},
    ],
}

def check_for_duplicates(**context):
    """Check for duplicate data before loading"""
    from google.cloud import bigquery
    
    execution_date = context['execution_date']
    year_month = execution_date.strftime('%Y-%m')
    
    client = bigquery.Client(project='marketplace-analytics-485915')
    
    # Check if data for this month already exists
    tables_to_check = ['purchases', 'clicks', 'impressions', 'search_events']
    
    for table in tables_to_check:
        query = f"""
        SELECT COUNT(*) as count
        FROM `marketplace-analytics-485915.raw.{table}`
        WHERE DATE_TRUNC(
            COALESCE(
                purchase_date, 
                click_date, 
                impression_date, 
                search_date
            ), MONTH
        ) = '{year_month}-01'
        """
        
        try:
            result = client.query(query).result()
            count = list(result)[0].count
            
            if count > 0:
                print(f"Warning: {table} already has {count} rows for {year_month}")
                print("Consider using WRITE_TRUNCATE or dedupe strategy")
        except Exception as e:
            print(f"Table {table} may not exist yet: {e}")

with DAG(
    'gcs_to_bigquery_incremental',
    default_args=default_args,
    description='Load monthly marketplace data from GCS to BigQuery',
    schedule_interval='0 3 1 * *',  # 3am on the 1st, after data generation
    catchup=False,
    max_active_runs=1,
    tags=['ingestion', 'bigquery', 'incremental'],
) as dag:
    
    # Check for duplicates before loading
    check_duplicates = PythonOperator(
        task_id='check_for_duplicates',
        python_callable=check_for_duplicates,
        provide_context=True,
    )
    
    # Load each table with appropriate settings
    load_tasks = []
    
    for table_name, schema in SCHEMAS.items():
        load_task = GCSToBigQueryOperator(
            task_id=f'load_{table_name}',
            bucket='marketplace-analytics-485915-data-lake',
            source_objects=[
                'raw/{{ execution_date.strftime("%Y-%m") }}/' + f'{table_name}.csv'
            ],
            destination_project_dataset_table=(
                f'marketplace-analytics-485915.raw.{table_name}'
            ),
            schema_fields=schema,
            source_format='CSV',
            skip_leading_rows=1,
            write_disposition='WRITE_APPEND',  # Append new data
            create_disposition='CREATE_IF_NEEDED',
            allow_jagged_rows=False,
            allow_quoted_newlines=True,
            gcp_conn_id='google_cloud_default',
        )
        
        load_tasks.append(load_task)
    
    # Data quality checks after loading
    quality_checks = []
    
    # Check 1: Verify row counts
    check_row_counts = BigQueryCheckOperator(
        task_id='check_row_counts',
        sql="""
        SELECT COUNT(*) > 0
        FROM `marketplace-analytics-485915.raw.users`
        WHERE DATE_TRUNC(signup_date, MONTH) = 
            DATE_TRUNC(DATE('{{ execution_date.strftime("%Y-%m-%d") }}'), MONTH)
        """,
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_default',
    )
    quality_checks.append(check_row_counts)
    
    # Check 2: Verify purchases have valid amounts
    check_purchase_amounts = BigQueryCheckOperator(
        task_id='check_purchase_amounts',
        sql="""
        SELECT COUNT(*) = 0
        FROM `marketplace-analytics-485915.raw.purchases`
        WHERE purchase_amount <= 0 OR purchase_amount IS NULL
        AND DATE_TRUNC(purchase_date, MONTH) = 
            DATE_TRUNC(DATE('{{ execution_date.strftime("%Y-%m-%d") }}'), MONTH)
        """,
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_default',
    )
    quality_checks.append(check_purchase_amounts)
    
    # Check 3: Verify referential integrity (users exist for purchases)
    check_referential_integrity = BigQueryCheckOperator(
        task_id='check_referential_integrity',
        sql="""
        SELECT COUNT(*) = 0
        FROM `marketplace-analytics-485915.raw.purchases` p
        LEFT JOIN `marketplace-analytics-485915.raw.users` u
            ON p.buyer_id = u.user_id
        WHERE u.user_id IS NULL
        AND DATE_TRUNC(p.purchase_date, MONTH) = 
            DATE_TRUNC(DATE('{{ execution_date.strftime("%Y-%m-%d") }}'), MONTH)
        """,
        use_legacy_sql=False,
        gcp_conn_id='google_cloud_default',
    )
    # quality_checks.append(check_referential_integrity)
    
    # Set up dependencies
    check_duplicates >> load_tasks
    
    # All loads must complete before quality checks
    for load_task in load_tasks:
        load_task >> quality_checks