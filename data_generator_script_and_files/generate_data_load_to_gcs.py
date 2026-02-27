"""
DAG: Generate synthetic marketplace data monthly
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from datetime import datetime, timedelta
import subprocess
import os

default_args = {
    'owner': 'data-team',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

def generate_monthly_data(**context):
    """Run your data generation script with proper logging"""
    import os
    import subprocess

    execution_date = context['execution_date']
    month_year = execution_date.strftime('%Y-%m')
    
    script_path = os.path.join(os.path.dirname(__file__), 'generate_synthetic_data.py')
    output_dir = f'/tmp/marketplace_data/{month_year}'
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        result = subprocess.run(
            ['python3', script_path, '--output_dir', output_dir],
            capture_output=True,
            text=True,
        )
        print("=== SCRIPT STDOUT ===")
        print(result.stdout)
        print("=== SCRIPT STDERR ===")
        print(result.stderr)
        
        # Will raise CalledProcessError if script failed
        result.check_returncode()
    
    except subprocess.CalledProcessError:
        print("Script failed with exit code", result.returncode)
        raise

    return output_dir

with DAG(
    'generate_marketplace_data',
    default_args=default_args,
    description='Generate and upload monthly marketplace data',
    schedule_interval='0 0 1 * *',  # First day of month at midnight
    catchup=False,
    tags=['data-generation', 'ingestion'],
) as dag:

    # Generate data
    generate_data = PythonOperator(
        task_id='generate_csv_data',
        python_callable=generate_monthly_data,
        provide_context=True,
    )

    # Upload to GCS with date partition
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        src='/tmp/marketplace_data/{{ execution_date.strftime("%Y-%m") }}/*.csv',
        dst='raw/{{ execution_date.strftime("%Y-%m") }}/',
        bucket='marketplace-analytics-485915-data-lake',
        gcp_conn_id='google_cloud_default',
    )

    generate_data >> upload_to_gcs