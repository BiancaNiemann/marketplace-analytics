"""
DAG: Generate incremental marketplace data monthly
- Adds 500-600 new users per month
- Generates activity for ALL users (new and existing)
- Maintains user state in GCS
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': datetime(2026, 3, 1),  # Start from Feb 2026
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['your-email@example.com'],
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

def generate_monthly_data(**context):
    """
    Run incremental data generation script
    - Loads existing users from GCS state
    - Adds new users
    - Generates activity for all users
    """
    import subprocess
    import os
    
    execution_date = context['execution_date']
    year_month = execution_date.strftime('%Y-%m')
    
    script_path = os.path.join(
        os.path.dirname(__file__), 
        'generate_incremental_data.py'
    )
    output_dir = f'/tmp/marketplace_data/{year_month}'
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Set Google Cloud credentials
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.expanduser(
        '~/.keys/service-account-key.json'
    )
    
    print(f"Generating data for {year_month}")
    print(f"Output directory: {output_dir}")
    
    try:
        # Run the generation script
        result = subprocess.run(
            [
                'python3',
                script_path,
                '--year_month', year_month,
                '--output_dir', output_dir
            ],
            capture_output=True,
            text=True,
            check=True
        )
        
        print("=== GENERATION OUTPUT ===")
        print(result.stdout)
        
        if result.stderr:
            print("=== WARNINGS ===")
            print(result.stderr)
        
        # Verify files were created
        files = os.listdir(output_dir)
        print(f"\nGenerated files: {files}")
        
        if len(files) < 8:
            raise Exception(f"Expected 8 CSV files, found {len(files)}")
        
        return output_dir
        
    except subprocess.CalledProcessError as e:
        print(f"Script failed with exit code {e.returncode}")
        print(f"STDOUT: {e.stdout}")
        print(f"STDERR: {e.stderr}")
        raise
    except Exception as e:
        print(f"Error: {str(e)}")
        raise

def validate_data(**context):
    """Basic validation of generated data"""
    import pandas as pd
    
    execution_date = context['execution_date']
    year_month = execution_date.strftime('%Y-%m')
    output_dir = f'/tmp/marketplace_data/{year_month}'
    
    print(f"Validating data in {output_dir}")
    
    # Check file sizes and row counts
    validations = []
    
    for filename in ['users.csv', 'items.csv', 'purchases.csv', 'clicks.csv']:
        filepath = os.path.join(output_dir, filename)
        if not os.path.exists(filepath):
            raise Exception(f"Missing file: {filename}")
        
        df = pd.read_csv(filepath)
        row_count = len(df)
        
        print(f"✓ {filename}: {row_count} rows")
        validations.append((filename, row_count))
    
    # Validate users are in expected range
    users_df = pd.read_csv(os.path.join(output_dir, 'users.csv'))
    n_users = len(users_df)
    
    if n_users < 500 or n_users > 600:
        print(f"Warning: Expected 500-600 new users, got {n_users}")
    
    print(f"\n✓ Validation passed!")
    return validations

with DAG(
    'generate_incremental_marketplace_data',
    default_args=default_args,
    description='Generate incremental monthly marketplace data with user state tracking',
    schedule_interval='0 1 1 * *',  # 1am on the 1st of each month
    catchup=False,  # Enable to backfill historical months
    max_active_runs=1,  # Only one month at a time
    tags=['data-generation', 'ingestion', 'incremental'],
) as dag:
    
    # Task 1: Generate CSV data
    generate_data = PythonOperator(
        task_id='generate_incremental_data',
        python_callable=generate_monthly_data,
        provide_context=True,
    )
    
    # Task 2: Validate generated data
    validate = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        provide_context=True,
    )
    
    # Task 3: Upload to GCS
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        src='/tmp/marketplace_data/{{ execution_date.strftime("%Y-%m") }}/*.csv',
        dst='raw/{{ execution_date.strftime("%Y-%m") }}/',
        bucket='marketplace-analytics-485915-data-lake',
        gcp_conn_id='google_cloud_default',
    )
    
    # Task 4: Cleanup local files
    cleanup = BashOperator(
        task_id='cleanup_local_files',
        bash_command='rm -rf /tmp/marketplace_data/{{ execution_date.strftime("%Y-%m") }}',
    )
    
    # Define task dependencies
    generate_data >> validate >> upload_to_gcs >> cleanup
