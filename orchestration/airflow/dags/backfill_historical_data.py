#!/usr/bin/env python3
"""
Historical Data Backfill Script

Generates 14 months of historical marketplace data (Jan 2025 - Feb 2026)
with proper incremental user growth and realistic activity scaling.

Usage:
    python3 backfill_historical_data.py
"""

import subprocess
import os
from datetime import datetime, timedelta
from pathlib import Path
import json
from google.cloud import storage

# Configuration
PROJECT_ID = "marketplace-analytics-485915"
BUCKET_NAME = f"{PROJECT_ID}-data-lake"
STATE_FILE = "state/user_state.json"
START_MONTH = "2025-01"  # January 2025
END_MONTH = "2026-02"    # February 2026

# Set credentials
CREDENTIALS_PATH = os.path.expanduser('~/.keys/service-account-key.json')
if os.path.exists(CREDENTIALS_PATH):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/Users/biancaniemann/.keys/service-account-key.json"

def generate_month_list(start_month, end_month):
    """Generate list of months between start and end (inclusive)"""
    start = datetime.strptime(start_month, "%Y-%m")
    end = datetime.strptime(end_month, "%Y-%m")
    
    months = []
    current = start
    while current <= end:
        months.append(current.strftime("%Y-%m"))
        # Move to next month
        if current.month == 12:
            current = datetime(current.year + 1, 1, 1)
        else:
            current = datetime(current.year, current.month + 1, 1)
    
    return months

def clear_gcs_state():
    """Clear existing state file to start fresh"""
    try:
        storage_client = storage.Client(project=PROJECT_ID)
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(STATE_FILE)
        
        if blob.exists():
            print(f"🗑️  Deleting existing state file...")
            blob.delete()
            print(f"✓ Deleted: gs://{BUCKET_NAME}/{STATE_FILE}")
        else:
            print(f"✓ No existing state file to delete")
    except Exception as e:
        print(f"⚠️  Warning: Could not delete state file: {e}")

def generate_month_data(year_month, script_path):
    """Generate data for a specific month"""
    
    print(f"\n{'='*70}")
    print(f"GENERATING DATA FOR {year_month}")
    print(f"{'='*70}")
    
    output_dir = f'/tmp/marketplace_data/{year_month}'
    os.makedirs(output_dir, exist_ok=True)
    
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
        
        print("✓ Data generation successful!")
        
        # Show summary from output
        if "Total user base:" in result.stdout:
            for line in result.stdout.split('\n'):
                if any(keyword in line for keyword in [
                    'new users', 'Total user base', 'Items:', 'Purchases:', 
                    'Searches:', 'Last user ID'
                ]):
                    print(f"  {line.strip()}")
        
        # Verify files were created
        files = os.listdir(output_dir)
        print(f"  Generated {len(files)} CSV files")
        
        if len(files) < 8:
            raise Exception(f"Expected 8 CSV files, found {len(files)}")
        
        return output_dir, True
        
    except subprocess.CalledProcessError as e:
        print(f"❌ Generation failed!")
        print(f"STDOUT: {e.stdout}")
        print(f"STDERR: {e.stderr}")
        return output_dir, False
    except Exception as e:
        print(f"❌ Error: {e}")
        return output_dir, False

def upload_to_gcs(local_dir, year_month):
    """Upload generated CSV files to GCS"""
    
    print(f"\n📤 Uploading to GCS...")
    
    try:
        storage_client = storage.Client(project=PROJECT_ID)
        bucket = storage_client.bucket(BUCKET_NAME)
        
        gcs_prefix = f"raw/{year_month}/"
        local_path = Path(local_dir)
        
        csv_files = list(local_path.glob("*.csv"))
        
        for csv_file in csv_files:
            blob_name = f"{gcs_prefix}{csv_file.name}"
            blob = bucket.blob(blob_name)
            
            print(f"  Uploading {csv_file.name}...", end=" ")
            blob.upload_from_filename(str(csv_file))
            print("✓")
        
        print(f"✓ Uploaded {len(csv_files)} files to gs://{BUCKET_NAME}/{gcs_prefix}")
        return True
        
    except Exception as e:
        print(f"❌ Upload failed: {e}")
        return False

def cleanup_local_files(local_dir):
    """Clean up local temporary files"""
    try:
        import shutil
        shutil.rmtree(local_dir)
        print(f"✓ Cleaned up: {local_dir}")
    except Exception as e:
        print(f"⚠️  Warning: Could not cleanup {local_dir}: {e}")

def show_final_summary(months, successes):
    """Show summary of backfill process"""
    
    print("\n" + "="*70)
    print("BACKFILL COMPLETE!")
    print("="*70)
    
    print(f"\nMonths processed: {len(months)}")
    print(f"Successful: {successes}")
    print(f"Failed: {len(months) - successes}")
    
    if successes == len(months):
        print("\n✅ All months generated successfully!")
        
        print("\n📊 Next steps:")
        print("  1. Load data to BigQuery:")
        print("     cd ~/airflow")
        print("     airflow dags backfill gcs_to_bigquery_incremental \\")
        print(f"       --start-date 2025-01-01 \\")
        print(f"       --end-date 2026-02-28")
        
        print("\n  2. Or load month by month:")
        for month in months:
            print(f"     airflow dags trigger gcs_to_bigquery_incremental --exec-date {month}-01")
        
        print("\n  3. Run dbt models:")
        print("     dbt run --select staging.* --full-refresh")
        print("     dbt run --select marts.*")
        
        print("\n  4. From March 2026 onwards, your DAGs will run automatically!")
        
        # Show final state
        try:
            storage_client = storage.Client(project=PROJECT_ID)
            bucket = storage_client.bucket(BUCKET_NAME)
            blob = bucket.blob(STATE_FILE)
            
            if blob.exists():
                state_json = blob.download_as_string()
                state = json.loads(state_json)
                
                print(f"\n📋 Final User State:")
                print(f"   Total users: {len(state.get('users', []))}")
                print(f"   Last user ID: {state.get('last_user_id')}")
                print(f"   Next month (Mar 2026) will add 500-600 users starting from ID {state.get('last_user_id', 0) + 1}")
        except:
            pass
    else:
        print("\n⚠️  Some months failed. Check the logs above.")

def main():
    """Main backfill process"""
    
    print("\n" + "*"*70)
    print("*" + " "*68 + "*")
    print("*" + "HISTORICAL DATA BACKFILL: JAN 2025 - FEB 2026".center(68) + "*")
    print("*" + " "*68 + "*")
    print("*"*70)
    
    # Get script path
    script_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        'generate_incremental_data.py'
    )
    
    if not os.path.exists(script_path):
        print(f"\n❌ Error: Could not find {script_path}")
        print("   Make sure generate_incremental_data.py is in the same directory")
        return
    
    print(f"\nUsing script: {script_path}")
    print(f"Project: {PROJECT_ID}")
    print(f"Bucket: {BUCKET_NAME}")
    
    # Generate month list
    months = generate_month_list(START_MONTH, END_MONTH)
    print(f"\nWill generate {len(months)} months of data:")
    print(f"  From: {months[0]}")
    print(f"  To:   {months[-1]}")
    
    # Confirm before proceeding
    response = input("\n⚠️  This will clear existing state and regenerate all data. Continue? (yes/no): ")
    if response.lower() != 'yes':
        print("Cancelled.")
        return
    
    # Clear existing state to start fresh
    print("\n" + "="*70)
    print("CLEARING EXISTING STATE")
    print("="*70)
    clear_gcs_state()
    
    # Generate data for each month
    successes = 0
    
    for i, month in enumerate(months, 1):
        print(f"\n\n{'#'*70}")
        print(f"# MONTH {i}/{len(months)}: {month}")
        print(f"{'#'*70}")
        
        # Generate
        output_dir, success = generate_month_data(month, script_path)
        
        if not success:
            print(f"⚠️  Skipping upload for {month} due to generation failure")
            continue
        
        # Upload to GCS
        if upload_to_gcs(output_dir, month):
            successes += 1
        
        # Cleanup
        cleanup_local_files(output_dir)
        
        print(f"\n✓ Month {i}/{len(months)} complete!")
        
        # Show progress
        remaining = len(months) - i
        if remaining > 0:
            print(f"   {remaining} month(s) remaining...")
    
    # Final summary
    show_final_summary(months, successes)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n⚠️  Backfill interrupted by user")
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        import traceback
        traceback.print_exc()