#!/usr/bin/env python3
"""
Upload CSV files from local directory to GCS bucket.
Usage: python upload_to_gcs.py
"""

from google.cloud import storage
import os
from pathlib import Path

# Configuration
PROJECT_ID = "marketplace-analytics-485915"
BUCKET_NAME = f"{PROJECT_ID}-data-lake"
LOCAL_CSV_DIR = "/Users/biancaniemann/Documents/Portfolio/marketplace-analytics/data_generator_script_and_files/csv_files"
GCS_PREFIX = "raw/"  # Folder in bucket

# Path to your service account key
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/biancaniemann/.keys/service-account-key.json"


def upload_csvs_to_gcs():
    """Upload all CSV files from local directory to GCS bucket."""
    
    # Initialize GCS client
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)
    
    # Get all CSV files
    csv_dir = Path(LOCAL_CSV_DIR)
    csv_files = list(csv_dir.glob("*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {LOCAL_CSV_DIR}")
        return
    
    print(f"Found {len(csv_files)} CSV files to upload\n")
    
    # Upload each file
    for csv_file in csv_files:
        blob_name = f"{GCS_PREFIX}{csv_file.name}"
        blob = bucket.blob(blob_name)
        
        print(f"Uploading {csv_file.name} → gs://{BUCKET_NAME}/{blob_name}")
        blob.upload_from_filename(str(csv_file))
        print(f"✓ Uploaded successfully\n")
    
    print(f"All files uploaded to gs://{BUCKET_NAME}/{GCS_PREFIX}")


if __name__ == "__main__":
    try:
        upload_csvs_to_gcs()
    except Exception as e:
        print(f"Error: {e}")
