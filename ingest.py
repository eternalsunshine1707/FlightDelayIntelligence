import boto3
import pandas as pd
from dotenv import load_dotenv
import os

# Load environment variables from .env file
# This is how we keep AWS credentials out of the code
load_dotenv()

# Read credentials from .env
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")
RAW_BUCKET = os.getenv("S3_RAW_BUCKET")

LOCAL_FILE_PATH = "data/flights_raw.csv"
S3_FILE_KEY = "raw/flights_raw.csv"

def load_and_validate(filepath):
    """
    Load the CSV and do basic validation.
    We want to make sure the file isn't empty and 
    has the columns we expect before uploading anything.
    """
    print(f"Loading data from {filepath}...")
    df = pd.read_csv(filepath, low_memory=False, encoding='latin-1', skiprows=0)
    
    print(f"Rows loaded: {len(df):,}")
    print(f"Columns: {list(df.columns)}")
    
    # Basic checks
    assert len(df) > 0, "File is empty - something went wrong"
    
    expected_columns = [
        "FlightDate", "Reporting_Airline", "Origin", 
        "Dest", "DepDelay", "ArrDelay", "Cancelled"
    ]
    
    missing = [col for col in expected_columns if col not in df.columns]
    if missing:
        print(f"Warning - these expected columns are missing: {missing}")
    else:
        print("All expected columns present.")
    
    return df

def upload_to_s3(local_path, bucket, s3_key):
    """
    Upload the raw file to S3.
    We're uploading the original untouched file - 
    this is the raw layer of our data lake.
    """
    print(f"\nUploading to S3 bucket: {bucket}...")
    
    s3_client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_DEFAULT_REGION
    )
    
    s3_client.upload_file(local_path, bucket, s3_key)
    print(f"Successfully uploaded to s3://{bucket}/{s3_key}")

if __name__ == "__main__":
    # Step 1 - load and validate
    df = load_and_validate(LOCAL_FILE_PATH)
    
    # Step 2 - upload raw file to S3
    upload_to_s3(LOCAL_FILE_PATH, RAW_BUCKET, S3_FILE_KEY)
    
    print("\nIngestion complete.")