import boto3
import pandas as pd
import awswrangler as wr
from dotenv import load_dotenv
import os

load_dotenv()

AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_DEFAULT_REGION = os.getenv("AWS_DEFAULT_REGION")
RAW_BUCKET = os.getenv("S3_RAW_BUCKET")
PROCESSED_BUCKET = os.getenv("S3_PROCESSED_BUCKET")

boto3_session = boto3.Session(
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_DEFAULT_REGION
)

def read_raw_from_s3():
    print("Reading raw data from S3...")
    df = wr.s3.read_csv(
        path=f"s3://{RAW_BUCKET}/raw/flights_raw.csv",
        boto3_session=boto3_session,
        low_memory=False
    )
    print(f"Loaded {len(df):,} rows")
    return df

def clean_and_transform(df):
    print("Cleaning and transforming data...")

    # Keep only columns we need - intentionally dropping city name columns
    # because they contain commas which break CSV parsing in Athena
    columns_needed = [
        "FL_DATE", "OP_UNIQUE_CARRIER", "ORIGIN", "DEST",
        "DEP_DELAY", "ARR_DELAY", "CANCELLED",
        "CANCELLATION_CODE", "AIR_TIME", "DISTANCE"
    ]
    existing = [c for c in columns_needed if c in df.columns]
    df = df[existing].copy()

    # Rename to cleaner names
    df = df.rename(columns={
        "FL_DATE": "flight_date",
        "OP_UNIQUE_CARRIER": "airline_code",
        "ORIGIN": "origin_airport",
        "DEST": "dest_airport",
        "DEP_DELAY": "dep_delay",
        "ARR_DELAY": "arr_delay",
        "CANCELLED": "is_cancelled",
        "CANCELLATION_CODE": "cancellation_code",
        "AIR_TIME": "air_time",
        "DISTANCE": "distance"
    })

    # Fix data types
    df["flight_date"] = pd.to_datetime(df["flight_date"], errors="coerce")
    df["dep_delay"] = pd.to_numeric(df["dep_delay"], errors="coerce")
    df["arr_delay"] = pd.to_numeric(df["arr_delay"], errors="coerce")
    df["is_cancelled"] = pd.to_numeric(df["is_cancelled"], errors="coerce")
    df["air_time"] = pd.to_numeric(df["air_time"], errors="coerce")
    df["distance"] = pd.to_numeric(df["distance"], errors="coerce")

    # Drop rows where critical fields are missing
    df = df.dropna(subset=["flight_date", "airline_code"])

    # Remove duplicates
    before = len(df)
    df = df.drop_duplicates()
    print(f"Removed {before - len(df):,} duplicate rows")

    # Cap extreme outliers
    df["dep_delay"] = df["dep_delay"].clip(lower=-120, upper=1500)
    df["arr_delay"] = df["arr_delay"].clip(lower=-120, upper=1500)
    print("Capped extreme delay outliers")

    # Add derived columns
    df["is_delayed"] = (df["arr_delay"] > 15).astype(int)
    df["delay_category"] = pd.cut(
        df["arr_delay"].fillna(0),
        bins=[-999, 0, 15, 45, 120, 999],
        labels=["Early/On-time", "Minor Delay", "Moderate Delay", "Severe Delay", "Extreme Delay"]
    ).astype(str)
    df["day_of_week"] = df["flight_date"].dt.day_name()
    df["month"] = df["flight_date"].dt.month
    df["year"] = df["flight_date"].dt.year
    df["flight_date"] = df["flight_date"].astype(str)

    print(f"Cleaned data: {len(df):,} rows")
    print(f"Delayed flights: {df['is_delayed'].sum():,} ({df['is_delayed'].mean()*100:.1f}%)")
    return df

def save_to_s3(df):
    print("Saving cleaned data to S3 as CSV...")
    wr.s3.to_csv(
        df=df,
        path=f"s3://{PROCESSED_BUCKET}/processed/flights_cleaned/flights_cleaned.csv",
        index=False,
        boto3_session=boto3_session
    )
    print("Saved successfully.")

if __name__ == "__main__":
    df = read_raw_from_s3()
    df = clean_and_transform(df)
    save_to_s3(df)
    print("\nTransformation complete.")