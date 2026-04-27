import awswrangler as wr
import boto3
from dotenv import load_dotenv
import os
import pandas as pd

print("Starting quality checks...")

load_dotenv()

boto3_session = boto3.Session(
    aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
    region_name=os.getenv("AWS_DEFAULT_REGION")
)

PROCESSED_BUCKET = os.getenv("S3_PROCESSED_BUCKET")

VALID_AIRLINE_CODES = [
    "AA", "DL", "UA", "WN", "B6", "AS", "NK", "F9",
    "HA", "G4", "MQ", "OO", "YX", "9E", "YV", "OH"
]

def run_quality_checks():
    print("Reading cleaned data from S3...")
    df = wr.s3.read_csv(
        path=f"s3://{PROCESSED_BUCKET}/processed/flights_cleaned/flights_cleaned.csv",
        boto3_session=boto3_session
    )

    print(f"Total rows loaded: {len(df):,}")
    checks = []

    # 1. NULL CHECKS
    for col in ["flight_date", "airline_code", "origin_airport", "dest_airport"]:
        null_count = int(df[col].isnull().sum())
        status = "PASS" if null_count == 0 else "FAIL"
        checks.append(("NULL CHECK", f"No nulls in {col}", status, f"{null_count} nulls found"))

    # 2. DATA TYPE CHECKS
    for col in ["dep_delay", "arr_delay", "distance", "air_time"]:
        status = "PASS" if df[col].dtype in ["float64", "int64"] else "FAIL"
        checks.append(("TYPE CHECK", f"{col} is numeric", status, str(df[col].dtype)))

    # 3. RANGE CHECKS
    negative_distance = int((df["distance"] < 0).sum())
    status = "PASS" if negative_distance == 0 else "FAIL"
    checks.append(("RANGE CHECK", "Distance always positive", status, f"{negative_distance} negative values"))

    not_cancelled = df[df["is_cancelled"] == 0]
    negative_airtime = int((not_cancelled["air_time"] < 0).sum())
    status = "PASS" if negative_airtime == 0 else "FAIL"
    checks.append(("RANGE CHECK", "AirTime positive for non-cancelled flights", status, f"{negative_airtime} negative values"))

    unrealistic_dep = int(((df["dep_delay"] < -120) | (df["dep_delay"] > 1500)).sum())
    status = "PASS" if unrealistic_dep == 0 else "FAIL"
    checks.append(("RANGE CHECK", "dep_delay within realistic bounds", status, f"{unrealistic_dep} outliers"))

    unrealistic_arr = int(((df["arr_delay"] < -120) | (df["arr_delay"] > 1500)).sum())
    status = "PASS" if unrealistic_arr == 0 else "FAIL"
    checks.append(("RANGE CHECK", "arr_delay within realistic bounds", status, f"{unrealistic_arr} outliers"))

    # 4. VALIDITY CHECKS
    invalid_cancelled = int(df[~df["is_cancelled"].isin([0, 1, 0.0, 1.0])]["is_cancelled"].count())
    status = "PASS" if invalid_cancelled == 0 else "FAIL"
    checks.append(("VALIDITY CHECK", "is_cancelled column only 0 or 1", status, f"{invalid_cancelled} invalid values"))

    unknown_airlines = df[~df["airline_code"].isin(VALID_AIRLINE_CODES)]["airline_code"].nunique()
    status = "PASS" if unknown_airlines == 0 else "WARN"
    checks.append(("VALIDITY CHECK", "Airline codes are known carriers", status, f"{unknown_airlines} unknown codes found"))

    invalid_origin = int((df["origin_airport"].str.len() != 3).sum())
    invalid_dest = int((df["dest_airport"].str.len() != 3).sum())
    status = "PASS" if invalid_origin == 0 else "FAIL"
    checks.append(("VALIDITY CHECK", "origin_airport is valid 3-letter code", status, f"{invalid_origin} invalid values"))
    status = "PASS" if invalid_dest == 0 else "FAIL"
    checks.append(("VALIDITY CHECK", "dest_airport is valid 3-letter code", status, f"{invalid_dest} invalid values"))

    # 5. CONSISTENCY CHECKS
    cancelled_with_airtime = int(((df["is_cancelled"] == 1) & (df["air_time"] > 0)).sum())
    status = "PASS" if cancelled_with_airtime == 0 else "FAIL"
    checks.append(("CONSISTENCY CHECK", "Cancelled flights have no air_time", status, f"{cancelled_with_airtime} inconsistent rows"))

    df["flight_date"] = pd.to_datetime(df["flight_date"], errors="coerce")
    out_of_range_dates = int(((df["flight_date"].dt.year < 2000) | (df["flight_date"].dt.year > 2026)).sum())
    status = "PASS" if out_of_range_dates == 0 else "FAIL"
    checks.append(("CONSISTENCY CHECK", "flight_date within valid year range", status, f"{out_of_range_dates} out of range"))

    # 6. COMPLETENESS CHECKS
    non_cancelled = df[df["is_cancelled"] == 0]
    arr_delay_fill_rate = (non_cancelled["arr_delay"].notnull().sum() / len(non_cancelled)) * 100
    status = "PASS" if arr_delay_fill_rate > 90 else "FAIL"
    checks.append(("COMPLETENESS CHECK", "arr_delay fill rate > 90% for non-cancelled", status, f"{arr_delay_fill_rate:.1f}% filled"))

    duplicate_count = int(df.duplicated().sum())
    status = "PASS" if duplicate_count == 0 else "FAIL"
    checks.append(("COMPLETENESS CHECK", "No duplicate rows", status, f"{duplicate_count} duplicates found"))

    # PRINT REPORT
    print("\n" + "="*70)
    print("DATA QUALITY REPORT")
    print("="*70)

    current_category = ""
    passed = 0
    failed = 0
    warned = 0

    for category, check_name, status, detail in checks:
        if category != current_category:
            print(f"\n{category}")
            print("-" * 50)
            current_category = category

        if status == "PASS":
            icon = "PASS"
            passed += 1
        elif status == "WARN":
            icon = "WARN"
            warned += 1
        else:
            icon = "FAIL"
            failed += 1

        print(f"  [{icon}] {check_name}")
        print(f"         {detail}")

    print("\n" + "="*70)
    print(f"SUMMARY: {passed} passed | {warned} warnings | {failed} failed")
    print(f"Total checks run: {len(checks)}")

    if failed == 0:
        print("Data is clean and ready for analysis.")
    else:
        print("Some checks failed - review before proceeding.")
    print("="*70)

run_quality_checks()