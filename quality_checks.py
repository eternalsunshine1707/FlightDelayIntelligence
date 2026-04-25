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

# Known valid airline codes operating in the US
VALID_AIRLINE_CODES = [
    "AA", "DL", "UA", "WN", "B6", "AS", "NK", "F9",
    "HA", "G4", "MQ", "OO", "YX", "9E", "YV", "OH"
]

def run_quality_checks():
    print("Reading cleaned data from S3...")
    df = wr.s3.read_parquet(
        path=f"s3://{PROCESSED_BUCKET}/processed/flights_cleaned/",
        boto3_session=boto3_session
    )

    print(f"Total rows loaded: {len(df):,}")
    checks = []

    # ----------------------------
    # 1. NULL CHECKS
    # ----------------------------
    for col in ["FlightDate", "Reporting_Airline", "Origin", "Dest"]:
        null_count = int(df[col].isnull().sum())
        status = "PASS" if null_count == 0 else "FAIL"
        checks.append(("NULL CHECK", f"No nulls in {col}", status, f"{null_count} nulls found"))

    # ----------------------------
    # 2. DATA TYPE CHECKS
    # ----------------------------
    for col in ["DepDelay", "ArrDelay", "Distance", "AirTime"]:
        status = "PASS" if df[col].dtype in ["float64", "int64"] else "FAIL"
        checks.append(("TYPE CHECK", f"{col} is numeric", status, str(df[col].dtype)))

    # ----------------------------
    # 3. RANGE / BOUNDARY CHECKS
    # ----------------------------
    # Distance should be positive
    negative_distance = int((df["Distance"] < 0).sum())
    status = "PASS" if negative_distance == 0 else "FAIL"
    checks.append(("RANGE CHECK", "Distance always positive", status, f"{negative_distance} negative values"))

    # AirTime should be positive when flight is not cancelled
    not_cancelled = df[df["Cancelled"] == 0]
    negative_airtime = int((not_cancelled["AirTime"] < 0).sum())
    status = "PASS" if negative_airtime == 0 else "FAIL"
    checks.append(("RANGE CHECK", "AirTime positive for non-cancelled flights", status, f"{negative_airtime} negative values"))

    # Departure delay should be within realistic bounds (-120 to 1500 mins)
    unrealistic_dep = int(((df["DepDelay"] < -120) | (df["DepDelay"] > 1500)).sum())
    status = "PASS" if unrealistic_dep == 0 else "FAIL"
    checks.append(("RANGE CHECK", "DepDelay within realistic bounds", status, f"{unrealistic_dep} outliers"))

    # Arrival delay should be within realistic bounds
    unrealistic_arr = int(((df["ArrDelay"] < -120) | (df["ArrDelay"] > 1500)).sum())
    status = "PASS" if unrealistic_arr == 0 else "FAIL"
    checks.append(("RANGE CHECK", "ArrDelay within realistic bounds", status, f"{unrealistic_arr} outliers"))

    # ----------------------------
    # 4. VALIDITY CHECKS
    # ----------------------------
    # Cancelled should only be 0 or 1
    invalid_cancelled = int(df[~df["Cancelled"].isin([0, 1, 0.0, 1.0])]["Cancelled"].count())
    status = "PASS" if invalid_cancelled == 0 else "FAIL"
    checks.append(("VALIDITY CHECK", "Cancelled column only 0 or 1", status, f"{invalid_cancelled} invalid values"))

    # Airline codes should be from known list
    unknown_airlines = df[~df["Reporting_Airline"].isin(VALID_AIRLINE_CODES)]["Reporting_Airline"].nunique()
    status = "PASS" if unknown_airlines == 0 else "WARN"
    checks.append(("VALIDITY CHECK", "Airline codes are known carriers", status, f"{unknown_airlines} unknown codes found"))

    # Origin and Dest should be 3-letter IATA codes
    invalid_origin = int((df["Origin"].str.len() != 3).sum())
    invalid_dest = int((df["Dest"].str.len() != 3).sum())
    status = "PASS" if invalid_origin == 0 else "FAIL"
    checks.append(("VALIDITY CHECK", "Origin is valid 3-letter code", status, f"{invalid_origin} invalid values"))
    status = "PASS" if invalid_dest == 0 else "FAIL"
    checks.append(("VALIDITY CHECK", "Dest is valid 3-letter code", status, f"{invalid_dest} invalid values"))

    # ----------------------------
    # 5. CONSISTENCY CHECKS
    # ----------------------------
    # Cancelled flights should not have delay values that suggest they flew
    cancelled_with_airtime = int(
        ((df["Cancelled"] == 1) & (df["AirTime"] > 0)).sum()
    )
    status = "PASS" if cancelled_with_airtime == 0 else "FAIL"
    checks.append(("CONSISTENCY CHECK", "Cancelled flights have no AirTime", status, f"{cancelled_with_airtime} inconsistent rows"))

    # FlightDate should all be within expected year range
    df["FlightDate"] = pd.to_datetime(df["FlightDate"], errors="coerce")
    out_of_range_dates = int(((df["FlightDate"].dt.year < 2000) | (df["FlightDate"].dt.year > 2026)).sum())
    status = "PASS" if out_of_range_dates == 0 else "FAIL"
    checks.append(("CONSISTENCY CHECK", "FlightDate within valid year range", status, f"{out_of_range_dates} out of range"))

    # ----------------------------
    # 6. COMPLETENESS CHECKS
    # ----------------------------
    # What % of rows have ArrDelay filled (non-cancelled flights should have it)
    non_cancelled = df[df["Cancelled"] == 0]
    arr_delay_fill_rate = (non_cancelled["ArrDelay"].notnull().sum() / len(non_cancelled)) * 100
    status = "PASS" if arr_delay_fill_rate > 90 else "FAIL"
    checks.append(("COMPLETENESS CHECK", "ArrDelay fill rate > 90% for non-cancelled", status, f"{arr_delay_fill_rate:.1f}% filled"))

    # Duplicate row check
    duplicate_count = int(df.duplicated().sum())
    status = "PASS" if duplicate_count == 0 else "FAIL"
    checks.append(("COMPLETENESS CHECK", "No duplicate rows", status, f"{duplicate_count} duplicates found"))

    # ----------------------------
    # PRINT REPORT
    # ----------------------------
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
        print("Data is clean and ready for the next pipeline stage.")
    else:
        print("Some checks failed - review before proceeding.")
    print("="*70)


run_quality_checks()