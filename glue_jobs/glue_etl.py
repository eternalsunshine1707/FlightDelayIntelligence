import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

RAW_BUCKET = "flight-delays-raw"
PROCESSED_BUCKET = "flight-delays-processed"

print("Reading raw flight data from S3...")
df = spark.read.option("header", "true").csv(
    f"s3://{RAW_BUCKET}/raw/flights_raw.csv"
)

print(f"Raw row count: {df.count():,}")

# Rename columns to clean names
df = df.withColumnRenamed("FL_DATE", "FlightDate") \
       .withColumnRenamed("OP_UNIQUE_CARRIER", "Reporting_Airline") \
       .withColumnRenamed("ORIGIN", "Origin") \
       .withColumnRenamed("ORIGIN_CITY_NAME", "OriginCityName") \
       .withColumnRenamed("DEST", "Dest") \
       .withColumnRenamed("DEST_CITY_NAME", "DestCityName") \
       .withColumnRenamed("DEP_DELAY", "DepDelay") \
       .withColumnRenamed("ARR_DELAY", "ArrDelay") \
       .withColumnRenamed("CANCELLED", "Cancelled") \
       .withColumnRenamed("CANCELLATION_CODE", "CancellationCode") \
       .withColumnRenamed("AIR_TIME", "AirTime") \
       .withColumnRenamed("DISTANCE", "Distance")

# Cast columns to correct types
df = df.withColumn("DepDelay", df["DepDelay"].cast(DoubleType())) \
       .withColumn("ArrDelay", df["ArrDelay"].cast(DoubleType())) \
       .withColumn("Cancelled", df["Cancelled"].cast(DoubleType())) \
       .withColumn("AirTime", df["AirTime"].cast(DoubleType())) \
       .withColumn("Distance", df["Distance"].cast(DoubleType()))

# Drop rows where critical fields are null
df = df.dropna(subset=["FlightDate", "Reporting_Airline"])

# Add derived columns
df = df.withColumn("is_delayed", F.when(df["ArrDelay"] > 15, 1).otherwise(0))
df = df.withColumn("day_of_week", F.dayofweek(F.to_date("FlightDate")))
df = df.withColumn("month", F.month(F.to_date("FlightDate")))
df = df.withColumn("year", F.year(F.to_date("FlightDate")))
df = df.withColumn(
    "delay_category",
    F.when(df["ArrDelay"] <= 0, "Early/On-time")
     .when(df["ArrDelay"] <= 15, "Minor delay")
     .when(df["ArrDelay"] <= 45, "Moderate delay")
     .when(df["ArrDelay"] <= 120, "Severe delay")
     .otherwise("Extreme delay")
)

print(f"Cleaned row count: {df.count():,}")

# Write to processed bucket as Parquet
print("Writing cleaned data to S3 as Parquet...")
df.write.mode("overwrite").parquet(
    f"s3://{PROCESSED_BUCKET}/glue_processed/flights_cleaned/"
)

print("Glue job complete.")
job.commit()