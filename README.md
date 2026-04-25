# US Flight Delay Intelligence Pipeline

An end-to-end data engineering project that ingests, transforms, and analyzes 
580,000+ real US domestic flight records from the Bureau of Transportation Statistics.

Built this to answer a simple question most travelers have but nobody has good data on: 
which airlines actually delay you the most, and how bad is it really?

---

## What this project does

Raw flight data from the BTS website goes through a full pipeline:

- Pulled into AWS S3 as the raw data lake layer
- Cleaned and transformed using Python and Pandas
- Data quality checks run automatically to catch issues before they reach the dashboard
- AWS Glue job handles the same transformation at scale using PySpark
- dbt models build three analytical layers on top of Athena - staging, facts, and aggregated marts
- Streamlit dashboard surfaces the insights

The pipeline is automated via GitHub Actions and designed to run on a weekly schedule. Data is sourced from BTS monthly releases - to refresh with new data, download the latest monthly CSV from transtats.bts.gov and upload it to the raw S3 bucket.

---

## What I found in the data

- Overall delay rate for December 2025: 25.9% of flights arrived more than 15 minutes late
- JetBlue had the highest delay rate at 35%
- PSA Airlines had the worst average arrival delay
- Southwest had the most flights but one of the lower delay rates

---

## Tech stack

| Layer | Tool |
|---|---|
| Cloud storage | AWS S3 |
| ETL at scale | AWS Glue (PySpark) |
| Query engine | AWS Athena |
| Transformation | dbt Core |
| Data quality | Python + custom checks |
| Orchestration | GitHub Actions |
| Dashboard | Streamlit |
| Language | Python |

---

## Project structure

```
flight-delay-intelligence/
│
├── ingest.py                        # pulls raw BTS data into S3
├── transform.py                     # cleans, deduplicates, transforms data
├── quality_checks.py                # 20 automated data quality checks
├── requirements.txt                 # python dependencies
├── .env.example                     # template for AWS credentials
│
├── glue_jobs/
│   └── glue_etl.py                  # PySpark ETL job for AWS Glue
│
├── dbt/
│   └── flight_delays/
│       └── models/
│           ├── staging/
│           │   └── stg_flights.sql  # raw data cleaned up
│           ├── facts/
│           │   └── fct_delays.sql   # one row per flight, business logic
│           └── marts/
│               ├── mart_airline_summary.sql
│               └── mart_airport_summary.sql
│
├── streamlit_app/
│   └── app.py                       # interactive dashboard
│
├── .streamlit/
│   └── config.toml                  # navy blue dashboard theme
│
└── .github/
    └── workflows/
        └── pipeline.yml             # automated weekly run via GitHub Actions
```
---

## Data source

Bureau of Transportation Statistics - Airline On-Time Performance Data
https://www.transtats.bts.gov

Free, public, updated monthly. No API key needed.

---

## How to run this locally

1. Clone the repo
2. Create a virtual environment with Python 3.11:
python -m venv venv
venv\Scripts\activate
3. Install dependencies:
pip install -r requirements.txt
4. Create a `.env` file in the root folder with your AWS credentials:
AWS_ACCESS_KEY_ID=your_key_here
AWS_SECRET_ACCESS_KEY=your_secret_here
AWS_DEFAULT_REGION=us-east-1
S3_RAW_BUCKET=flight-delays-raw
S3_PROCESSED_BUCKET=flight-delays-processed
5. Download the BTS flight data from the link above and place it in the `data/` folder as `flights_raw.csv`
6. Run the pipeline:
python ingest.py
python transform.py
python quality_checks.py
7. Run the dashboard:
streamlit run streamlit_app/app.py

---

## Data quality checks

The pipeline runs 20 automated checks across 6 categories before any data reaches the dashboard:

- Null checks on critical columns
- Data type validation
- Range and boundary checks on delay values
- Validity checks on airline codes and airport codes
- Consistency checks - cancelled flights shouldn't have airtime recorded
- Completeness checks - fill rate and duplicate detection

Found and handled 1,342 duplicate rows and 76 delay outliers in the raw BTS data.

---

## Pipeline architecture
BTS Website
↓
Python ingestion script → S3 raw bucket
↓
AWS Lambda trigger
↓
AWS Glue PySpark ETL job
↓
S3 processed bucket (CSV)
↓
AWS Athena (query engine)
↓
dbt models (staging → facts → marts)
↓
Streamlit dashboard
↓
GitHub Actions (weekly automation)

---

## Why I built this

I wanted a project that reflects what data engineering actually looks like at work -
messy real-world data, multiple pipeline stages, automated quality checks, and insights
that actually mean something to real people. Flight delays affect everyone and the patterns
in the data tell a clear story.

The data had real issues - city names with commas breaking CSV parsing, duplicate records,
extreme outlier delays that would skew analysis. Handling those problems is what the job is
actually about.