import streamlit as st
import pandas as pd
import plotly.express as px
import boto3
from pyathena import connect
import os
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(
    page_title="US Flight Delay Intelligence",
    page_icon="✈",
    layout="wide"
)

@st.cache_resource
def get_connection():
    return connect(
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
        region_name=os.getenv("AWS_DEFAULT_REGION"),
        s3_staging_dir="s3://flight-delays-processed/athena-results/",
        schema_name="flight_delays_db"
    )

@st.cache_data(ttl=3600)
def run_query(query):
    conn = get_connection()
    return pd.read_sql(query, conn)

@st.cache_data(ttl=3600)
def load_airline_summary():
    return run_query("SELECT * FROM flight_delays_db.mart_airline_summary")

@st.cache_data(ttl=3600)
def load_airport_summary():
    return run_query("SELECT * FROM flight_delays_db.mart_airport_summary")

@st.cache_data(ttl=3600)
def load_day_of_week():
    return run_query("""
        SELECT
            day_of_week,
            COUNT(*) as total_flights,
            ROUND(AVG(arrival_delay_mins), 2) as avg_delay,
            ROUND(SUM(CASE WHEN arrival_delay_mins > 15 THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) as delay_rate_pct
        FROM flight_delays_db.fct_delays
        WHERE is_cancelled = 0
        GROUP BY day_of_week
    """)

@st.cache_data(ttl=3600)
def load_cancellation_reasons():
    return run_query("""
        SELECT
            cancellation_reason_desc,
            COUNT(*) as total_cancellations
        FROM flight_delays_db.fct_delays
        WHERE is_cancelled = 1
        AND cancellation_reason_desc != 'Not Cancelled'
        GROUP BY cancellation_reason_desc
        ORDER BY total_cancellations DESC
    """)

@st.cache_data(ttl=3600)
def load_delay_distribution():
    return run_query("""
        SELECT
            delay_severity,
            COUNT(*) as total_flights
        FROM flight_delays_db.fct_delays
        WHERE is_cancelled = 0
        GROUP BY delay_severity
    """)

# ── header ───────────────────────────────────────────────────
st.title("US Flight Delay Intelligence")
st.markdown("Analyzing 580,000+ domestic flights from December 2025 BTS data.")
st.markdown("---")

with st.spinner("Loading data from Athena..."):
    try:
        airline_df = load_airline_summary()
        airport_df = load_airport_summary()
        dow_df = load_day_of_week()
        cancel_df = load_cancellation_reasons()
        delay_dist_df = load_delay_distribution()
    except Exception as e:
        st.error(f"Error loading data: {e}")
        st.stop()

# ── top level metrics ────────────────────────────────────────
st.subheader("Overall picture")
col1, col2, col3, col4 = st.columns(4)

total_flights = int(airline_df["total_flights"].sum())
total_delayed = int(airline_df["total_delayed"].sum())
total_cancelled = int(airline_df["total_cancelled"].sum())
overall_delay_rate = round(total_delayed / total_flights * 100, 1)

col1.metric("Total Flights", f"{total_flights:,}")
col2.metric("Delayed Flights", f"{total_delayed:,}")
col3.metric("Cancelled Flights", f"{total_cancelled:,}")
col4.metric("Overall Delay Rate", f"{overall_delay_rate}%")

st.markdown("---")

# ── airline performance ──────────────────────────────────────
st.subheader("Airline performance — who delays you the most?")

col1, col2 = st.columns(2)

with col1:
    fig = px.bar(
        airline_df.sort_values("delay_rate_pct", ascending=True),
        x="delay_rate_pct",
        y="airline_name",
        orientation="h",
        title="Delay rate by airline (%)",
        color="delay_rate_pct",
        color_continuous_scale="Reds",
        labels={"delay_rate_pct": "Delay Rate (%)", "airline_name": "Airline"}
    )
    fig.update_layout(showlegend=False, coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)

with col2:
    fig = px.bar(
        airline_df.sort_values("avg_arrival_delay_mins", ascending=True),
        x="avg_arrival_delay_mins",
        y="airline_name",
        orientation="h",
        title="Average arrival delay by airline (mins)",
        color="avg_arrival_delay_mins",
        color_continuous_scale="Oranges",
        labels={"avg_arrival_delay_mins": "Avg Delay (mins)", "airline_name": "Airline"}
    )
    fig.update_layout(showlegend=False, coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)

with st.expander("Full airline stats table"):
    st.dataframe(
        airline_df[[
            "airline_name", "total_flights", "total_delayed",
            "total_cancelled", "delay_rate_pct",
            "cancellation_rate_pct", "avg_arrival_delay_mins",
            "worst_delay_mins"
        ]].sort_values("delay_rate_pct", ascending=False),
        use_container_width=True
    )

st.markdown("---")

# ── airport performance ──────────────────────────────────────
st.subheader("Airport performance — worst departure airports")

top_airports = airport_df.sort_values("delay_rate_pct", ascending=False).head(20)

col1, col2 = st.columns(2)

with col1:
    fig = px.bar(
        top_airports.sort_values("delay_rate_pct", ascending=True),
        x="delay_rate_pct",
        y="airport_code",
        orientation="h",
        title="Top 20 airports by delay rate (%)",
        color="delay_rate_pct",
        color_continuous_scale="Reds",
        labels={"delay_rate_pct": "Delay Rate (%)", "airport_code": "Airport"}
    )
    fig.update_layout(showlegend=False, coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)

with col2:
    fig = px.bar(
        top_airports.sort_values("avg_departure_delay_mins", ascending=True),
        x="avg_departure_delay_mins",
        y="airport_code",
        orientation="h",
        title="Top 20 airports by avg departure delay (mins)",
        color="avg_departure_delay_mins",
        color_continuous_scale="Oranges",
        labels={"avg_departure_delay_mins": "Avg Delay (mins)", "airport_code": "Airport"}
    )
    fig.update_layout(showlegend=False, coloraxis_showscale=False)
    st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ── day of week ──────────────────────────────────────────────
st.subheader("Best and worst days to fly")

day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
dow_df["day_of_week"] = pd.Categorical(dow_df["day_of_week"], categories=day_order, ordered=True)
dow_df = dow_df.sort_values("day_of_week")

fig = px.bar(
    dow_df,
    x="day_of_week",
    y="delay_rate_pct",
    title="Delay rate by day of week (%)",
    color="delay_rate_pct",
    color_continuous_scale="RdYlGn_r",
    labels={"delay_rate_pct": "Delay Rate (%)", "day_of_week": "Day"}
)
fig.update_layout(showlegend=False, coloraxis_showscale=False)
st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ── delay severity + cancellations ───────────────────────────
st.subheader("How bad are the delays?")

col1, col2 = st.columns(2)

with col1:
    severity_order = ["Early/On-time", "Minor Delay", "Moderate Delay", "Severe Delay", "Extreme Delay"]
    delay_dist_df["delay_severity"] = pd.Categorical(
        delay_dist_df["delay_severity"],
        categories=severity_order,
        ordered=True
    )
    delay_dist_df = delay_dist_df.sort_values("delay_severity")
    fig = px.pie(
        delay_dist_df,
        names="delay_severity",
        values="total_flights",
        title="Flight distribution by delay severity",
        color_discrete_sequence=px.colors.sequential.RdBu
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    if not cancel_df.empty:
        fig = px.pie(
            cancel_df,
            names="cancellation_reason_desc",
            values="total_cancellations",
            title="Why are flights getting cancelled?",
            color_discrete_sequence=px.colors.sequential.Reds
        )
        st.plotly_chart(fig, use_container_width=True)

st.markdown("---")

# ── airline drill down ───────────────────────────────────────
st.subheader("Drill down — pick an airline")

selected_airline = st.selectbox(
    "Select an airline to see detailed stats",
    options=airline_df["airline_name"].tolist()
)

selected = airline_df[airline_df["airline_name"] == selected_airline].iloc[0]

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Total Flights", f"{int(selected['total_flights']):,}")
col2.metric("Delay Rate", f"{selected['delay_rate_pct']}%")
col3.metric("Cancellation Rate", f"{selected['cancellation_rate_pct']}%")
col4.metric("Avg Arrival Delay", f"{selected['avg_arrival_delay_mins']} mins")
col5.metric("Worst Delay Ever", f"{int(selected['worst_delay_mins'])} mins")

st.markdown("---")
st.caption("Data source: Bureau of Transportation Statistics (BTS) — December 2025. Pipeline built with Python, AWS S3, AWS Glue, AWS Athena, dbt, and Streamlit.")