import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import os
from dotenv import load_dotenv

# Load env vars
load_dotenv()
PG_HOST = os.getenv("POSTGRES_HOST")
PG_PORT = os.getenv("POSTGRES_PORT")
PG_USER = os.getenv("POSTGRES_USER")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD")
PG_DB = os.getenv("POSTGRES_DB")

# Connect to PostgreSQL
engine = create_engine(f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}")

st.set_page_config(page_title="CloudJet Dashboard", layout="wide")
st.title("✈️ CloudJet Flight Dashboard")

# Load Data
@st.cache_data(ttl=600)
def load_data():
    df = pd.read_sql("SELECT * FROM flight_weather", engine)
    df["date"] = pd.to_datetime(df["date"])
    df["arrival_time"] = pd.to_datetime(df["arrival_time"])
    return df

df = load_data()

# Sidebar Filters
with st.sidebar:
    st.header("🔍 Filter")
    dates = sorted(df["date"].dt.date.unique())
    selected_date = st.selectbox("Select Date", options=dates)
    cities = sorted(df["city"].dropna().unique())
    selected_city = st.multiselect("Select City", options=cities, default=cities)

# Filter data
filtered = df[(df["date"].dt.date == selected_date) & (df["city"].isin(selected_city))]

# Flight Overview
st.subheader("📊 Flights per Airline")
airline_chart = filtered["airline"].value_counts().reset_index()
airline_chart.columns = ["Airline", "Count"]
st.plotly_chart(px.bar(airline_chart, x="Airline", y="Count", color="Airline"))

# Weather Summary
st.subheader("🌤 Average Temperature by City")
temp_chart = filtered.groupby("city")["temp"].mean().reset_index()
st.plotly_chart(px.bar(temp_chart, x="city", y="temp", title="Avg Temp (°C)", color="city"))

# Flight vs Temp
st.subheader("✈️ Flights vs. Temperature")
count_per_city = filtered.groupby("city").agg({"flight_iata": "count", "temp": "mean"}).reset_index()
count_per_city.columns = ["city", "flight_count", "avg_temp"]
st.plotly_chart(px.scatter(count_per_city, x="avg_temp", y="flight_count", text="city",
                            title="Flights vs Avg Temperature",
                            labels={"avg_temp": "Avg Temp (°C)", "flight_count": "# of Flights"}))
