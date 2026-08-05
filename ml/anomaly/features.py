import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

def get_engine():
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5434")
    db   = os.getenv("POSTGRES_DB", "smart_city")
    user = os.getenv("POSTGRES_USER", "postgres")
    pw   = os.getenv("POSTGRES_PASSWORD")
    return create_engine(f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}")

RAW_QUERY = """
select city, observed_at, aqi, pm2_5_ug_m3, pm10_ug_m3
from intermediate.int_city_hourly_pollution
order by city, observed_at
"""

def load_raw():
    return pd.read_sql(RAW_QUERY, get_engine(), parse_dates=["observed_at"])

def build_features(df):
    df = df.copy()
    df["hour_of_day"] = df["observed_at"].dt.hour
    df["day_of_week"] = df["observed_at"].dt.dayofweek
    df["city_code"] = df["city"].astype("category").cat.codes

    feature_cols = ["pm2_5_ug_m3", "pm10_ug_m3", "aqi", "hour_of_day", "day_of_week", "city_code"]
    return df, feature_cols