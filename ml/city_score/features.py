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
select city, date_utc, comfort_index, norm_temp, norm_aqi, norm_traffic, rolling_7d_comfort
from marts.mart_city_daily
order by city, date_utc
"""

def load_raw():
    return pd.read_sql(RAW_QUERY, get_engine(), parse_dates=["date_utc"])

def build_features(df, horizon_days=1, tolerance_days=1):
    df = df.copy()
    df = df.sort_values(["city", "date_utc"]).drop_duplicates(["city", "date_utc"]).reset_index(drop=True)
    df["row_id"] = df.index

    def nearest_comfort_at(offset_days):
        src = df[["city", "date_utc", "comfort_index"]].sort_values("date_utc")
        src["date_utc"] = src["date_utc"].astype("datetime64[ns]")
        want = df[["row_id", "city", "date_utc"]].copy()
        want["want_ts"] = (want["date_utc"] + pd.Timedelta(days=offset_days)).astype("datetime64[ns]")
        want = want.sort_values("want_ts")
        merged = pd.merge_asof(
            want, src,
            left_on="want_ts", right_on="date_utc",
            by="city", direction="nearest",
            tolerance=pd.Timedelta(days=tolerance_days),
            suffixes=("", "_src"),
        )
        merged = merged.set_index("row_id").reindex(df["row_id"])
        return merged["comfort_index"].to_numpy()

    df["target_comfort"] = nearest_comfort_at(horizon_days)
    df["day_of_week"] = df["date_utc"].dt.dayofweek
    df["city_code"] = df["city"].astype("category").cat.codes

    feature_cols = [
        "comfort_index", "norm_temp", "norm_aqi", "norm_traffic",
        "rolling_7d_comfort", "day_of_week", "city_code",
    ]
    return df, feature_cols