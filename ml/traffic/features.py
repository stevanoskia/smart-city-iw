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
select city, observed_at, congestion_score, current_speed_kmh, free_flow_speed_kmh
from intermediate.int_city_hourly_traffic_flow
order by city, observed_at
"""

def load_raw():
    return pd.read_sql(RAW_QUERY, get_engine(), parse_dates=["observed_at"])

def build_features(df, horizon_hours=1, tolerance_hours=1):
    df = df.copy()
    df["hour_ts"] = df["observed_at"].dt.floor("h")
    df = (df.sort_values(["city", "observed_at"])
            .drop_duplicates(["city", "hour_ts"], keep="last")
            .sort_values(["city", "hour_ts"]).reset_index(drop=True))
    df["row_id"] = df.index

    def nearest_congestion_at(offset_hours):
        src = df[["city", "hour_ts", "congestion_score"]].sort_values("hour_ts")
        want = df[["row_id", "city", "hour_ts"]].copy()
        want["want_ts"] = want["hour_ts"] + pd.Timedelta(hours=offset_hours)
        want = want.sort_values("want_ts")
        merged = pd.merge_asof(
            want, src, left_on="want_ts", right_on="hour_ts",
            by="city", direction="nearest",
            tolerance=pd.Timedelta(hours=tolerance_hours), suffixes=("", "_src"),
        )
        merged = merged.set_index("row_id").reindex(df["row_id"])
        return merged["congestion_score"].to_numpy()

    df["lag_1h"] = nearest_congestion_at(-1)
    df["target_congestion"] = nearest_congestion_at(horizon_hours)
    df["speed_ratio"] = df["current_speed_kmh"] / df["free_flow_speed_kmh"]
    df["hour_of_day"] = df["hour_ts"].dt.hour
    df["day_of_week"] = df["hour_ts"].dt.dayofweek
    df["city_code"] = df["city"].astype("category").cat.codes

    feature_cols = ["congestion_score", "speed_ratio", "lag_1h", "hour_of_day", "day_of_week", "city_code"]
    return df, feature_cols