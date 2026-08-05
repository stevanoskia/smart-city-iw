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
select city, observed_at, temp_celsius, humidity_pct, pressure_hpa, wind_speed_ms, cloudiness_pct
from intermediate.int_city_hourly_weather
order by city, observed_at
"""

def load_raw():
    return pd.read_sql(RAW_QUERY, get_engine(), parse_dates=["observed_at"])

def build_features(df, horizon_hours=3, tolerance_hours=2):
    df = df.copy()
    df["hour_ts"] = df["observed_at"].dt.floor("h")
    df = (df.sort_values(["city", "observed_at"])
            .drop_duplicates(["city", "hour_ts"], keep="last")
            .sort_values(["city", "hour_ts"]).reset_index(drop=True))
    df["row_id"] = df.index

    def nearest_temp_at(offset_hours):
        src = df[["city", "hour_ts", "temp_celsius"]].sort_values("hour_ts")
        want = df[["row_id", "city", "hour_ts"]].copy()
        want["want_ts"] = want["hour_ts"] + pd.Timedelta(hours=offset_hours)
        want = want.sort_values("want_ts")
        merged = pd.merge_asof(
            want, src, left_on="want_ts", right_on="hour_ts",
            by="city", direction="nearest",
            tolerance=pd.Timedelta(hours=tolerance_hours), suffixes=("", "_src"),
        )
        merged = merged.set_index("row_id").reindex(df["row_id"])
        return merged["temp_celsius"].to_numpy()

    df["lag_1h"] = nearest_temp_at(-1)
    df["target_temp"] = nearest_temp_at(horizon_hours)

    df = df.sort_values(["city", "hour_ts"]).reset_index(drop=True)
    df["roll_mean_6h"] = float("nan")
    for city, idx in df.groupby("city").groups.items():
        sub = df.loc[idx].set_index("hour_ts")["temp_celsius"]
        df.loc[idx, "roll_mean_6h"] = sub.rolling("6h").mean().to_numpy()

    df["hour_of_day"] = df["hour_ts"].dt.hour
    df["day_of_week"] = df["hour_ts"].dt.dayofweek
    df["city_code"] = df["city"].astype("category").cat.codes

    feature_cols = [
        "temp_celsius", "humidity_pct", "pressure_hpa", "wind_speed_ms", "cloudiness_pct",
        "lag_1h", "roll_mean_6h", "hour_of_day", "day_of_week", "city_code",
    ]
    return df, feature_cols