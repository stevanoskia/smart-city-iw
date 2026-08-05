"""
The six prediction pipelines, as data.

Each Pipeline bundles everything that differs between models — source query,
feature construction, estimator, output table — so train.py and predict.py stay
generic. The original layout repeated a near-identical train.py and predict.py in
five directories; a fix to the evaluation logic had to be made five times, which
is why the same leakage bug appears in all of them.

Covers the six ideas from the project brief:
    1. AQI / PM2.5     24h    XGBoost + lag features
    2. Temperature      3h    XGBoost + lag features
    3. Traffic          1h    XGBoost + lag features
    4. Rain probability 3h    XGBoost classifier      <- was missing entirely
    5. City score       1d    XGBoost regression
    6. Pollution anomaly  —   IsolationForest on city-relative deviations
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Callable

import numpy as np
import pandas as pd

import featurelib as fl


@dataclass
class Pipeline:
    name: str
    query: str
    time_col: str
    build: Callable[[pd.DataFrame], tuple[pd.DataFrame, list[str]]]
    target_col: str | None
    baseline_col: str | None          # naive "no change" prediction
    baseline_name: str
    task: str                          # 'regression' | 'classification' | 'anomaly'
    horizon: str
    unit: str
    table: str                         # ml_predictions output table
    value_col: str                     # prediction column in that table
    horizon_col: str                   # 'horizon_hours' | 'horizon_days'
    horizon_value: int
    horizon_delta: pd.Timedelta
    model_version: str
    params: dict = field(default_factory=dict)
    freq: str = "h"
    notes: str = ""

    # XGBoost learns a default branch direction for missing values, so a row with
    # some lags unavailable is still usable training data. Given the coverage gaps
    # in this dataset (10 of 24 hours never observed), insisting every lag be
    # present threw away ~75% of the rows. sklearn's IsolationForest cannot take
    # NaN, so the anomaly pipeline sets this False and drops instead.
    handles_nan: bool = True
    # Learn the CHANGE from the current value instead of the level itself, then add
    # the current value back at prediction time. Where persistence is a strong
    # baseline (24h PM2.5, next-day comfort) predicting the level means re-deriving
    # "roughly today's number" from scratch and paying tree-quantisation error for
    # it; predicting the delta makes a weak model degrade TOWARDS persistence —
    # a model that outputs 0 is exactly persistence — instead of underperforming it.
    predict_delta: bool = False
    # Columns a row must have to be scoreable at all — the current observation the
    # forecast is anchored on, as opposed to its optional history.
    predict_required: list[str] = field(default_factory=list)


# ── 1. AQI / PM2.5, 24h ahead ────────────────────────────────────────────────

AQI_QUERY = """
select city, observed_at, aqi, pm2_5_ug_m3, pm10_ug_m3
from intermediate.int_city_hourly_pollution
where city is not null
order by city, observed_at
"""


def build_aqi(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    d = fl.prepare_panel(df, "observed_at")

    # Lags are strictly backward-looking (see featurelib for why that matters).
    d["lag_1h"] = fl.lag_value(d, "pm2_5_ug_m3", 1, tolerance_hours=2)
    d["lag_3h"] = fl.lag_value(d, "pm2_5_ug_m3", 3, tolerance_hours=3)
    d["lag_24h"] = fl.lag_value(d, "pm2_5_ug_m3", 24, tolerance_hours=4)
    d["roll_mean_6h"] = fl.rolling_mean(d, "pm2_5_ug_m3", "6h")
    d["roll_mean_24h"] = fl.rolling_mean(d, "pm2_5_ug_m3", "24h")

    # Change already under way — the single most useful signal for "where next".
    d["delta_1h"] = d["pm2_5_ug_m3"] - d["lag_1h"]
    d["delta_24h"] = d["pm2_5_ug_m3"] - d["lag_24h"]
    d["pm_ratio"] = d["pm2_5_ug_m3"] / d["pm10_ug_m3"].replace(0, np.nan)

    d = fl.add_calendar(d)
    d["target"], d["_gap"] = fl.future_value(d, "pm2_5_ug_m3", 24, tolerance_hours=4)

    cols = [
        "pm2_5_ug_m3", "pm10_ug_m3", "aqi",
        "lag_1h", "lag_3h", "lag_24h",
        "roll_mean_6h", "roll_mean_24h",
        "delta_1h", "delta_24h", "pm_ratio",
        "hour_of_day", "hour_sin", "hour_cos", "day_of_week", "is_weekend",
        "city_code",
    ]
    return d, cols


# ── 2. Temperature, 3h ahead ─────────────────────────────────────────────────

TEMP_QUERY = """
select city, observed_at, temp_celsius, feels_like_celsius, humidity_pct,
       pressure_hpa, wind_speed_ms, cloudiness_pct
from intermediate.int_city_hourly_weather
where city is not null
order by city, observed_at
"""


def build_temperature(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    d = fl.prepare_panel(df, "observed_at")

    d["lag_1h"] = fl.lag_value(d, "temp_celsius", 1, tolerance_hours=2)
    d["lag_3h"] = fl.lag_value(d, "temp_celsius", 3, tolerance_hours=3)
    d["lag_24h"] = fl.lag_value(d, "temp_celsius", 24, tolerance_hours=4)
    d["roll_mean_6h"] = fl.rolling_mean(d, "temp_celsius", "6h")
    d["delta_1h"] = d["temp_celsius"] - d["lag_1h"]

    # Same hour yesterday: the strongest single predictor for a daily cycle, and
    # it survives the coverage gaps better than a long chain of short lags.
    d["same_hour_yesterday"] = d["lag_24h"]

    d = fl.add_calendar(d)
    d["target"], d["_gap"] = fl.future_value(d, "temp_celsius", 3, tolerance_hours=2)

    cols = [
        "temp_celsius", "feels_like_celsius", "humidity_pct", "pressure_hpa",
        "wind_speed_ms", "cloudiness_pct",
        "lag_1h", "lag_3h", "same_hour_yesterday", "roll_mean_6h", "delta_1h",
        "hour_of_day", "hour_sin", "hour_cos", "day_of_week", "is_weekend",
        "city_code",
    ]
    return d, cols


# ── 3. Traffic congestion, 1h ahead ──────────────────────────────────────────

TRAFFIC_QUERY = """
select city, observed_at, congestion_score, current_speed_kmh, free_flow_speed_kmh
from intermediate.int_city_hourly_traffic_flow
where city is not null
order by city, observed_at
"""


def build_traffic(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    d = fl.prepare_panel(df, "observed_at")

    d["lag_1h"] = fl.lag_value(d, "congestion_score", 1, tolerance_hours=2)
    d["lag_2h"] = fl.lag_value(d, "congestion_score", 2, tolerance_hours=2)
    d["lag_24h"] = fl.lag_value(d, "congestion_score", 24, tolerance_hours=4)
    d["roll_mean_3h"] = fl.rolling_mean(d, "congestion_score", "3h")
    d["speed_ratio"] = d["current_speed_kmh"] / d["free_flow_speed_kmh"].replace(0, np.nan)
    d["delta_1h"] = d["congestion_score"] - d["lag_1h"]

    d = fl.add_calendar(d)
    # tolerance 1h (not 2h): with a 1h horizon a wider window is mostly matching
    # the next morning's first reading, which is not a 1-hour-ahead forecast.
    d["target"], d["_gap"] = fl.future_value(d, "congestion_score", 1, tolerance_hours=1)

    cols = [
        "congestion_score", "current_speed_kmh", "free_flow_speed_kmh", "speed_ratio",
        "lag_1h", "lag_2h", "lag_24h", "roll_mean_3h", "delta_1h",
        "hour_of_day", "hour_sin", "hour_cos", "day_of_week", "is_weekend",
        "city_code",
    ]
    return d, cols


# ── 4. Rain probability, 3h ahead (the pipeline the original PR never built) ──

RAIN_QUERY = """
select city, observed_at, temp_celsius, humidity_pct, pressure_hpa,
       wind_speed_ms, cloudiness_pct, rain_1h_mm, weather_main
from intermediate.int_city_hourly_weather
where city is not null
order by city, observed_at
"""


def build_rain(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    d = fl.prepare_panel(df, "observed_at")

    # "It is raining" = a positive rain reading OR a wet weather_main. rain_1h_mm
    # is null on dry hours rather than 0, so it cannot be tested on its own.
    d["is_raining"] = (
        d["rain_1h_mm"].fillna(0).gt(0)
        | d["weather_main"].isin(["Rain", "Drizzle", "Thunderstorm"])
    ).astype(float)

    d["lag_1h_humidity"] = fl.lag_value(d, "humidity_pct", 1, tolerance_hours=2)
    d["lag_3h_pressure"] = fl.lag_value(d, "pressure_hpa", 3, tolerance_hours=3)
    d["roll_mean_humidity_6h"] = fl.rolling_mean(d, "humidity_pct", "6h")
    d["roll_mean_cloud_6h"] = fl.rolling_mean(d, "cloudiness_pct", "6h")

    # Falling pressure is the classic precursor; the level alone says little.
    d["pressure_delta_3h"] = d["pressure_hpa"] - d["lag_3h_pressure"]
    d["humidity_delta_1h"] = d["humidity_pct"] - d["lag_1h_humidity"]

    d = fl.add_calendar(d)
    d["target"], d["_gap"] = fl.future_value(d, "is_raining", 3, tolerance_hours=2)

    cols = [
        "temp_celsius", "humidity_pct", "pressure_hpa", "wind_speed_ms", "cloudiness_pct",
        "roll_mean_humidity_6h", "roll_mean_cloud_6h",
        "pressure_delta_3h", "humidity_delta_1h", "is_raining",
        "hour_of_day", "hour_sin", "hour_cos", "day_of_week", "is_weekend",
        "city_code",
    ]
    return d, cols


# ── 5. City score (comfort index), 1 day ahead ───────────────────────────────

CITY_SCORE_QUERY = """
select city, date_utc, comfort_index, livability_score,
       norm_temp, norm_aqi, norm_traffic, rolling_7d_comfort, prior_7d_comfort,
       avg_temp_celsius, avg_aqi, avg_congestion_score, hours_poor_air
from marts.mart_city_daily
where city is not null
order by city, date_utc
"""


def build_city_score(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    d = df.copy()
    d["date_utc"] = pd.to_datetime(d["date_utc"])
    d = fl.prepare_panel(d, "date_utc", freq=None)

    d["lag_1d"] = fl.lag_value(d, "comfort_index", 24, tolerance_hours=36)
    d["lag_7d"] = fl.lag_value(d, "comfort_index", 24 * 7, tolerance_hours=48)
    d["delta_1d"] = d["comfort_index"] - d["lag_1d"]
    d["vs_7d_avg"] = d["comfort_index"] - d["rolling_7d_comfort"]

    d = fl.add_calendar(d)
    d["target"], d["_gap"] = fl.future_value(
        d, "comfort_index", 24, tolerance_hours=36
    )

    cols = [
        "comfort_index", "norm_temp", "norm_aqi", "norm_traffic",
        "rolling_7d_comfort", "prior_7d_comfort",
        "avg_temp_celsius", "avg_aqi", "hours_poor_air",
        "lag_1d", "lag_7d", "delta_1d", "vs_7d_avg",
        "day_of_week", "is_weekend", "city_code",
    ]
    return d, cols


# ── 6. Pollution anomaly ─────────────────────────────────────────────────────

ANOMALY_QUERY = AQI_QUERY


def build_anomaly(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """Features expressed RELATIVE to each city's own recent normal.

    The original fed raw pm2_5/pm10/aqi straight to IsolationForest, so the model
    mostly learned "which cities are dirty" — a permanently polluted city looks
    anomalous every hour, and a genuine spike in a clean city does not stand out
    against the global distribution. The brief asked for spike detection
    ("ненадејно зголемено загадување"), which is a within-city question.
    """
    d = fl.prepare_panel(df, "observed_at")

    med, iqr = fl.rolling_baseline(d, "pm2_5_ug_m3", "7d")
    d["pm25_median_7d"] = med
    # Guard the divisor: a city with a flat week has IQR 0 and every tiny wobble
    # would otherwise score as an infinite deviation.
    d["pm25_robust_z"] = (d["pm2_5_ug_m3"] - med) / np.maximum(iqr, 1.0)

    med10, iqr10 = fl.rolling_baseline(d, "pm10_ug_m3", "7d")
    d["pm10_robust_z"] = (d["pm10_ug_m3"] - med10) / np.maximum(iqr10, 1.0)

    d["lag_1h"] = fl.lag_value(d, "pm2_5_ug_m3", 1, tolerance_hours=2)
    d["jump_1h"] = d["pm2_5_ug_m3"] - d["lag_1h"]
    d["ratio_to_median"] = d["pm2_5_ug_m3"] / np.maximum(med, 0.1)
    d["pm_ratio"] = d["pm2_5_ug_m3"] / d["pm10_ug_m3"].replace(0, np.nan)

    d = fl.add_calendar(d)

    cols = [
        "pm25_robust_z", "pm10_robust_z", "jump_1h", "ratio_to_median", "pm_ratio",
        "hour_sin", "hour_cos",
    ]
    return d, cols


# ── Registry ─────────────────────────────────────────────────────────────────

XGB_REG = dict(
    n_estimators=300, max_depth=4, learning_rate=0.05,
    subsample=0.8, colsample_bytree=0.8, min_child_weight=5,
    reg_lambda=1.0, random_state=42, n_jobs=2,
)

PIPELINES: dict[str, Pipeline] = {
    "aqi": Pipeline(
        name="aqi", query=AQI_QUERY, time_col="observed_at", build=build_aqi,
        target_col="target", baseline_col="pm2_5_ug_m3",
        baseline_name="persistence (pm2_5 unchanged)",
        task="regression", horizon="24h", unit=" ug/m3",
        table="aqi_forecast", value_col="predicted_pm2_5",
        horizon_col="horizon_hours", horizon_value=24,
        horizon_delta=pd.Timedelta(hours=24),
        model_version="xgb_v2", params=XGB_REG,
        predict_required=["pm2_5_ug_m3"],
        predict_delta=True,
        notes="24h PM2.5; persistence is a strong baseline at this horizon",
    ),
    "temperature": Pipeline(
        name="temperature", query=TEMP_QUERY, time_col="observed_at",
        build=build_temperature,
        target_col="target", baseline_col="temp_celsius",
        baseline_name="persistence (temp unchanged)",
        task="regression", horizon="3h", unit=" C",
        table="temperature_forecast", value_col="predicted_temp",
        horizon_col="horizon_hours", horizon_value=3,
        horizon_delta=pd.Timedelta(hours=3),
        model_version="xgb_v2", params=XGB_REG,
        predict_required=["temp_celsius"],
    ),
    "traffic": Pipeline(
        name="traffic", query=TRAFFIC_QUERY, time_col="observed_at",
        build=build_traffic,
        target_col="target", baseline_col="congestion_score",
        baseline_name="persistence (congestion unchanged)",
        task="regression", horizon="1h", unit="",
        table="traffic_forecast", value_col="predicted_congestion",
        horizon_col="horizon_hours", horizon_value=1,
        horizon_delta=pd.Timedelta(hours=1),
        model_version="xgb_v2", params=XGB_REG,
        predict_required=["congestion_score"],
    ),
    "rain": Pipeline(
        name="rain", query=RAIN_QUERY, time_col="observed_at", build=build_rain,
        target_col="target", baseline_col=None,
        baseline_name="coin flip (AUC 0.5)",
        task="classification", horizon="3h", unit="",
        table="rain_forecast", value_col="rain_probability",
        horizon_col="horizon_hours", horizon_value=3,
        horizon_delta=pd.Timedelta(hours=3),
        model_version="xgb_clf_v1",
        params=dict(
            n_estimators=200, max_depth=3, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8, min_child_weight=5,
            random_state=42, n_jobs=2, eval_metric="logloss",
        ),
        predict_required=["humidity_pct", "pressure_hpa"],
        notes="rare positive class (~4% of hours) — AUC, not accuracy",
    ),
    "city_score": Pipeline(
        name="city_score", query=CITY_SCORE_QUERY, time_col="date_utc",
        build=build_city_score,
        target_col="target", baseline_col="comfort_index",
        baseline_name="persistence (today's comfort_index)",
        task="regression", horizon="1d", unit="",
        table="city_score_forecast", value_col="predicted_comfort_index",
        horizon_col="horizon_days", horizon_value=1,
        horizon_delta=pd.Timedelta(days=1),
        model_version="xgb_v2",
        params=dict(
            n_estimators=150, max_depth=3, learning_rate=0.05,
            subsample=0.8, colsample_bytree=0.8, min_child_weight=3,
            random_state=42, n_jobs=2,
        ),
        freq=None,
        predict_required=["comfort_index"],
        predict_delta=True,
        notes="~30 days per city — small sample, treat skill as provisional",
    ),
    "anomaly": Pipeline(
        name="anomaly", query=ANOMALY_QUERY, time_col="observed_at",
        build=build_anomaly,
        target_col=None, baseline_col=None, baseline_name="n/a (unsupervised)",
        task="anomaly", horizon="-", unit="",
        table="pollution_anomaly", value_col="anomaly_score",
        horizon_col="", horizon_value=0, horizon_delta=pd.Timedelta(0),
        model_version="iforest_v2",
        params=dict(n_estimators=300, contamination=0.03, random_state=42, n_jobs=2),
        handles_nan=False,
        notes="scores city-relative deviation, not absolute pollution level",
    ),
}
