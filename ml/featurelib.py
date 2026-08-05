"""
Leak-free lag / target construction.

WHY THIS FILE EXISTS
--------------------
The original pipelines built both their lag features and their targets with a
single helper that did:

    pd.merge_asof(..., direction="nearest", tolerance=pd.Timedelta(hours=N))

`direction="nearest"` is the bug. It matches the closest row on EITHER side of
the wanted timestamp, which breaks two different ways:

  1. LAG FEATURES LEAK THE FUTURE. Asking for t-1h and letting pandas match the
     nearest row within ±3h means a "lag" can be sourced from t+2h. Measured on
     this database: 28% of AQI rows and 26% of temperature rows had lag_1h equal
     to the current value, i.e. the lag was matched to the row itself or later.

  2. TARGETS COLLAPSE ONTO THE PRESENT. Asking for t+1h with a ±1h tolerance
     lets pandas match the row at t itself — distance exactly 1h, inside the
     tolerance. The model is then trained to predict the number it was just
     handed. Measured: 48% of traffic rows and 38% of city-score rows had
     target == current value. That is what produced the flattering MAEs.

This module fixes both by construction:

  * lag_value()    → direction="backward", so a lag is ALWAYS strictly in the past.
  * future_value() → returns the matched timestamp too, and the caller drops any
                     row whose match is not genuinely far enough ahead.

The hourly-coverage constraint documented in CLAUDE.md (hours 01–05 and 16–19
have zero rows, ever) is exactly why tolerances are needed at all — and exactly
why they must not be allowed to silently degenerate into self-matches.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

ROW_ID = "_row_id"


def prepare_panel(df: pd.DataFrame, time_col: str, freq: str = "h") -> pd.DataFrame:
    """One row per (city, time bucket), newest reading wins, stable row ids.

    Two syncs inside one clock hour must collapse to a single row or the lag
    joins see duplicate timestamps.
    """
    out = df.copy()
    # Force a single datetime resolution. A DATE column arrives as datetime64[s]
    # while adding a Timedelta yields datetime64[us], and merge_asof refuses to
    # join keys whose resolutions differ.
    ts = pd.to_datetime(out[time_col]).astype("datetime64[ns]")
    out["_ts"] = ts.dt.floor(freq) if freq else ts
    out = (
        out.sort_values(["city", time_col])
        .drop_duplicates(["city", "_ts"], keep="last")
        .sort_values(["city", "_ts"])
        .reset_index(drop=True)
    )
    out[ROW_ID] = np.arange(len(out))
    return out


def _asof(
    df: pd.DataFrame,
    value_col: str,
    offset: pd.Timedelta,
    tolerance: pd.Timedelta,
    direction: str,
) -> tuple[np.ndarray, np.ndarray]:
    """Value of `value_col` at (_ts + offset), plus the timestamp actually matched.

    Rows whose value is null are removed from the SOURCE side first: otherwise a
    match can land on a null and report "no data" when a perfectly good earlier
    reading was available.
    """
    src = (
        df.loc[df[value_col].notna(), ["city", "_ts", value_col]]
        .rename(columns={"_ts": "_matched_ts"})
        .sort_values("_matched_ts")
    )
    want = df[[ROW_ID, "city", "_ts"]].copy()
    want["_want_ts"] = want["_ts"] + offset
    want = want.sort_values("_want_ts")

    if src.empty:
        nan = np.full(len(df), np.nan)
        return nan, np.full(len(df), np.datetime64("NaT"))

    merged = pd.merge_asof(
        want,
        src,
        left_on="_want_ts",
        right_on="_matched_ts",
        by="city",
        direction=direction,
        tolerance=tolerance,
    )
    merged = merged.set_index(ROW_ID).reindex(df[ROW_ID])
    return merged[value_col].to_numpy(), merged["_matched_ts"].to_numpy()


def lag_value(
    df: pd.DataFrame,
    value_col: str,
    hours: float,
    tolerance_hours: float,
) -> np.ndarray:
    """Value ~`hours` in the PAST. Never looks forward.

    direction="backward" takes the most recent row at or before (t - hours), so
    the result is always genuinely historical, which is what a lag feature has to
    be to be usable at prediction time.
    """
    if hours <= 0:
        raise ValueError("lag hours must be positive")
    vals, _ = _asof(
        df,
        value_col,
        offset=-pd.Timedelta(hours=hours),
        tolerance=pd.Timedelta(hours=tolerance_hours),
        direction="backward",
    )
    return vals


def future_value(
    df: pd.DataFrame,
    value_col: str,
    horizon_hours: float,
    tolerance_hours: float,
    min_gap_frac: float = 0.5,
) -> tuple[np.ndarray, np.ndarray]:
    """Value ~`horizon_hours` in the FUTURE, or NaN when no honest match exists.

    Returns (values, actual_gap_hours).

    A match only counts when it is at least `min_gap_frac` of the requested
    horizon ahead of the current row. Without that floor, a ±1h tolerance on a
    1h horizon happily matches the current row itself and the "forecast" becomes
    an identity function.
    """
    vals, matched = _asof(
        df,
        value_col,
        offset=pd.Timedelta(hours=horizon_hours),
        tolerance=pd.Timedelta(hours=tolerance_hours),
        direction="nearest",
    )
    gap_hours = (
        pd.Series(matched).to_numpy(dtype="datetime64[ns]")
        - df["_ts"].to_numpy(dtype="datetime64[ns]")
    ) / np.timedelta64(1, "h")

    too_close = ~(gap_hours >= horizon_hours * min_gap_frac)
    vals = vals.astype("float64").copy()
    vals[too_close] = np.nan
    gap_hours = gap_hours.astype("float64").copy()
    gap_hours[too_close] = np.nan
    return vals, gap_hours


def rolling_mean(df: pd.DataFrame, value_col: str, window: str) -> np.ndarray:
    """Per-city time-based rolling mean over a trailing window (includes current row)."""
    out = np.full(len(df), np.nan)
    for _, sub in df.groupby("city", sort=False):
        s = sub.set_index("_ts")[value_col].rolling(window).mean()
        out[sub[ROW_ID].to_numpy()] = s.to_numpy()
    return out


def rolling_baseline(
    df: pd.DataFrame, value_col: str, window: str
) -> tuple[np.ndarray, np.ndarray]:
    """Per-city trailing median and IQR — a robust 'normal for this city' band.

    Shifted by one row so the current observation is never part of the baseline it
    is being compared against; otherwise a big spike drags its own reference up and
    hides itself.
    """
    med = np.full(len(df), np.nan)
    iqr = np.full(len(df), np.nan)
    for _, sub in df.groupby("city", sort=False):
        s = sub.set_index("_ts")[value_col].shift(1)
        r = s.rolling(window, min_periods=3)
        idx = sub[ROW_ID].to_numpy()
        med[idx] = r.median().to_numpy()
        iqr[idx] = (r.quantile(0.75) - r.quantile(0.25)).to_numpy()
    return med, iqr


def add_calendar(df: pd.DataFrame) -> pd.DataFrame:
    """Hour-of-day / day-of-week, plus cyclical encodings.

    hour_of_day as a plain integer tells a tree that 23:00 and 00:00 are maximally
    far apart. The sin/cos pair restores the wraparound. Both are kept: the raw
    integer still helps where the split really is "before/after noon".
    """
    out = df.copy()
    out["hour_of_day"] = out["_ts"].dt.hour
    out["day_of_week"] = out["_ts"].dt.dayofweek
    out["is_weekend"] = (out["day_of_week"] >= 5).astype(int)
    out["hour_sin"] = np.sin(2 * np.pi * out["hour_of_day"] / 24)
    out["hour_cos"] = np.cos(2 * np.pi * out["hour_of_day"] / 24)
    return out
