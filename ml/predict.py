"""
Score the trained models and write predictions into ml_predictions.*.

    python ml/predict.py                    # all six
    python ml/predict.py --model aqi        # just one
    python ml/predict.py --anomaly-days 7   # rescore a week of pollution history

Forecast pipelines score the most recent usable observation per city. The anomaly
pipeline scores a trailing window of observations instead of only the newest row —
an anomaly log that holds one row per city cannot answer "when did Skopje spike?",
which is the whole point of keeping it.

Writes go through execute_values (one round trip per table) and upsert on the
tables' natural keys, so re-running — or an Airflow retry — is idempotent.
"""

from __future__ import annotations

import argparse
import sys

import numpy as np
import pandas as pd

import common
from pipelines import PIPELINES, Pipeline


def _features(pipe: Pipeline, bundle: dict) -> pd.DataFrame:
    """Rebuild features and apply the TRAINING-TIME city encoding."""
    raw = common.read_sql(pipe.query, parse_dates=[pipe.time_col])
    if raw.empty:
        raise LookupError(f"[{pipe.name}] source query returned no rows")

    df, _ = pipe.build(raw)
    df["city_code"] = common.apply_city_codes(df["city"], bundle["city_codes"])

    unseen = sorted(set(df["city"]) - set(bundle["city_codes"]))
    if unseen:
        print(f"  note: {len(unseen)} city(ies) not seen in training ({', '.join(unseen)}) "
              f"— encoded as -1; retrain to include them")
    return df


def _write(table: str, columns: list[str], rows: list[tuple], conflict: list[str],
           update: list[str], ts_col: str = "predicted_for") -> int:
    """Upsert rows, resolving the star keys from dim_city in the same statement.

    `city_key` is JOINED IN, never computed here. dbt builds it with
    dbt_utils.generate_surrogate_key(['city']); reproducing that hash in Python
    would silently couple this file to the package's null-placeholder and
    separator conventions and drift on an upgrade. The dimension is the authority,
    so a row can't be written with a fabricated key. `date_key` is YYYYMMDD::int —
    a format, not a hash — so it is derived inline from the row's own timestamp.

    The join is deliberately INNER: dim_city is derived from the same warehouse
    that feeds these models, so a city missing from it is a real anomaly, not a
    routine case, and must not pass silently. Hence the rowcount check below.
    """
    from psycopg2.extras import execute_values

    if not rows:
        return 0

    sets = ", ".join(f"{c} = excluded.{c}" for c in update)
    insert_cols = ["city_key", "date_key"] + columns
    select_cols = ", ".join(f"v.{c}" for c in columns)
    sql = (
        f"insert into ml_predictions.{table} ({', '.join(insert_cols)}) "
        f"select d.city_key, to_char(v.{ts_col}, 'YYYYMMDD')::int, {select_cols} "
        f"from (values %s) as v ({', '.join(columns)}) "
        f"join marts.dim_city d on d.city = v.city "
        f"on conflict ({', '.join(conflict)}) do update set {sets}, "
        f"city_key = excluded.city_key, date_key = excluded.date_key, scored_at = now()"
    )

    with common.get_conn() as conn:
        with conn.cursor() as cur:
            execute_values(cur, sql, rows)
            written = cur.rowcount
        conn.commit()

    # Report what was actually written, not what was offered — with an inner join
    # those can differ, and a silently shrinking count is how bad data hides.
    if written < len(rows):
        offered = {r[0] for r in rows}
        with common.get_conn() as conn, conn.cursor() as cur:
            cur.execute("select city from marts.dim_city")
            known = {r[0] for r in cur.fetchall()}
        missing = sorted(offered - known)
        print(f"  WARNING: {len(rows) - written} row(s) dropped — not in marts.dim_city: "
              f"{', '.join(missing) or '(unknown)'}")
    return written


def predict_forecast(pipe: Pipeline, bundle: dict) -> int:
    """One prediction per city, from that city's newest fully-featured observation."""
    df = _features(pipe, bundle)
    cols = bundle["feature_cols"]

    # Require only the anchor observation, not every optional lag — same reasoning
    # as in train.py. Otherwise a city whose 24h-ago reading is missing (routine,
    # given the coverage gaps) would silently get no forecast at all.
    required = pipe.predict_required if pipe.handles_nan else cols
    latest = (
        df.dropna(subset=required)
        .sort_values("_ts")
        .groupby("city", as_index=False)
        .tail(1)
        .copy()
    )
    if latest.empty:
        print("  no city has a complete feature row yet — nothing to score")
        return 0

    model = bundle["model"]
    if pipe.task == "classification":
        prob = model.predict_proba(latest[cols])[:, 1]
        latest["_value"] = prob
        latest["_flag"] = prob >= 0.5
    else:
        pred = model.predict(latest[cols])
        if bundle.get("predict_delta"):
            # The model was trained on the change from the current value, so add
            # that value back to recover the level (see Pipeline.predict_delta).
            pred = pred + latest[bundle["baseline_col"]].to_numpy()
        latest["_value"] = pred

    latest["_for"] = latest["_ts"] + pipe.horizon_delta

    # city_score's predicted_for column is a DATE, the rest are timestamps.
    as_date = pipe.horizon_col == "horizon_days"

    if pipe.task == "classification":
        columns = ["city", "predicted_for", pipe.horizon_col, pipe.value_col,
                   "predicted_rain", "model_version"]
        rows = [
            (r["city"], r["_for"].to_pydatetime(), pipe.horizon_value,
             float(r["_value"]), bool(r["_flag"]), pipe.model_version)
            for _, r in latest.iterrows()
        ]
        update = [pipe.value_col, "predicted_rain", "model_version"]
    else:
        columns = ["city", "predicted_for", pipe.horizon_col, pipe.value_col,
                   "model_version"]
        rows = [
            (r["city"],
             r["_for"].date() if as_date else r["_for"].to_pydatetime(),
             pipe.horizon_value, float(r["_value"]), pipe.model_version)
            for _, r in latest.iterrows()
        ]
        update = [pipe.value_col, "model_version"]

    n = _write(pipe.table, columns, rows,
               conflict=["city", "predicted_for", pipe.horizon_col], update=update)
    lo, hi = latest["_value"].min(), latest["_value"].max()
    print(f"  wrote {n} predictions for {pipe.horizon} ahead "
          f"(range {lo:.3f} .. {hi:.3f}{pipe.unit})")
    return n


def predict_anomaly(pipe: Pipeline, bundle: dict, days: int) -> int:
    """Score a trailing window so the anomaly table is a usable history."""
    df = _features(pipe, bundle)
    cols = bundle["feature_cols"]

    recent = df.dropna(subset=cols).copy()
    if recent.empty:
        print("  no fully-featured rows to score")
        return 0

    cutoff = recent["_ts"].max() - pd.Timedelta(days=days)
    recent = recent[recent["_ts"] >= cutoff]

    model = bundle["model"]
    recent["_score"] = model.decision_function(recent[cols])
    recent["_flag"] = model.predict(recent[cols]) == -1

    rows = [
        (r["city"], r["_ts"].to_pydatetime(),
         None if pd.isna(r["pm2_5_ug_m3"]) else float(r["pm2_5_ug_m3"]),
         None if pd.isna(r["pm10_ug_m3"]) else float(r["pm10_ug_m3"]),
         None if pd.isna(r["aqi"]) else int(r["aqi"]),
         float(r["_score"]), bool(r["_flag"]), pipe.model_version)
        for _, r in recent.iterrows()
    ]
    n = _write(
        pipe.table,
        ["city", "observed_at", "pm2_5_ug_m3", "pm10_ug_m3", "aqi",
         "anomaly_score", "is_anomaly", "model_version"],
        rows,
        conflict=["city", "observed_at"],
        update=["pm2_5_ug_m3", "pm10_ug_m3", "aqi", "anomaly_score",
                "is_anomaly", "model_version"],
        ts_col="observed_at",  # measured, not forecast — no predicted_for column
    )
    flagged = int(recent["_flag"].sum())
    print(f"  scored {n} observations over {days}d; {flagged} flagged as anomalies")
    if flagged:
        worst = recent.nsmallest(min(3, flagged), "_score")
        for _, r in worst.iterrows():
            print(f"    {r['city']:<12} {r['_ts']:%Y-%m-%d %H:%M}  "
                  f"pm2.5={r['pm2_5_ug_m3']:.1f}  score={r['_score']:.3f}")
    return n


def predict_one(name: str, anomaly_days: int) -> int:
    pipe = PIPELINES[name]
    print(f"\n{'='*72}\n{name}  ({pipe.horizon})\n{'='*72}")

    bundle = common.load_bundle(name)
    if bundle.get("model_version") != pipe.model_version:
        print(f"  note: saved model is {bundle.get('model_version')}, "
              f"config expects {pipe.model_version} — consider retraining")

    if pipe.task == "anomaly":
        return predict_anomaly(pipe, bundle, anomaly_days)
    return predict_forecast(pipe, bundle)


def main_programmatic(model: str | None = None, anomaly_days: int = 3) -> int:
    """Importable entry point — same behaviour as the CLI, minus argv parsing.

    Returns the NUMBER OF ROWS WRITTEN, not an exit code, so a caller can decide
    for itself whether zero rows is a failure.
    """
    common.load_env()
    common.ensure_schema()

    names = [model] if model else list(PIPELINES)
    total, missing, empty = 0, [], []

    for name in names:
        try:
            total += predict_one(name, anomaly_days)
        except FileNotFoundError as exc:
            print(f"  SKIPPED: {exc}")
            missing.append(name)
        except LookupError as exc:
            print(f"  SKIPPED: {exc}")
            empty.append(name)

    print(f"\n{'='*72}\nwrote {total} rows into ml_predictions.*")
    if missing:
        print(f"  untrained: {', '.join(missing)}  (run ml/train.py)")
    if empty:
        print(f"  no source data: {', '.join(empty)}")
    return total


def main() -> int:
    ap = argparse.ArgumentParser(description="Score the smart-city ML pipelines")
    ap.add_argument("--model", choices=sorted(PIPELINES), help="one pipeline (default: all)")
    ap.add_argument("--anomaly-days", type=int, default=3,
                    help="trailing window of pollution rows to rescore (default 3)")
    args = ap.parse_args()

    total = main_programmatic(model=args.model, anomaly_days=args.anomaly_days)

    # Only a total failure is worth a non-zero exit; individual untrained models
    # are reported above and are recoverable by running train.py.
    return 1 if total == 0 else 0


if __name__ == "__main__":
    sys.exit(main())
