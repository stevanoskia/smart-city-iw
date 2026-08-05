"""
Train one or all ML pipelines.

    python ml/train.py                      # all six
    python ml/train.py --model aqi          # just one
    python ml/train.py --list

Every run:
  * builds features with strictly-past lags and honestly-future targets,
  * splits CHRONOLOGICALLY (never at random),
  * scores against a named naive baseline,
  * writes the outcome to ml_predictions.model_registry,
  * saves the model to ml/_models/<name>.joblib (gitignored build artifact).

Exit code is non-zero if any model fails to beat its baseline. That is deliberate:
a forecast worse than "assume no change" should not pass silently, which is exactly
what happened before — three of the four original models lost to persistence while
reporting good-looking numbers from a shuffled split.
Use --allow-worse-than-baseline to save anyway (still recorded as a FAIL).
"""

from __future__ import annotations

import argparse
import sys

import numpy as np
import pandas as pd

import common
import evaluate as ev
from pipelines import PIPELINES, Pipeline


def _prepare(pipe: Pipeline) -> tuple[pd.DataFrame, list[str], dict[str, int]]:
    """Load, build features, encode cities, drop unusable rows."""
    raw = common.read_sql(pipe.query, parse_dates=[pipe.time_col])
    if raw.empty:
        raise LookupError(f"[{pipe.name}] source query returned no rows")

    city_codes = common.fit_city_codes(raw["city"])
    df, cols = pipe.build(raw)
    df["city_code"] = common.apply_city_codes(df["city"], city_codes)

    # Drop only what genuinely makes a row unusable. For the XGBoost pipelines
    # that is the target and the baseline column (needed to score skill); missing
    # LAGS are left as NaN on purpose, because XGBoost learns a default split
    # direction for them. Requiring every lag to be present discarded ~75% of the
    # rows on this dataset, which is a real cost when a city has ~30 days of history.
    if pipe.handles_nan:
        needed = [c for c in (pipe.target_col, pipe.baseline_col) if c]
    else:
        needed = list(cols) + ([pipe.target_col] if pipe.target_col else [])

    before = len(df)
    df = df.dropna(subset=needed) if needed else df
    kept_note = "target/baseline" if pipe.handles_nan else "features/target"
    print(f"  rows: {before} built -> {len(df)} usable "
          f"({before - len(df)} dropped for missing {kept_note})")
    return df, cols, city_codes


def _register(pipe: Pipeline, result: ev.Result | None, extra: str = "") -> None:
    """Append the training outcome to ml_predictions.model_registry."""
    if result is None:
        return
    row = result.as_row()
    notes = "; ".join(x for x in (pipe.notes, extra) if x)
    with common.get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                insert into ml_predictions.model_registry
                    (model_name, model_version, horizon, rows_trained, rows_tested,
                     metric_name, metric_value, baseline_name, baseline_value,
                     skill, beats_baseline, train_window, notes)
                values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                on conflict (model_name, trained_at) do nothing
                """,
                (
                    pipe.name, pipe.model_version, pipe.horizon,
                    row["rows_trained"], row["rows_tested"],
                    row["metric_name"],
                    None if np.isnan(row["metric_value"]) else row["metric_value"],
                    row["baseline_name"],
                    None if np.isnan(row["baseline_value"]) else row["baseline_value"],
                    None if np.isnan(row["skill"]) else row["skill"],
                    row["beats_baseline"], row["train_window"], notes,
                ),
            )
        conn.commit()


def train_regression(pipe: Pipeline, df, cols, city_codes) -> tuple[dict, ev.Result]:
    from xgboost import XGBRegressor

    train, test = ev.chronological_split(df)
    if len(test) < 10 or len(train) < 30:
        raise LookupError(
            f"[{pipe.name}] not enough history: train={len(train)} test={len(test)}"
        )

    def fit_target(frame):
        """What the regressor learns: either the level or the change from now."""
        if pipe.predict_delta:
            return frame[pipe.target_col] - frame[pipe.baseline_col]
        return frame[pipe.target_col]

    model = XGBRegressor(**pipe.params)
    model.fit(train[cols], fit_target(train))

    pred = model.predict(test[cols])
    if pipe.predict_delta:
        pred = pred + test[pipe.baseline_col].to_numpy()

    result = ev.regression_result(
        y_true=test[pipe.target_col],
        y_pred=pred,
        y_baseline=test[pipe.baseline_col],
        baseline_name=pipe.baseline_name,
        n_train=len(train),
        train_window=ev.window_str(train),
    )

    # Refit on everything once evaluated, so the deployed model has seen the most
    # recent days too. The metrics above still come from the held-out period only.
    final = XGBRegressor(**pipe.params)
    final.fit(df[cols], fit_target(df))

    bundle = {
        "model": final, "feature_cols": cols, "city_codes": city_codes,
        "pipeline": pipe.name, "model_version": pipe.model_version,
        "task": pipe.task, "metrics": result.as_row(),
        "predict_delta": pipe.predict_delta,
        "baseline_col": pipe.baseline_col,
    }
    return bundle, result


def train_classification(pipe: Pipeline, df, cols, city_codes) -> tuple[dict, ev.Result]:
    from xgboost import XGBClassifier

    train, test = ev.chronological_split(df)
    y_train = train[pipe.target_col].astype(int)
    if y_train.nunique() < 2:
        raise LookupError(
            f"[{pipe.name}] training window contains only one class "
            f"({y_train.iloc[0]}) — cannot fit a classifier"
        )
    if len(test) < 10:
        raise LookupError(f"[{pipe.name}] holdout too small: {len(test)} rows")

    # Rain is rare; without rebalancing the model just predicts "dry" everywhere.
    pos = max(int(y_train.sum()), 1)
    params = dict(pipe.params)
    params["scale_pos_weight"] = (len(y_train) - pos) / pos

    model = XGBClassifier(**params)
    model.fit(train[cols], y_train)

    y_test = test[pipe.target_col].astype(int)
    prob = model.predict_proba(test[cols])[:, 1]
    result = ev.classification_result(
        y_true=y_test, y_prob=prob, baseline_name=pipe.baseline_name,
        n_train=len(train), train_window=ev.window_str(train),
    )

    final = XGBClassifier(**params)
    final.fit(df[cols], df[pipe.target_col].astype(int))

    bundle = {
        "model": final, "feature_cols": cols, "city_codes": city_codes,
        "pipeline": pipe.name, "model_version": pipe.model_version,
        "task": pipe.task, "metrics": result.as_row(),
        "positive_rate": float(df[pipe.target_col].mean()),
    }
    return bundle, result


def train_anomaly(pipe: Pipeline, df, cols, city_codes) -> tuple[dict, None]:
    from sklearn.ensemble import IsolationForest

    model = IsolationForest(**pipe.params)
    model.fit(df[cols])
    flagged = int((model.predict(df[cols]) == -1).sum())
    print(f"  fitted on {len(df)} rows; {flagged} flagged ({flagged/len(df):.1%})")

    bundle = {
        "model": model, "feature_cols": cols, "city_codes": city_codes,
        "pipeline": pipe.name, "model_version": pipe.model_version,
        "task": pipe.task,
        "metrics": {"rows_trained": len(df), "flagged": flagged},
    }
    return bundle, None


def train_one(name: str) -> bool:
    """Train a single pipeline. Returns True if it beat its baseline."""
    pipe = PIPELINES[name]
    print(f"\n{'='*72}\n{name}  ({pipe.horizon})\n{'='*72}")

    df, cols, city_codes = _prepare(pipe)

    if pipe.task == "regression":
        bundle, result = train_regression(pipe, df, cols, city_codes)
    elif pipe.task == "classification":
        bundle, result = train_classification(pipe, df, cols, city_codes)
    else:
        bundle, result = train_anomaly(pipe, df, cols, city_codes)

    path = common.save_bundle(name, bundle)
    if result is not None:
        print(result.render(pipe.unit))
        _register(pipe, result)
    print(f"  saved -> {path}")

    return True if result is None else result.beats_baseline


def main_programmatic(
    model: str | None = None, allow_worse_than_baseline: bool = False
) -> int:
    """Importable entry point — same behaviour as the CLI, minus argv parsing.

    Returns the would-be exit code. The Airflow DAG shells out to this file
    instead of importing it (the pipelines live in their own venv), but keeping
    logic separate from argument parsing makes the module testable.
    """
    common.load_env()
    common.ensure_schema()

    names = [model] if model else list(PIPELINES)
    passed, failed, errored = [], [], []

    for name in names:
        try:
            (passed if train_one(name) else failed).append(name)
        except LookupError as exc:
            # Not enough data yet is a real condition on a laptop-hosted pipeline,
            # not a crash — report it and keep training the others.
            print(f"  SKIPPED: {exc}")
            errored.append(name)

    print(f"\n{'='*72}\nSUMMARY\n{'='*72}")
    if passed:
        print(f"  beat baseline : {', '.join(passed)}")
    if failed:
        print(f"  WORSE than baseline: {', '.join(failed)}")
    if errored:
        print(f"  skipped (insufficient data): {', '.join(errored)}")

    if failed and not allow_worse_than_baseline:
        print("\n  Models worse than their naive baseline should not be shipped.")
        print("  Re-run with --allow-worse-than-baseline to accept them anyway.")
        return 1
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description="Train the smart-city ML pipelines")
    ap.add_argument("--model", choices=sorted(PIPELINES), help="one pipeline (default: all)")
    ap.add_argument("--list", action="store_true", help="list pipelines and exit")
    ap.add_argument(
        "--allow-worse-than-baseline", action="store_true",
        help="exit 0 even when a model loses to its naive baseline",
    )
    args = ap.parse_args()

    if args.list:
        for n, p in sorted(PIPELINES.items()):
            print(f"  {n:<12} {p.task:<15} {p.horizon:>4}   -> ml_predictions.{p.table}")
        return 0

    return main_programmatic(
        model=args.model,
        allow_worse_than_baseline=args.allow_worse_than_baseline,
    )


if __name__ == "__main__":
    sys.exit(main())
