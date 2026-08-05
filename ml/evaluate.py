"""
Honest evaluation: chronological splits and a naive-baseline skill gate.

WHY THIS FILE EXISTS
--------------------
Every original train.py used:

    train_test_split(df, test_size=0.25, random_state=42)

A uniform random shuffle over a time series puts 3 p.m. in the training set and
2 p.m. of the same day in the test set. The model gets to interpolate between
neighbouring hours it has already seen, so the reported error is not an estimate
of forecasting performance at all. Measured on this database, the shuffle
understated error by 59% for AQI and 25% for temperature.

Worse, nothing compared the model to the obvious alternative: "tomorrow will be
like today". Re-run honestly, three of the four regressors lost to that baseline
— i.e. they were worse than making no model at all, while printing numbers that
looked good.

So this module enforces two things:
  * split by TIME, never at random;
  * always report skill against a named naive baseline, and treat losing to it
    as a failure rather than a footnote.
"""

from __future__ import annotations

from dataclasses import dataclass, asdict

import numpy as np
import pandas as pd


@dataclass
class Result:
    """Outcome of one training run — also the row written to model_registry."""

    metric_name: str
    metric_value: float
    baseline_name: str
    baseline_value: float
    rows_trained: int
    rows_tested: int
    train_window: str

    @property
    def skill(self) -> float:
        """1 - model/baseline. >0 beats the baseline, <0 is worse than doing nothing.

        For error metrics (MAE) lower is better, so the ratio is model/baseline.
        For score metrics (ROC AUC) higher is better, so it is inverted below.
        """
        if self.baseline_value in (0, None) or np.isnan(self.baseline_value):
            return float("nan")
        if self.metric_name == "roc_auc":
            # AUC 0.5 is chance; express skill as the fraction of the available
            # headroom above chance that the model actually captured.
            headroom = 1.0 - self.baseline_value
            return (self.metric_value - self.baseline_value) / headroom if headroom else float("nan")
        return 1.0 - (self.metric_value / self.baseline_value)

    @property
    def beats_baseline(self) -> bool:
        s = self.skill
        return bool(s == s and s > 0)  # NaN-safe

    def as_row(self) -> dict:
        d = asdict(self)
        d["skill"] = self.skill
        d["beats_baseline"] = self.beats_baseline
        return d

    def render(self, unit: str = "") -> str:
        verdict = "PASS — beats baseline" if self.beats_baseline else "FAIL — worse than baseline"
        arrow = "better" if self.beats_baseline else "WORSE"
        return (
            f"  rows: train={self.rows_trained}  test={self.rows_tested}\n"
            f"  window: {self.train_window}\n"
            f"  {self.metric_name:<10} model    = {self.metric_value:9.4f}{unit}\n"
            f"  {'':<10} baseline = {self.baseline_value:9.4f}{unit}  ({self.baseline_name})\n"
            f"  skill      = {self.skill:+.1%}  ({arrow} than baseline)\n"
            f"  {verdict}"
        )


def chronological_split(df: pd.DataFrame, time_col: str = "_ts", test_frac: float = 0.25):
    """Split on a single global time cutoff — the test set is strictly later.

    Cutting on a timestamp rather than a row index keeps whole cities together at
    the boundary: with 10 cities interleaved, an index cut would put the same hour
    on both sides of the split for different cities.
    """
    if not 0 < test_frac < 1:
        raise ValueError("test_frac must be in (0, 1)")

    ordered = df.sort_values(time_col)
    cutoff = ordered[time_col].quantile(1 - test_frac)
    train = ordered[ordered[time_col] <= cutoff]
    test = ordered[ordered[time_col] > cutoff]

    # Quantile ties (many rows sharing one timestamp) can empty one side.
    if len(test) == 0 or len(train) == 0:
        cut = int(len(ordered) * (1 - test_frac))
        train, test = ordered.iloc[:cut], ordered.iloc[cut:]
    return train, test


def window_str(df: pd.DataFrame, time_col: str = "_ts") -> str:
    lo, hi = df[time_col].min(), df[time_col].max()
    return f"{pd.Timestamp(lo):%Y-%m-%d %H:%M} .. {pd.Timestamp(hi):%Y-%m-%d %H:%M}"


def regression_result(
    y_true, y_pred, y_baseline, baseline_name: str, n_train: int, train_window: str
) -> Result:
    from sklearn.metrics import mean_absolute_error

    return Result(
        metric_name="mae",
        metric_value=float(mean_absolute_error(y_true, y_pred)),
        baseline_name=baseline_name,
        baseline_value=float(mean_absolute_error(y_true, y_baseline)),
        rows_trained=int(n_train),
        rows_tested=int(len(y_true)),
        train_window=train_window,
    )


def classification_result(
    y_true, y_prob, baseline_name: str, n_train: int, train_window: str
) -> Result:
    """ROC AUC against a 0.5 (coin-flip) reference.

    AUC is used rather than accuracy because rain is rare here (~4% of hours); a
    model that always says "no rain" scores 96% accuracy and is useless.
    """
    from sklearn.metrics import roc_auc_score

    y_true = np.asarray(y_true).astype(int)
    if len(np.unique(y_true)) < 2:
        auc = float("nan")  # one class only in the holdout — AUC undefined
    else:
        auc = float(roc_auc_score(y_true, y_prob))

    return Result(
        metric_name="roc_auc",
        metric_value=auc,
        baseline_name=baseline_name,
        baseline_value=0.5,
        rows_trained=int(n_train),
        rows_tested=int(len(y_true)),
        train_window=train_window,
    )
