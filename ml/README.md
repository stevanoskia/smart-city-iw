# ML Prediction Pipelines

Six models over the smart-city warehouse, written into the **`ml_predictions`**
schema and scheduled by the `smart_city_ml` Airflow DAG.

| # | Pipeline | Predicts | Horizon | Model | Output table |
|---|---|---|---|---|---|
| 1 | `aqi` | PM2.5 concentration | 24h | XGBoost regressor | `aqi_forecast` |
| 2 | `temperature` | Air temperature | 3h | XGBoost regressor | `temperature_forecast` |
| 3 | `traffic` | Congestion score | 1h | XGBoost regressor | `traffic_forecast` |
| 4 | `rain` | Probability of rain | 3h | XGBoost classifier | `rain_forecast` |
| 5 | `city_score` | Comfort index | 1 day | XGBoost regressor | `city_score_forecast` |
| 6 | `anomaly` | Pollution spikes | — | IsolationForest | `pollution_anomaly` |

---

## Quick start

```bash
# 1. Dedicated venv — NOT venv313, which holds the pinned dbt toolchain
python -m venv .venv-ml
.venv-ml/Scripts/pip install -r ml/requirements.txt

# 2. Train (creates the ml_predictions schema on first run)
.venv-ml/Scripts/python ml/train.py

# 3. Score
.venv-ml/Scripts/python ml/predict.py
```

Both read the database from `.env` (`POSTGRES_*`) on the host, or
`SMART_CITY_PG_*` inside the Airflow container — the same dual-env convention as
`ai/common.py`.

Useful flags:

```bash
python ml/train.py --list                       # what exists
python ml/train.py --model aqi                  # one pipeline
python ml/train.py --allow-worse-than-baseline  # exit 0 even on a failing model
python ml/predict.py --model rain
python ml/predict.py --anomaly-days 7           # rescore a week of pollution history
```

---

## Layout

```
ml/
├── README.md          ← this file
├── requirements.txt   ← keep in sync with airflow/Dockerfile's ml_venv
├── schema.sql         ← ml_predictions DDL (idempotent, applied automatically)
├── common.py          ← DB connection, env, model save/load, city encoding
├── featurelib.py      ← leak-free lag/target construction   ← the important one
├── evaluate.py        ← chronological split + naive-baseline skill gate
├── pipelines.py       ← the six pipelines, declared as data
├── train.py           ← CLI: fit, evaluate, register, save
├── predict.py         ← CLI: score and upsert
└── _models/           ← gitignored build artifacts (*.joblib)
```

One shared core with six declarative pipeline definitions, rather than a
`features.py`/`train.py`/`predict.py` triple per model. The earlier layout had
five near-identical copies of each, which is how one evaluation bug ended up in
all five at once and why the wrong default DB port appeared five times.

---

## The two rules this code exists to enforce

### 1. A feature may never see the future

`featurelib.lag_value()` uses `merge_asof(direction="backward")`, so a lag is
always sourced from at or before the requested past timestamp.

The natural-looking `direction="nearest"` breaks this: asking for `t-1h` with a
±3h tolerance happily matches a row at `t+2h`. That is not a lag, it is the
answer. Targets get the mirror-image treatment — `future_value()` returns the
timestamp it matched, and any match closer than half the requested horizon is
discarded, so a "1h-ahead forecast" can never be resolved to the current row.

### 2. Evaluation is chronological, and always against a baseline

`evaluate.chronological_split()` cuts on a timestamp: the test set is strictly
later than the training set. A random shuffle over a time series lets the model
interpolate between hours it has already seen, which measures nothing useful.

Every run is then scored against the obvious alternative — **persistence**
("nothing changes"), or a coin flip for the classifier — and the result is
written to `ml_predictions.model_registry`. `train.py` **exits non-zero** if a
model loses to its baseline, because a forecast worse than assuming no change
should not ship quietly.

---

## Current model skill

**These numbers move.** The DAG retrains daily, so every run re-scores on a fresh holdout and the
figures shift as history accumulates. Treat the table below as a dated snapshot and the database as
the source of truth:

```sql
select * from ml_predictions.model_health order by skill desc;   -- latest per model
select * from ml_predictions.model_registry order by trained_at; -- skill over time
```

Snapshot from the live warehouse, 2026-08-05 (~7 weeks of history, 10 cities):

| Pipeline | Metric | Model | Baseline | Skill | Verdict |
|---|---|---|---|---|---|
| `rain` | ROC AUC | 0.964 | 0.500 | **+92.7%** | ✅ strong |
| `temperature` | MAE | 1.49 °C | 2.59 °C | **+42.6%** | ✅ strong |
| `traffic` | MAE | 0.110 | 0.119 | **+7.4%** | ✅ modest |
| `aqi` | MAE | 4.05 µg/m³ | 3.17 µg/m³ | −27.6% | ❌ below baseline |
| `city_score` | MAE | 0.040 | 0.037 | −8.1% | ❌ below baseline |
| `anomaly` | — | ~3% flagged | — | — | unsupervised |

Which models pass has been stable across runs; only the magnitudes move.

**`aqi` and `city_score` genuinely lose to persistence, and that is reported
rather than hidden.** Both already model the *change* from the current value
instead of the level (`Pipeline.predict_delta`), which helps but does not close
the gap. The honest reading is that 24h-ahead PM2.5 and next-day comfort need
more history than seven gappy weeks — the fix is data, not hyperparameters, and
tuning until the holdout passes would just be overfitting it. Their predictions
are still written and labelled; treat them as weak.

---

## Constraints that shape these models

**Hourly coverage is partial** (see the constraint documented in `CLAUDE.md`).
Airflow only runs while the dev machine is on, so of 24 UTC hours:

- hours **01–05 and 16–19 have zero rows, ever** — 9 of 24; weather also has none at 15
- the bulk of observations sit in **07–14 UTC**

Consequences baked into the code:

- Lag and target lookups need tolerances, because the exact hour is often absent.
  Those tolerances are what made `direction="nearest"` so damaging.
- Rows with missing lags are **kept**, not dropped: XGBoost learns a default
  branch for missing values, and requiring every lag discarded ~75% of the data.
  IsolationForest cannot take NaN, so the anomaly pipeline drops instead
  (`Pipeline.handles_nan`).
- No model claims overnight or rush-hour behaviour — that data does not exist.

**Rain is rare** (~4% of observed hours). The classifier is scored on ROC AUC,
not accuracy: always predicting "dry" scores 96% accuracy and is useless. Training
applies `scale_pos_weight` so the model does not collapse onto the majority class.

**The anomaly model scores city-relative deviation**, not absolute pollution.
Features are robust z-scores against each city's own trailing 7-day median and
IQR, shifted one row so a spike is not part of the baseline it is measured
against. Feeding raw PM2.5 to IsolationForest — as the first version did — mostly
learns *which cities are dirty*: a permanently polluted city looks anomalous every
hour, while a real spike in a clean city never stands out. The brief asked for
spike detection, which is a within-city question.

---

## Scheduling

The `smart_city_ml` DAG (`airflow/dags/dag_smart_city_ml.py`) runs `@daily`:

```
train → predict → report
```

- **Its own DAG, not part of the hourly pipeline.** Horizons are 1h–1d and
  `city_score` is daily-grain; hourly runs would rewrite the same forecasts 24
  times a day off partial data.
- **Retrains every run.** Models are build artifacts, not source — the ~1,700-row
  training set fits in seconds, so retraining is cheaper than distributing
  `.joblib` files, and the models never drift from a stale snapshot. If training
  ever costs real time, split into a weekly train DAG and a daily predict DAG
  sharing a volume.
- **Runs in `/home/airflow/ml_venv`**, a separate virtualenv in the Airflow image,
  for the same reason dbt has one: airflow 2.9.3 pins pandas and numpy, and
  scikit-learn/xgboost want their own. The tasks shell out and the wrapper puts
  the tail of the child's output into the raised exception, so alert emails still
  say what broke.
- **A model losing to its baseline does not fail the DAG** (it passes
  `--allow-worse-than-baseline`). That condition is a property of the data, not an
  incident; daily emails about it would just train everyone to ignore alerts. The
  `report` task logs current skill so a real regression stays visible.

Requires the `../ml:/opt/airflow/ml` mount in `airflow/docker-compose.yml` and an
image rebuild for `ml_venv`:

```bash
cd airflow && docker compose build && docker compose up -d
```

---

## Output tables

All under `ml_predictions`, all upserted on their natural keys so re-runs and
Airflow retries are idempotent.

Two helper views:

- **`model_health`** — latest training result per model (the skill table above).
- **`latest_predictions`** — all five forecast tables unioned into one long feed
  (`model_name, city, predicted_for, predicted_value, unit, horizon`), convenient
  for a single Power BI import instead of five.

`scored_at` (when the prediction was made) is kept alongside `predicted_for`
(what it is about), which is what makes after-the-fact accuracy scoring possible —
the same split `marts.fct_forecast_accuracy` uses for OpenWeather's forecasts.

---

## Adding a pipeline

Add one `Pipeline(...)` entry to `PIPELINES` in `pipelines.py` and a `build_*`
function beside it. `train.py` and `predict.py` are generic and need no changes.
Add the output table to `schema.sql`.
