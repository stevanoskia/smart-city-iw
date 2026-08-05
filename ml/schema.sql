-- ============================================================================
-- ml_predictions schema — output tables for the ML pipelines in ml/
--
-- Idempotent: safe to re-run. Mirrors the convention used by config/schema.sql.
--
-- Every prediction table is keyed on (city, what-is-being-predicted) so a re-run
-- of predict.py for the same slot UPDATES in place rather than appending a second
-- opinion — the pipelines are re-runnable and Airflow retries must not duplicate.
--
-- `scored_at` is when we made the prediction; `predicted_for` is the timestamp the
-- prediction is ABOUT. Keeping both is what makes after-the-fact accuracy scoring
-- possible (the same split marts.fct_forecast_accuracy makes for OpenWeather).
-- ============================================================================

create schema if not exists ml_predictions;

-- ── 1. AQI / PM2.5 forecast ─────────────────────────────────────────────────
create table if not exists ml_predictions.aqi_forecast (
    city             text        not null,
    predicted_for    timestamp   not null,
    horizon_hours    int         not null,
    predicted_pm2_5  double precision,
    model_version    text        not null,
    scored_at        timestamptz not null default now(),
    primary key (city, predicted_for, horizon_hours)
);

-- ── 2. Temperature forecast ─────────────────────────────────────────────────
create table if not exists ml_predictions.temperature_forecast (
    city             text        not null,
    predicted_for    timestamp   not null,
    horizon_hours    int         not null,
    predicted_temp   double precision,
    model_version    text        not null,
    scored_at        timestamptz not null default now(),
    primary key (city, predicted_for, horizon_hours)
);

-- ── 3. Traffic / congestion forecast ────────────────────────────────────────
create table if not exists ml_predictions.traffic_forecast (
    city                  text        not null,
    predicted_for         timestamp   not null,
    horizon_hours         int         not null,
    predicted_congestion  double precision,
    model_version         text        not null,
    scored_at             timestamptz not null default now(),
    primary key (city, predicted_for, horizon_hours)
);

-- ── 4. Rain probability ─────────────────────────────────────────────────────
-- rain_probability is a calibrated probability in [0,1]; predicted_rain is the
-- thresholded call, stored so a consumer doesn't have to re-pick the threshold.
create table if not exists ml_predictions.rain_forecast (
    city              text        not null,
    predicted_for     timestamp   not null,
    horizon_hours     int         not null,
    rain_probability  double precision,
    predicted_rain    boolean,
    model_version     text        not null,
    scored_at         timestamptz not null default now(),
    primary key (city, predicted_for, horizon_hours)
);

-- ── 5. City score (comfort index) forecast ──────────────────────────────────
create table if not exists ml_predictions.city_score_forecast (
    city                     text        not null,
    predicted_for            date        not null,
    horizon_days             int         not null,
    predicted_comfort_index  double precision,
    model_version            text        not null,
    scored_at                timestamptz not null default now(),
    primary key (city, predicted_for, horizon_days)
);

-- ── 6. Pollution anomaly ────────────────────────────────────────────────────
-- Grain is one row per scored observation, NOT one per city: an anomaly log that
-- only ever holds "the latest reading" cannot answer "when did Skopje spike?".
create table if not exists ml_predictions.pollution_anomaly (
    city           text        not null,
    observed_at    timestamp   not null,
    pm2_5_ug_m3    double precision,
    pm10_ug_m3     double precision,
    aqi            int,
    anomaly_score  double precision,   -- lower = more anomalous (sklearn convention)
    is_anomaly     boolean     not null,
    model_version  text        not null,
    scored_at      timestamptz not null default now(),
    primary key (city, observed_at)
);

create index if not exists ix_pollution_anomaly_flagged
    on ml_predictions.pollution_anomaly (city, observed_at desc)
    where is_anomaly;

-- ── 7. Model registry — the training audit trail ────────────────────────────
-- Written by train.py on every run. This is what makes "is the model any good?"
-- answerable in SQL instead of by scrolling a terminal, and it's the table that
-- records whether a model actually beat its naive baseline on that training run.
create table if not exists ml_predictions.model_registry (
    model_name      text        not null,
    trained_at      timestamptz not null default now(),
    model_version   text        not null,
    horizon         text        not null,
    rows_trained    int         not null,
    rows_tested     int         not null,
    metric_name     text        not null,   -- 'mae' | 'roc_auc'
    metric_value    double precision,
    baseline_name   text        not null,   -- what it was compared against
    baseline_value  double precision,
    skill           double precision,       -- 1 - model/baseline; >0 means better
    beats_baseline  boolean     not null,
    train_window    text,                   -- min..max timestamp of training rows
    notes           text,
    primary key (model_name, trained_at)
);

-- Latest training result per model — the one-line health check.
create or replace view ml_predictions.model_health as
select distinct on (model_name)
       model_name, trained_at, model_version, horizon,
       rows_trained, rows_tested,
       metric_name, metric_value, baseline_name, baseline_value,
       skill, beats_baseline, train_window, notes
from ml_predictions.model_registry
order by model_name, trained_at desc;

-- Most recent prediction per city per model, unioned into one feed. Convenient
-- for a Power BI import: one table instead of five.
create or replace view ml_predictions.latest_predictions as
    select 'aqi_pm2_5'   as model_name, city, predicted_for::timestamp,
           predicted_pm2_5 as predicted_value, 'ug/m3' as unit,
           horizon_hours::text || 'h' as horizon, model_version, scored_at
    from ml_predictions.aqi_forecast
union all
    select 'temperature', city, predicted_for::timestamp,
           predicted_temp, 'C', horizon_hours::text || 'h', model_version, scored_at
    from ml_predictions.temperature_forecast
union all
    select 'traffic', city, predicted_for::timestamp,
           predicted_congestion, 'score', horizon_hours::text || 'h', model_version, scored_at
    from ml_predictions.traffic_forecast
union all
    select 'rain', city, predicted_for::timestamp,
           rain_probability, 'probability', horizon_hours::text || 'h', model_version, scored_at
    from ml_predictions.rain_forecast
union all
    select 'city_score', city, predicted_for::timestamp,
           predicted_comfort_index, 'index', horizon_days::text || 'd', model_version, scored_at
    from ml_predictions.city_score_forecast;
