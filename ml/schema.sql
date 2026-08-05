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
    city_key         text,
    date_key         int,
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
    city_key         text,
    date_key         int,
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
    city_key              text,
    date_key              int,
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
    city_key          text,
    date_key          int,
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
    city_key                 text,
    date_key                 int,
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
    city_key       text,
    date_key       int,
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

-- ── Star keys — retrofit + self-healing backfill ────────────────────────────
-- `city_key` / `date_key` let these tables join dim_city / dim_date like every
-- other fact, instead of sitting beside the star on a text `city` column.
--
-- Two halves, both idempotent:
--   1. ADD COLUMN IF NOT EXISTS, so a database created before these columns
--      existed retrofits itself (a no-op on a fresh one, where the create-table
--      bodies above already declared them).
--   2. A backfill guarded by `where city_key is null`. This file is applied on
--      every train/predict run, so rows written before the change repair
--      themselves on the next run and the statement then matches nothing.
--
-- city_key is COPIED FROM dim_city, never recomputed. dbt builds it with
-- dbt_utils.generate_surrogate_key(['city']), which currently reduces to
-- md5(city) — but only incidentally, via that package's null-placeholder and
-- separator conventions. Re-deriving it here would couple this schema to
-- dbt_utils internals and drift silently on an upgrade. date_key is different:
-- it is YYYYMMDD::int, a format rather than a hash, so it is computed directly.

do $$
declare
    t record;
begin
    for t in
        select * from (values
            ('aqi_forecast',         'predicted_for'),
            ('temperature_forecast', 'predicted_for'),
            ('traffic_forecast',     'predicted_for'),
            ('rain_forecast',        'predicted_for'),
            ('city_score_forecast',  'predicted_for'),
            ('pollution_anomaly',    'observed_at')     -- measured, not forecast
        ) as v(table_name, ts_col)
    loop
        execute format(
            'alter table ml_predictions.%I add column if not exists city_key text', t.table_name);
        execute format(
            'alter table ml_predictions.%I add column if not exists date_key int', t.table_name);
        execute format(
            'create index if not exists ix_%s_city_key on ml_predictions.%I (city_key)',
            t.table_name, t.table_name);
        execute format(
            'update ml_predictions.%I x
                set city_key = d.city_key,
                    date_key = to_char(x.%I, ''YYYYMMDD'')::int
               from marts.dim_city d
              where d.city = x.city
                and x.city_key is null',
            t.table_name, t.ts_col);
    end loop;
end $$;

-- Latest training result per model — the one-line health check.
create or replace view ml_predictions.model_health as
select distinct on (model_name)
       model_name, trained_at, model_version, horizon,
       rows_trained, rows_tested,
       metric_name, metric_value, baseline_name, baseline_value,
       skill, beats_baseline, train_window, notes
from ml_predictions.model_registry
order by model_name, trained_at desc;

-- Every prediction from all five forecast tables, unioned into one long feed.
--
-- NOT "the latest per city" despite an earlier name of latest_predictions: it
-- returns full history on purpose. A dashboard can always narrow to the newest row
-- with a measure, but it cannot recover history it never imported — and these
-- tables accumulate one row per city per horizon per run, so the history is the
-- part that becomes valuable over time.
--
-- This is the intended Power BI import surface: ONE table instead of five, which also avoids
-- importing six tables that all share a `city` column — the exact shape that trips
-- Power BI's relationship autodetect into a bogus "cyclic reference" error.
-- Carries city_key/date_key so it relates to dim_city on the same key every other
-- fact uses. Relate city_key -> dim_city; leave date_key UNRELATED, matching
-- mart_forecast_latest — these rows point at FUTURE dates, and a dim_date filter
-- pinned to the latest actual reading would blank every forecast.
-- DROP first, not CREATE OR REPLACE: replacing a view cannot add or reorder
-- columns ("cannot change name of view column"), and this one gained city_key /
-- date_key. Dropping keeps the file re-runnable against an older database.
-- DROP both names: the view was called latest_predictions before it was renamed,
-- and a stale one left behind would keep serving a misleading name.
drop view if exists ml_predictions.latest_predictions;
drop view if exists ml_predictions.all_predictions;
create view ml_predictions.all_predictions as
    select 'aqi_pm2_5'   as model_name, city, city_key, date_key,
           predicted_for::timestamp,
           predicted_pm2_5 as predicted_value, 'ug/m3' as unit,
           horizon_hours::text || 'h' as horizon, model_version, scored_at
    from ml_predictions.aqi_forecast
union all
    select 'temperature', city, city_key, date_key, predicted_for::timestamp,
           predicted_temp, 'C', horizon_hours::text || 'h', model_version, scored_at
    from ml_predictions.temperature_forecast
union all
    select 'traffic', city, city_key, date_key, predicted_for::timestamp,
           predicted_congestion, 'score', horizon_hours::text || 'h', model_version, scored_at
    from ml_predictions.traffic_forecast
union all
    select 'rain', city, city_key, date_key, predicted_for::timestamp,
           rain_probability, 'probability', horizon_hours::text || 'h', model_version, scored_at
    from ml_predictions.rain_forecast
union all
    select 'city_score', city, city_key, date_key, predicted_for::timestamp,
           predicted_comfort_index, 'index', horizon_days::text || 'd', model_version, scored_at
    from ml_predictions.city_score_forecast;
