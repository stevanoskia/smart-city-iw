-- Durable hourly per-city weather facts (one row per real observation).
-- Incremental + append-only: accumulates clean, deduped hourly history forever,
-- independent of airbyte_raw retention. The daily rollup is built from this.

{{ config(
    materialized='incremental',
    unique_key='city_hour_key',
    incremental_strategy='delete+insert'
) }}

with new_rows as (
    select *
    from {{ ref('stg_current_weather') }}
    where city is not null
    {% if is_incremental() %}
      -- 6h lookback absorbs late/re-synced rows; the dedupe + delete+insert below
      -- makes reprocessing idempotent (no duplicates land).
      and extracted_at > (select max(extracted_at) - interval '6 hours' from {{ this }})
    {% endif %}
),

deduped as (
    select *,
           row_number() over (
               partition by city, observed_at      -- one row per real hourly observation
               order by extracted_at desc            -- keep the most recently synced copy
           ) as _rn
    from new_rows
)

select
    md5(city || '|' || observed_at::text)           as city_hour_key,
    city,
    country,
    observed_at,
    date_trunc('day', observed_at)::date            as date_utc,   -- for daily rollups
    extract(hour from observed_at)::int             as hour_utc,   -- for time-of-day analysis
    extracted_at,

    -- Temperature
    temp_celsius,
    feels_like_celsius,
    temp_min_celsius,
    temp_max_celsius,

    -- Atmospheric
    humidity_pct,
    pressure_hpa,

    -- Wind
    wind_speed_ms,
    wind_gust_ms,

    -- Condition
    weather_main,
    weather_description,

    -- Clouds & visibility
    cloudiness_pct,
    visibility_m,

    -- Precipitation
    rain_1h_mm,
    snow_1h_mm
from deduped
where _rn = 1
