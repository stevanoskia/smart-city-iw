-- Durable hourly per-city air-quality facts (one row per real observation).
-- Incremental + append-only: accumulates clean, deduped hourly history forever,
-- independent of airbyte_raw retention. The daily rollup is built from this.

{{ config(
    materialized='incremental',
    unique_key='city_hour_key',
    incremental_strategy='delete+insert'
) }}

with new_rows as (
    select *
    from {{ ref('stg_air_pollution') }}
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
               partition by city, observed_at      -- one row per real hourly AQI reading
               order by extracted_at desc            -- keep the most recently synced copy
           ) as _rn
    from new_rows
)

select
    md5(city || '|' || observed_at::text)           as city_hour_key,
    city,
    observed_at,
    date_trunc('day', observed_at)::date            as date_utc,   -- for daily rollups
    extract(hour from observed_at)::int             as hour_utc,   -- for time-of-day analysis
    extracted_at,

    -- AQI (OpenWeather scale 1=Good ... 5=Very Poor)
    aqi,
    aqi_label,

    -- Pollutants (μg/m³)
    co_ug_m3,
    no_ug_m3,
    no2_ug_m3,
    o3_ug_m3,
    so2_ug_m3,
    pm2_5_ug_m3,
    pm10_ug_m3,
    nh3_ug_m3
from deduped
where _rn = 1
