-- Durable forecast issue-history (one row per prediction issuance).
-- Incremental + append-only: persists every forecast AS IT WAS ISSUED, so it
-- survives airbyte_raw pruning (7-day window) and can be scored for accuracy
-- after the target time has passed.
--
-- Two timestamps:
--   forecast_at  — the future time the prediction describes (3-hour steps, 5 days out)
--   issued_at    — when the prediction was made (the hourly sync / extracted_at)
-- lead_time = forecast_at - issued_at (how far ahead the prediction looked).

{{ config(
    materialized='incremental',
    unique_key='forecast_key',
    incremental_strategy='delete+insert'
) }}

with new_rows as (
    -- issued_at as naive UTC so it's the same type as forecast_at (which is
    -- `to_timestamp(dt) at time zone 'UTC'`). Mixing naive + tz-aware here would
    -- coerce via the session timezone and skew every lead_time by the TZ offset.
    select *,
           (extracted_at at time zone 'UTC') as issued_at_utc
    from {{ ref('stg_weather_forecast') }}
    where city is not null
    {% if is_incremental() %}
      -- 6h lookback absorbs late/re-synced rows; the dedupe + delete+insert below
      -- makes reprocessing idempotent (no duplicates land).
      and (extracted_at at time zone 'UTC') > (select max(issued_at) - interval '6 hours' from {{ this }})
    {% endif %}
),

deduped as (
    select *,
           row_number() over (
               partition by city, forecast_at, issued_at_utc   -- one row per issuance of a slot
               order by extracted_at desc
           ) as _rn
    from new_rows
)

select
    md5(city || '|' || forecast_at::text || '|' || issued_at_utc::text) as forecast_key,
    city,
    forecast_at,
    issued_at_utc                                           as issued_at,

    -- Lead time: how far ahead this prediction looked (both sides naive UTC)
    round(extract(epoch from (forecast_at - issued_at_utc)) / 3600.0)::int as lead_time_hours,
    case
        when forecast_at - issued_at_utc < interval '6 hours'  then '<6h'
        when forecast_at - issued_at_utc < interval '24 hours' then '6-24h'
        when forecast_at - issued_at_utc < interval '3 days'   then '1-3d'
        else                                                        '3-5d'
    end                                                     as lead_time_bucket,

    -- Slicing helpers on the target time
    date_trunc('day', forecast_at)::date                    as forecast_date_utc,
    extract(hour from forecast_at)::int                     as forecast_hour_utc,

    -- Predicted measures
    temp_celsius,
    feels_like_celsius,
    precipitation_probability,
    weather_main,
    wind_speed_ms,
    humidity_pct,
    rain_3h_mm
from deduped
where _rn = 1
