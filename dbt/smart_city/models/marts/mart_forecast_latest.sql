-- Latest forward-looking forecast: the most recent prediction for each future slot.
-- Dedupes the forecast issue-history to the newest issuance per (city, forecast_at),
-- keeping only slots that are still in the future.
--
-- Stays materialized=table (NOT incremental): this is a forward-looking snapshot — each run
-- slots that have passed must DISAPPEAR (they drop out of `forecast_at >= now()`). delete+insert
-- only replaces matching keys, never removes rows that fell out of the filter, so an incremental
-- build would leave stale past forecasts forever. Full rebuild is the correct semantics.

with ranked as (
    select *,
           row_number() over (
               partition by city, forecast_at
               order by issued_at desc
           ) as _rn
    from {{ ref('int_city_weather_forecast') }}
    where forecast_at >= date_trunc('hour', now() at time zone 'UTC')
)

select
    {{ dbt_utils.generate_surrogate_key(['city', 'forecast_at']) }} as forecast_slot_key,
    {{ dbt_utils.generate_surrogate_key(['city']) }}               as city_key,
    city,
    forecast_at,
    forecast_date_utc,
    forecast_hour_utc,
    issued_at,
    lead_time_hours,
    temp_celsius,
    feels_like_celsius,
    precipitation_probability,
    weather_main,
    wind_speed_ms,
    humidity_pct,
    rain_3h_mm
from ranked
where _rn = 1
