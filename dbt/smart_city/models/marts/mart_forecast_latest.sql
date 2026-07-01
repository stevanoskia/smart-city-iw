-- Latest forward-looking forecast: the most recent prediction for each future slot.
-- Dedupes the forecast issue-history to the newest issuance per (city, forecast_at),
-- keeping only slots that are still in the future.

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
    md5(city || '|' || forecast_at::text)   as forecast_slot_key,
    md5(city)                               as city_key,
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
