-- Current forecast: the latest-issued prediction per (city, forecast_at), for
-- future time slots only. This is the forward-looking "what's the 5-day forecast"
-- view, derived from the durable issue history.

with ranked as (
    select *,
           row_number() over (
               partition by city, forecast_at      -- one row per future slot
               order by issued_at desc              -- keep the most recently issued prediction
           ) as _rn
    from {{ ref('int_city_weather_forecast') }}
    -- forward-looking only; compare in naive UTC to match forecast_at
    where forecast_at >= date_trunc('hour', now() at time zone 'UTC')
)

select
    md5(city || '|' || forecast_at::text)           as forecast_slot_key,
    city,
    forecast_at,
    forecast_date_utc,
    forecast_hour_utc,
    issued_at,
    lead_time_hours,

    -- Predicted measures
    temp_celsius,
    feels_like_celsius,
    precipitation_probability,
    weather_main,
    wind_speed_ms,
    humidity_pct,
    rain_3h_mm
from ranked
where _rn = 1
