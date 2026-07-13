-- Hourly weather fact (grain: city × date × hour) for time-of-day and latest-reading analysis.
-- Thin wrap of the hourly weather facts + the star keys (city_key/date_key/hour_utc).
-- Full hourly history (append-only in the intermediate layer), so point-in-time reads at the
-- latest observed_at (e.g. "Latest Temp") and diurnal patterns are possible; fct_weather_daily
-- rolls this same source up to daily. Mirrors fct_traffic_hourly.

select
    city_hour_key,
    {{ dbt_utils.generate_surrogate_key(['city']) }} as city_key,    -- FK → dim_city
    to_char(date_utc, 'YYYYMMDD')::int  as date_key,     -- FK → dim_date
    hour_utc,                                            -- FK → dim_hour
    city,
    country,
    date_utc,
    observed_at,
    temp_celsius,
    feels_like_celsius,
    temp_min_celsius,
    temp_max_celsius,
    humidity_pct,
    pressure_hpa,
    wind_speed_ms,
    wind_gust_ms,
    weather_main,
    weather_description,
    cloudiness_pct,
    visibility_m,
    rain_1h_mm,
    snow_1h_mm
from {{ ref('int_city_hourly_weather') }}
