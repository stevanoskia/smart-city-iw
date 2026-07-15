-- Hourly air-quality fact (grain: city × date × hour) for time-of-day and latest-reading analysis.
-- Thin wrap of the hourly pollution facts + the star keys (city_key/date_key/hour_utc).
-- Full hourly history, so point-in-time reads at the latest observed_at (e.g. "Latest PM2.5",
-- to compare against live third-party readings) are possible; fct_pollution_daily rolls this
-- same source up to daily. Mirrors fct_traffic_hourly.

select
    city_hour_key,
    {{ dbt_utils.generate_surrogate_key(['city']) }} as city_key,    -- FK → dim_city
    to_char(date_utc, 'YYYYMMDD')::int  as date_key,     -- FK → dim_date
    hour_utc,                                            -- FK → dim_hour
    city,
    date_utc,
    observed_at,
    aqi,
    aqi_label,
    co_ug_m3,
    no_ug_m3,
    no2_ug_m3,
    o3_ug_m3,
    so2_ug_m3,
    pm2_5_ug_m3,
    pm10_ug_m3,
    nh3_ug_m3
from {{ ref('int_city_hourly_pollution') }}
