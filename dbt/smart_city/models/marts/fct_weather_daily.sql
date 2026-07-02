-- Daily weather fact: one row per (city, date_utc), rolled up from the hourly
-- weather facts. Star-schema fact — carries city_key / date_key for the dims.

with daily as (
    select
        city,
        country,
        date_utc,
        round(avg(temp_celsius)::numeric, 2)            as avg_temp_celsius,
        round(min(temp_celsius)::numeric, 2)            as min_temp_celsius,
        round(max(temp_celsius)::numeric, 2)            as max_temp_celsius,
        round(avg(feels_like_celsius)::numeric, 2)      as avg_feels_like_celsius,
        round(avg(humidity_pct)::numeric, 1)            as avg_humidity_pct,
        round(avg(pressure_hpa)::numeric, 1)            as avg_pressure_hpa,
        round(avg(wind_speed_ms)::numeric, 2)           as avg_wind_speed_ms,
        round(max(wind_speed_ms)::numeric, 2)           as max_wind_speed_ms,
        round(coalesce(sum(rain_1h_mm), 0)::numeric, 2) as total_rain_mm,
        round(coalesce(sum(snow_1h_mm), 0)::numeric, 2) as total_snow_mm,
        round(avg(cloudiness_pct)::numeric, 1)          as avg_cloudiness_pct,
        mode() within group (order by weather_main)     as dominant_weather_main,
        count(*)                                        as observation_count
    from {{ ref('int_city_hourly_weather') }}
    group by city, country, date_utc
)

select
    md5(city || '|' || date_utc::text)  as city_date_key,   -- row identity (PK)
    md5(city)                           as city_key,          -- FK → dim_city
    to_char(date_utc, 'YYYYMMDD')::int  as date_key,          -- FK → dim_date
    daily.*
from daily
