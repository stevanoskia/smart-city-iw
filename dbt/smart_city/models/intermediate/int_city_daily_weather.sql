-- Daily per-city weather aggregates, rolled up from the hourly facts.
-- Dedupe already happened upstream in int_city_hourly_weather, so this is a
-- pure aggregation to one row per (city, date_utc) with a surrogate key.

with daily as (
    select
        city,
        country,
        date_utc,

        -- Temperature
        round(avg(temp_celsius)::numeric, 2)            as avg_temp_celsius,
        round(min(temp_celsius)::numeric, 2)            as min_temp_celsius,
        round(max(temp_celsius)::numeric, 2)            as max_temp_celsius,
        round(avg(feels_like_celsius)::numeric, 2)      as avg_feels_like_celsius,

        -- Atmospheric
        round(avg(humidity_pct)::numeric, 1)            as avg_humidity_pct,
        round(avg(pressure_hpa)::numeric, 1)            as avg_pressure_hpa,

        -- Wind
        round(avg(wind_speed_ms)::numeric, 2)           as avg_wind_speed_ms,
        round(max(wind_speed_ms)::numeric, 2)           as max_wind_speed_ms,

        -- Precipitation (sum of hourly readings as a daily proxy)
        round(coalesce(sum(rain_1h_mm), 0)::numeric, 2) as total_rain_mm,
        round(coalesce(sum(snow_1h_mm), 0)::numeric, 2) as total_snow_mm,

        -- Clouds
        round(avg(cloudiness_pct)::numeric, 1)          as avg_cloudiness_pct,

        -- Most frequent weather condition of the day
        mode() within group (order by weather_main)     as dominant_weather_main,

        count(*)                                        as observation_count
    from {{ ref('int_city_hourly_weather') }}
    group by city, country, date_utc
)

select
    md5(city || '|' || date_utc::text)                  as city_date_key,
    daily.*
from daily
