with source as (
    select * from {{ source('postgres_staging', 'stg_current_weather') }}
),

daily as (
    select
        city,
        country,
        date_trunc('day', observed_at)::date                           as date_utc,

        -- Temperature
        round(avg(temp_celsius)::numeric, 2)                           as avg_temp_celsius,
        round(min(temp_celsius)::numeric, 2)                           as min_temp_celsius,
        round(max(temp_celsius)::numeric, 2)                           as max_temp_celsius,
        round(avg(feels_like_celsius)::numeric, 2)                     as avg_feels_like_celsius,

        -- Atmospheric
        round(avg(humidity_pct)::numeric, 1)                           as avg_humidity_pct,
        round(avg(pressure_hpa)::numeric, 1)                           as avg_pressure_hpa,

        -- Wind
        round(avg(wind_speed_ms)::numeric, 2)                          as avg_wind_speed_ms,
        round(max(wind_speed_ms)::numeric, 2)                          as max_wind_speed_ms,

        -- Precipitation (sum of hourly readings as daily proxy)
        round(coalesce(sum(rain_1h_mm), 0)::numeric, 2)                as total_rain_mm,
        round(coalesce(sum(snow_1h_mm), 0)::numeric, 2)                as total_snow_mm,

        -- Clouds
        round(avg(cloudiness_pct)::numeric, 1)                         as avg_cloudiness_pct,

        -- Most frequent weather condition of the day
        mode() within group (order by weather_main)                    as dominant_weather_main,

        count(*)                                                        as observation_count

    from source
    where city is not null
    group by city, country, date_trunc('day', observed_at)::date
)

select * from daily
