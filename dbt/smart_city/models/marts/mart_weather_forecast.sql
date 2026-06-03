with source as (
    select * from {{ source('postgres_staging', 'stg_weather_forecast') }}
    where city is not null
),

-- Take the latest sync only to avoid duplicate forecasts
-- (every hourly sync re-ingests the full 5-day window)
latest_sync as (
    select max(extracted_at) as latest_at
    from source
),

deduped as (
    select s.*
    from source s
    inner join latest_sync l on s.extracted_at = l.latest_at
),

forecast as (
    select
        city,
        forecast_at,
        date_trunc('day', forecast_at)::date                       as forecast_date,
        extract(hour from forecast_at)::integer                    as forecast_hour,
        day_or_night,

        -- Temperature
        temp_celsius,
        feels_like_celsius,
        temp_min_celsius,
        temp_max_celsius,

        -- Rain
        precipitation_probability_pct,
        rain_3h_mm,
        snow_3h_mm,

        -- Weather condition
        weather_main,
        weather_description,

        -- Atmospheric
        humidity_pct,
        wind_speed_ms,
        cloudiness_pct,

        -- Alert flags
        (precipitation_probability_pct >= 70)                      as heavy_rain_alert,
        case
            when precipitation_probability_pct >= 70 then 'Heavy rain likely'
            when precipitation_probability_pct >= 40 then 'Rain possible'
            else 'Dry'
        end                                                         as rain_outlook,

        -- Daily summary (used for grouping in Power BI)
        round(avg(temp_celsius) over (
            partition by city, date_trunc('day', forecast_at)::date
        )::numeric, 1)                                             as day_avg_temp,

        round(max(precipitation_probability_pct) over (
            partition by city, date_trunc('day', forecast_at)::date
        )::numeric, 0)                                             as day_max_rain_pct,

        -- Days from now (0 = today, 1 = tomorrow, etc.)
        (date_trunc('day', forecast_at)::date
            - current_date)::integer                               as days_from_now

    from deduped
)

select * from forecast
