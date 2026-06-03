with source as (
    select * from {{ ref('int_city_daily_weather') }}
),

windowed as (
    select
        city,
        country,
        date_utc,

        -- Core temperature metrics
        avg_temp_celsius,
        min_temp_celsius,
        max_temp_celsius,
        avg_feels_like_celsius,
        avg_humidity_pct,
        avg_pressure_hpa,
        avg_wind_speed_ms,
        max_wind_speed_ms,
        total_rain_mm,
        total_snow_mm,
        avg_cloudiness_pct,
        dominant_weather_main,
        observation_count,

        -- Rolling averages (partitioned per city, ordered by date)
        round(
            avg(avg_temp_celsius) over (
                partition by city order by date_utc
                rows between 6 preceding and current row
            )::numeric, 2
        )                                                           as rolling_7d_avg_temp,

        round(
            avg(avg_temp_celsius) over (
                partition by city order by date_utc
                rows between 29 preceding and current row
            )::numeric, 2
        )                                                           as rolling_30d_avg_temp,

        round(
            stddev(avg_temp_celsius) over (
                partition by city order by date_utc
                rows between 29 preceding and current row
            )::numeric, 3
        )                                                           as rolling_30d_stddev_temp,

        -- Day-over-day change
        round(
            (avg_temp_celsius - lag(avg_temp_celsius, 1) over (
                partition by city order by date_utc
            ))::numeric, 2
        )                                                           as temp_change_1d

    from source
),

with_anomaly as (
    select
        *,

        -- How far today's temp deviates from the 30-day mean
        round(
            (avg_temp_celsius - rolling_30d_avg_temp)::numeric, 2
        )                                                           as temp_anomaly,

        -- Anomaly flag: deviation > 2 standard deviations (from CLAUDE.md)
        case
            when rolling_30d_stddev_temp > 0
                and abs(avg_temp_celsius - rolling_30d_avg_temp)
                    > 2 * rolling_30d_stddev_temp
            then true
            else false
        end                                                         as is_temp_anomaly

    from windowed
)

select * from with_anomaly
