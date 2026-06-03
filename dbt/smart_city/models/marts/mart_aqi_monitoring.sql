with source as (
    select * from {{ ref('int_city_daily_pollution') }}
),

windowed as (
    select
        city,
        date_utc,

        -- Raw pollution metrics
        avg_aqi,
        max_aqi,
        min_aqi,
        avg_pm2_5_ug_m3,
        max_pm2_5_ug_m3,
        avg_pm10_ug_m3,
        max_pm10_ug_m3,
        avg_no2_ug_m3,
        avg_o3_ug_m3,
        avg_co_ug_m3,
        avg_so2_ug_m3,
        hours_poor_air,
        observation_count,

        -- AQI labels (OpenWeather 1–5 scale)
        case round(avg_aqi)::integer
            when 1 then 'Good'
            when 2 then 'Fair'
            when 3 then 'Moderate'
            when 4 then 'Poor'
            when 5 then 'Very Poor'
            else 'Unknown'
        end                                                         as avg_aqi_label,

        case max_aqi
            when 1 then 'Good'
            when 2 then 'Fair'
            when 3 then 'Moderate'
            when 4 then 'Poor'
            when 5 then 'Very Poor'
            else 'Unknown'
        end                                                         as max_aqi_label,

        -- Alert: 3+ hours of poor air quality in a day (from CLAUDE.md)
        (hours_poor_air >= 3)                                       as aqi_alert,

        -- 7-day rolling average AQI
        round(
            avg(avg_aqi) over (
                partition by city order by date_utc
                rows between 6 preceding and current row
            )::numeric, 2
        )                                                           as rolling_7d_avg_aqi,

        -- Prior 7-day average (days 8–14 ago) for trend comparison
        round(
            avg(avg_aqi) over (
                partition by city order by date_utc
                rows between 13 preceding and 7 preceding
            )::numeric, 2
        )                                                           as prior_7d_avg_aqi

    from source
),

with_trend as (
    select
        *,
        case
            when prior_7d_avg_aqi is null then 'Insufficient data'
            when rolling_7d_avg_aqi < prior_7d_avg_aqi - 0.2 then 'Improving'
            when rolling_7d_avg_aqi > prior_7d_avg_aqi + 0.2 then 'Worsening'
            else 'Stable'
        end                                                         as aqi_trend

    from windowed
)

select * from with_trend
