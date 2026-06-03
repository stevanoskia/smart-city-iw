with source as (
    select * from {{ ref('int_composite_city_score') }}
),

windowed as (
    select
        city,
        date_utc,

        -- Headline scores
        comfort_index,
        comfort_index                                               as livability_score,

        -- Key raw metrics for KPI cards
        avg_temp_celsius,
        avg_aqi,
        avg_congestion_score,
        total_rain_mm,
        hours_poor_air,
        total_incidents,
        major_incidents,
        dominant_weather_main,

        -- Normalized components (useful for gauge charts)
        norm_temp,
        norm_aqi,
        norm_traffic,

        -- Comfort label
        case
            when comfort_index >= 0.75 then 'Excellent'
            when comfort_index >= 0.50 then 'Good'
            when comfort_index >= 0.25 then 'Fair'
            else                            'Poor'
        end                                                         as comfort_index_label,

        -- AQI alert: 3+ hours of poor air quality in the day
        (hours_poor_air >= 3)                                       as aqi_alert,

        -- Congestion label
        case
            when avg_congestion_score is null    then 'No data'
            when avg_congestion_score < 0.2      then 'Low'
            when avg_congestion_score < 0.4      then 'Moderate'
            when avg_congestion_score < 0.6      then 'High'
            else                                      'Severe'
        end                                                         as congestion_label,

        -- 7-day rolling comfort index
        round(
            avg(comfort_index) over (
                partition by city order by date_utc
                rows between 6 preceding and current row
            )::numeric, 3
        )                                                           as rolling_7d_comfort,

        -- Prior 7-day comfort for trend comparison
        round(
            avg(comfort_index) over (
                partition by city order by date_utc
                rows between 13 preceding and 7 preceding
            )::numeric, 3
        )                                                           as prior_7d_comfort

    from source
),

with_trend as (
    select
        *,
        case
            when prior_7d_comfort is null then 'Insufficient data'
            when rolling_7d_comfort > prior_7d_comfort + 0.02 then 'Improving'
            when rolling_7d_comfort < prior_7d_comfort - 0.02 then 'Worsening'
            else 'Stable'
        end                                                         as comfort_trend

    from windowed
)

select * from with_trend
