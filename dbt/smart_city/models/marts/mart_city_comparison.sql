with source as (
    select * from {{ ref('int_composite_city_score') }}
),

ranked as (
    select
        city,
        date_utc,

        -- Temperature
        avg_temp_celsius,
        min_temp_celsius,
        max_temp_celsius,
        avg_humidity_pct,
        total_rain_mm,
        dominant_weather_main,

        -- Air quality
        avg_aqi,
        max_aqi,
        avg_pm2_5_ug_m3,
        avg_pm10_ug_m3,
        hours_poor_air,

        -- Traffic (null for cities without TomTom coverage)
        avg_congestion_score,
        avg_current_speed_kmh,
        total_incidents,
        major_incidents,

        -- Normalized scores
        norm_temp,
        norm_aqi,
        norm_traffic,
        comfort_index,

        -- Comfort label
        case
            when comfort_index >= 0.75 then 'Excellent'
            when comfort_index >= 0.50 then 'Good'
            when comfort_index >= 0.25 then 'Fair'
            else                            'Poor'
        end                                                         as comfort_index_label,

        -- Whether this city has traffic data (TomTom cities: London, Berlin, Amsterdam)
        (avg_congestion_score is not null)                          as has_traffic_data,

        -- Per-day city rankings (only ranks cities present on that date)
        rank() over (
            partition by date_utc order by comfort_index desc
        )                                                           as city_rank_comfort,

        rank() over (
            partition by date_utc order by avg_aqi asc
        )                                                           as city_rank_aqi,

        rank() over (
            partition by date_utc order by avg_temp_celsius desc
        )                                                           as city_rank_temp,

        rank() over (
            partition by date_utc
            order by coalesce(avg_congestion_score, 1.0) asc
        )                                                           as city_rank_congestion

    from source
)

select * from ranked
