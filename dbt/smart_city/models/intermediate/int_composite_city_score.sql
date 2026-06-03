with weather as (
    select * from {{ ref('int_city_daily_weather') }}
),

pollution as (
    select * from {{ ref('int_city_daily_pollution') }}
),

traffic as (
    select * from {{ ref('int_city_daily_traffic') }}
),

joined as (
    select
        w.city,
        w.date_utc,

        -- Raw metrics carried forward for mart layer
        w.avg_temp_celsius,
        w.min_temp_celsius,
        w.max_temp_celsius,
        w.avg_humidity_pct,
        w.total_rain_mm,
        w.dominant_weather_main,

        p.avg_aqi,
        p.max_aqi,
        p.avg_pm2_5_ug_m3,
        p.avg_pm10_ug_m3,
        p.hours_poor_air,

        t.avg_congestion_score,
        t.avg_current_speed_kmh,
        t.total_incidents,
        t.major_incidents,

        -- Normalized scores (0 = worst, 1 = best)
        -- Temperature: 0°C → 0, 30°C → 1, clamped
        least(greatest(w.avg_temp_celsius / 30.0, 0), 1)              as norm_temp,

        -- AQI: OpenWeather scale 1–5 → 0–1 (1=Good → 0, 5=VeryPoor → 1)
        (p.avg_aqi - 1.0) / 4.0                                       as norm_aqi,

        -- Traffic: congestion_score 0=free flow, 1=standstill → invert for quality
        -- Default to 0.5 when traffic data is not available for this city
        1.0 - coalesce(t.avg_congestion_score, 0.5)                   as norm_traffic

    from weather w
    inner join pollution p
        on w.city = p.city
        and w.date_utc = p.date_utc
    left join traffic t
        on w.city = t.city
        and w.date_utc = t.date_utc
),

scored as (
    select
        *,
        -- Comfort Index from CLAUDE.md business logic
        round(
            (0.4 * norm_temp + 0.4 * (1.0 - norm_aqi) + 0.2 * norm_traffic)::numeric,
            3
        )                                                              as comfort_index
    from joined
)

select * from scored
