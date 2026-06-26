{{ config(materialized='table') }}

-- mart_city_daily: one wide row per city per UTC date (OBT - One Big Table).
-- Joins weather + pollution + traffic so dashboards need only one table.
-- Madrid has no traffic data -> traffic columns will be NULL for Madrid.

WITH weather AS(
    SELECT * FROM {{ ref('fct_weather_daily')}}
),

pollution AS(
    SELECT * FROM {{ ref('fct_pollution_daily')}}
),

traffic AS(
    SELECT * FROM {{ ref('fct_traffic_daily')}}
)

SELECT
    --Surrogate key
    w.city_date_key,

    --Foreign keys
    w.city_key,
    w.date_key,

    w.city,
    w.country,
    w.date_utc,

    --Weather
    w.avg_temp_c,
    w.min_temp_c,
    w.max_temp_c,
    w.avg_humidity_pct,
    w.avg_wind_speed_ms,
    w.total_rain_mm,
    w.total_snow_mm,
    w.dominant_weather,

    --Pollution
    p.avg_aqi,
    p.max_aqi,
    p.dominant_aqi_label,
    p.avg_pm2_5_ug_m3,
    p.avg_pm10_ug_m3,
    p.avg_no2_ug_m3,
    p.avg_co_ug_m3,

    --Traffic
    t.avg_speed_kmh,
    t.avg_congestion_score,
    t.peak_congestion_score,
    t.had_road_closure,
    t.total_incidents,
    t.total_delay_sec

    FROM weather w
    LEFT JOIN pollution p
        ON w.city_date_key=p.city_date_key
    LEFT JOIN traffic t
        ON w.city_date_key=t.city_date_key

    ORDER BY w.city, w.date_utc    
