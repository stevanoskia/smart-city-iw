{{ config(materialized='table') }}

-- fct_pollution_daily: one row per city per UTC date.
-- Rolls up hourly AQI readings into daily summaries.
-- Grain: (city, date_utc) — one row per city per day.

WITH daily_agg AS (
    SELECT
        city,
        date_utc,

        -- AQI (OpenWeather scale: 1=Good, 2=Fair, 3=Moderate, 4=Poor, 5=Very Poor)
        ROUND(AVG(aqi)::numeric, 2)              AS avg_aqi,
        MAX(aqi)                                 AS max_aqi,
        MODE() WITHIN GROUP (ORDER BY aqi_label) AS dominant_aqi_label,

        -- Pollutant daily averages (μg/m³)
        ROUND(AVG(co_ug_m3)::numeric, 2)         AS avg_co_ug_m3,
        ROUND(AVG(no2_ug_m3)::numeric, 2)        AS avg_no2_ug_m3,
        ROUND(AVG(o3_ug_m3)::numeric, 2)         AS avg_o3_ug_m3,
        ROUND(AVG(pm2_5_ug_m3)::numeric, 2)      AS avg_pm2_5_ug_m3,
        ROUND(AVG(pm10_ug_m3)::numeric, 2)       AS avg_pm10_ug_m3,

        -- Number of hourly readings available for this day
        COUNT(*) AS observation_count

    FROM {{ ref('int_city_hourly_pollution') }}
    GROUP BY city, date_utc
)

SELECT
    -- Surrogate key: unique identifier for each city + date combination
    md5(city || '|' || date_utc::text)   AS city_date_key,

    -- Foreign keys for star schema joins
    md5(city)                            AS city_key,
    TO_CHAR(date_utc, 'YYYYMMDD')::int  AS date_key,

    city,
    date_utc,

    avg_aqi,
    max_aqi,
    dominant_aqi_label,
    avg_co_ug_m3,
    avg_no2_ug_m3,
    avg_o3_ug_m3,
    avg_pm2_5_ug_m3,
    avg_pm10_ug_m3,
    observation_count

FROM daily_agg
ORDER BY city, date_utc
