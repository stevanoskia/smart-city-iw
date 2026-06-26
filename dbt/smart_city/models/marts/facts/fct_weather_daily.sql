{{ config(materialized='table') }}

-- fct_weather_daily: one row per city per UTC date.
-- Rolls up hourly weather observations into daily summaries.
-- Grain: (city, date_utc) — one row per city per day.

WITH daily_agg AS (
    SELECT
        city,
        country,
        date_utc,

        -- Temperature (°C)
        ROUND(AVG(temp_celsius)::numeric, 2)   AS avg_temp_c,
        ROUND(MIN(temp_celsius)::numeric, 2)   AS min_temp_c,
        ROUND(MAX(temp_celsius)::numeric, 2)   AS max_temp_c,

        -- Atmospheric
        ROUND(AVG(humidity_pct)::numeric, 1)   AS avg_humidity_pct,

        -- Wind (m/s)
        ROUND(AVG(wind_speed_ms)::numeric, 2)  AS avg_wind_speed_ms,

        -- Precipitation: sum across hours = daily total (mm)
        ROUND(SUM(COALESCE(rain_1h_mm, 0))::numeric, 2) AS total_rain_mm,
        ROUND(SUM(COALESCE(snow_1h_mm, 0))::numeric, 2) AS total_snow_mm,

        -- Most frequent weather condition that day
        MODE() WITHIN GROUP (ORDER BY weather_main) AS dominant_weather,

        -- Number of hourly readings available for this day
        COUNT(*) AS observation_count

    FROM {{ ref('int_city_hourly_weather') }}
    GROUP BY city, country, date_utc
)

SELECT
    -- Surrogate key: unique identifier for each city + date combination
    md5(city || '|' || date_utc::text)   AS city_date_key,

    -- Foreign keys for star schema joins
    md5(city)                            AS city_key,
    TO_CHAR(date_utc, 'YYYYMMDD')::int  AS date_key,

    city,
    country,
    date_utc,

    avg_temp_c,
    min_temp_c,
    max_temp_c,
    avg_humidity_pct,
    avg_wind_speed_ms,
    total_rain_mm,
    total_snow_mm,
    dominant_weather,
    observation_count

FROM daily_agg
ORDER BY city, date_utc
