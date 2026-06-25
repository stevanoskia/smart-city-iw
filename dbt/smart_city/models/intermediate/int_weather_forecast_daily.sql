{{
    config(
        materialized='incremental',
        unique_key=['city', 'country', 'forecast_day'],
        incremental_strategy='delete+insert'
    )
}}

-- Incremental daily weather forecast facts per city.
-- Re-processes last 2 days on each run — forecasts are updated hourly by OpenWeather,
-- so recent days need to be refreshed to capture the latest predictions.

WITH forecast_base AS (

    SELECT
        city,
        country,
        DATE_TRUNC('day', forecast_at) AS forecast_day,

        temp_celsius,
        feels_like_celsius,
        temp_min_celsius,
        temp_max_celsius,
        humidity_pct,
        pressure_hpa,
        wind_speed_ms,
        wind_gust_ms,
        precipitation_probability_pct,
        rain_3h_mm,
        snow_3h_mm,
        cloudiness_pct,
        weather_main,
        weather_description,
        day_or_night,

        forecast_at,
        extracted_at

    FROM {{ ref('stg_weather_forecast') }}

    {% if is_incremental() %}
    WHERE forecast_at >= (
        SELECT COALESCE(MAX(forecast_day), NOW() - INTERVAL '7 days') - INTERVAL '2 days'
        FROM {{ this }}
    )
    {% endif %}

),

weather_label_counts AS (

    -- Find the most common weather condition per city per day.
    SELECT
        city,
        country,
        forecast_day,
        weather_main,
        COUNT(*) AS label_count,

        ROW_NUMBER() OVER (
            PARTITION BY city, country, forecast_day
            ORDER BY COUNT(*) DESC, weather_main
        ) AS row_number

    FROM forecast_base

    GROUP BY
        city,
        country,
        forecast_day,
        weather_main

),

daily_forecast AS (

    -- Aggregate forecast measurements by city and day.
    SELECT
        city,
        country,
        forecast_day,

        -- Temperature summary
        ROUND(AVG(temp_celsius)::numeric, 2)        AS avg_temp_celsius,
        MIN(temp_min_celsius)                        AS min_temp_celsius,
        MAX(temp_max_celsius)                        AS max_temp_celsius,
        ROUND(AVG(feels_like_celsius)::numeric, 2)  AS avg_feels_like_celsius,

        -- Humidity & pressure
        ROUND(AVG(humidity_pct)::numeric, 1)         AS avg_humidity_pct,
        ROUND(AVG(pressure_hpa)::numeric, 1)         AS avg_pressure_hpa,

        -- Wind
        ROUND(AVG(wind_speed_ms)::numeric, 2)        AS avg_wind_speed_ms,
        ROUND(MAX(wind_gust_ms)::numeric, 2)         AS max_wind_gust_ms,

        -- Precipitation
        MAX(precipitation_probability_pct)           AS max_precipitation_probability_pct,
        ROUND(SUM(COALESCE(rain_3h_mm, 0))::numeric, 2) AS total_rain_mm,
        ROUND(SUM(COALESCE(snow_3h_mm, 0))::numeric, 2) AS total_snow_mm,

        -- Cloudiness
        ROUND(AVG(cloudiness_pct)::numeric, 1)       AS avg_cloudiness_pct,

        -- Number of forecast slots in this day (usually 8 = every 3h)
        COUNT(*)                                     AS forecast_slots,

        MAX(extracted_at)                            AS latest_extracted_at

    FROM forecast_base

    GROUP BY
        city,
        country,
        forecast_day

)

-- Final daily forecast dataset per city.
SELECT
    d.city,
    d.country,
    d.forecast_day,

    d.avg_temp_celsius,
    d.min_temp_celsius,
    d.max_temp_celsius,
    d.avg_feels_like_celsius,

    w.weather_main        AS dominant_weather,
    w.weather_main        AS dominant_weather_description,

    d.avg_humidity_pct,
    d.avg_pressure_hpa,
    d.avg_wind_speed_ms,
    d.max_wind_gust_ms,

    d.max_precipitation_probability_pct,
    d.total_rain_mm,
    d.total_snow_mm,
    d.avg_cloudiness_pct,

    d.forecast_slots,
    d.latest_extracted_at

FROM daily_forecast d

LEFT JOIN weather_label_counts w
    ON d.city = w.city
    AND d.country = w.country
    AND d.forecast_day = w.forecast_day
    AND w.row_number = 1
