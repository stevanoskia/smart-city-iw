{{
    config(
        materialized='incremental',
        unique_key=['city', 'country', 'observed_hour'],
        incremental_strategy='delete+insert'
    )
}}

-- Incremental hourly weather facts per city.
-- On each run, re-processes the last 6 hours to handle late-arriving or duplicate raw records.
-- delete+insert on (city, country, observed_hour) ensures one clean row per city per hour.

WITH weather_base AS (

    SELECT
        city,
        country,
        DATE_TRUNC('hour', observed_at) AS observed_hour,

        temp_celsius,
        feels_like_celsius,
        temp_min_celsius,
        temp_max_celsius,
        humidity_pct,
        pressure_hpa,
        wind_speed_ms,

        weather_main,
        weather_description,
        cloudiness_pct,
        visibility_m,
        rain_1h_mm,
        snow_1h_mm,

        observed_at,
        extracted_at

    FROM {{ ref('stg_current_weather') }}

{% if is_incremental() %}
WHERE observed_at >= (
    SELECT COALESCE(MAX(observed_hour), NOW() - INTERVAL '7 days') - INTERVAL '24 hours'
    FROM {{ this }}
)
{% endif %}

),

latest_weather AS (

    -- Rank records inside each city-hour group.
    -- The latest observation is used to keep descriptive fields such as weather_main and weather_description.
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY city, country, observed_hour
            ORDER BY observed_at DESC, extracted_at DESC
        ) AS row_number

    FROM weather_base

),

hourly_weather AS (

    -- Aggregate numeric weather measurements by city and hour.
    -- This gives one clean hourly record per city.
    SELECT
        city,
        country,
        observed_hour,

        ROUND(AVG(temp_celsius)::numeric, 2) AS avg_temp_celsius,
        MIN(temp_celsius) AS min_temp_celsius,
        MAX(temp_celsius) AS max_temp_celsius,

        ROUND(AVG(feels_like_celsius)::numeric, 2) AS avg_feels_like_celsius,
        MIN(temp_min_celsius) AS min_temp_min_celsius,
        MAX(temp_max_celsius) AS max_temp_max_celsius,

        ROUND(AVG(humidity_pct)::numeric, 2) AS avg_humidity_pct,
        ROUND(AVG(pressure_hpa)::numeric, 2) AS avg_pressure_hpa,
        ROUND(AVG(wind_speed_ms)::numeric, 2) AS avg_wind_speed_ms,

        COUNT(*) AS measurements_count

    FROM weather_base

    GROUP BY
        city,
        country,
        observed_hour

)

-- Final hourly weather dataset.
-- Numeric values are aggregated, while descriptive weather fields are taken from the latest record in the hour.
SELECT
    h.city,
    h.country,
    h.observed_hour,

    h.avg_temp_celsius,
    h.min_temp_celsius,
    h.max_temp_celsius,
    h.avg_feels_like_celsius,
    h.min_temp_min_celsius,
    h.max_temp_max_celsius,
    h.avg_humidity_pct,
    h.avg_pressure_hpa,
    h.avg_wind_speed_ms,

    l.weather_main,
    l.weather_description,
    l.cloudiness_pct,
    l.visibility_m,
    l.rain_1h_mm,
    l.snow_1h_mm,

    h.measurements_count,
    l.observed_at AS latest_observed_at,
    l.extracted_at AS latest_extracted_at

FROM hourly_weather h

LEFT JOIN latest_weather l
    ON h.city = l.city
    AND h.country = l.country
    AND h.observed_hour = l.observed_hour
    AND l.row_number = 1