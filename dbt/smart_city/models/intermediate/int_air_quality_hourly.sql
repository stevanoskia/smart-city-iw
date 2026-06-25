{{
    config(
        materialized='incremental',
        unique_key=['city', 'country', 'observed_hour'],
        incremental_strategy='delete+insert'
    )
}}

-- Incremental hourly air quality facts per city.
-- Re-processes last 6 hours on each run to handle late-arriving raw records.

WITH air_quality_base AS (

    SELECT
        city,
        country,
        DATE_TRUNC('hour', observed_at) AS observed_hour,

        aqi,
        aqi_label,
        co_ug_m3,
        no_ug_m3,
        no2_ug_m3,
        o3_ug_m3,
        so2_ug_m3,
        pm2_5_ug_m3,
        pm10_ug_m3,
        nh3_ug_m3,

        observed_at,
        extracted_at

    FROM {{ ref('stg_air_pollution') }}

    {% if is_incremental() %}
    WHERE observed_at >= (
        SELECT COALESCE(MAX(observed_hour), NOW() - INTERVAL '7 days') - INTERVAL '24 hours'
        FROM {{ this }}
    )
    {% endif %}

),

aqi_label_counts AS (

    -- Count how often each AQI label appears within each city-hour group.
    SELECT
        city,
        country,
        observed_hour,
        aqi_label,
        COUNT(*) AS label_count,

        ROW_NUMBER() OVER (
            PARTITION BY city, country, observed_hour
            ORDER BY COUNT(*) DESC, aqi_label
        ) AS row_number

    FROM air_quality_base

    GROUP BY
        city,
        country,
        observed_hour,
        aqi_label

),

hourly_air_quality AS (

    -- Aggregate numeric air pollution measurements by city and hour.
    SELECT
        city,
        country,
        observed_hour,

        ROUND(AVG(aqi)::numeric, 2) AS avg_aqi,
        MIN(aqi) AS min_aqi,
        MAX(aqi) AS max_aqi,

        ROUND(AVG(co_ug_m3)::numeric, 2) AS avg_co_ug_m3,
        ROUND(AVG(no_ug_m3)::numeric, 2) AS avg_no_ug_m3,
        ROUND(AVG(no2_ug_m3)::numeric, 2) AS avg_no2_ug_m3,
        ROUND(AVG(o3_ug_m3)::numeric, 2) AS avg_o3_ug_m3,
        ROUND(AVG(so2_ug_m3)::numeric, 2) AS avg_so2_ug_m3,
        ROUND(AVG(pm2_5_ug_m3)::numeric, 2) AS avg_pm2_5_ug_m3,
        ROUND(AVG(pm10_ug_m3)::numeric, 2) AS avg_pm10_ug_m3,
        ROUND(AVG(nh3_ug_m3)::numeric, 2) AS avg_nh3_ug_m3,

        COUNT(*) AS measurements_count,
        MAX(observed_at) AS latest_observed_at,
        MAX(extracted_at) AS latest_extracted_at

    FROM air_quality_base

    GROUP BY
        city,
        country,
        observed_hour

)

-- Final hourly air quality dataset per city.
SELECT
    h.city,
    h.country,
    h.observed_hour,

    h.avg_aqi,
    h.min_aqi,
    h.max_aqi,

    l.aqi_label AS most_common_aqi_label,

    h.avg_co_ug_m3,
    h.avg_no_ug_m3,
    h.avg_no2_ug_m3,
    h.avg_o3_ug_m3,
    h.avg_so2_ug_m3,
    h.avg_pm2_5_ug_m3,
    h.avg_pm10_ug_m3,
    h.avg_nh3_ug_m3,

    h.measurements_count,
    h.latest_observed_at,
    h.latest_extracted_at

FROM hourly_air_quality h

LEFT JOIN aqi_label_counts l
    ON h.city = l.city
    AND h.country = l.country
    AND h.observed_hour = l.observed_hour
    AND l.row_number = 1