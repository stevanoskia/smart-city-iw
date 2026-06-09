{{ config(materialized='view') }}

-- Purpose:
-- This intermediate model prepares traffic incident data for analytics per city.
-- city and country are now available in staging after the ingest.py fix.

WITH incidents_base AS (

    SELECT
        city,
        country,
        DATE_TRUNC('hour', observed_at) AS observed_hour,

        category_id,
        geometry,

        road_from,
        road_to,
        started_at,
        ends_at,
        delay_sec,
        length_m,
        number_of_reports,

        observed_at,
        extracted_at

    FROM {{ ref('stg_traffic_incidents') }}

),

geometry_summary AS (

    -- Count geometry types per city-hour.
    SELECT
        city,
        country,
        observed_hour,
        geometry->>'type' AS geometry_type,
        COUNT(*) AS geometry_count,

        ROW_NUMBER() OVER (
            PARTITION BY city, country, observed_hour
            ORDER BY COUNT(*) DESC, geometry->>'type'
        ) AS row_number

    FROM incidents_base

    GROUP BY
        city,
        country,
        observed_hour,
        geometry->>'type'

),

category_summary AS (

    -- Count incidents by category per city-hour.
    SELECT
        city,
        country,
        observed_hour,
        category_id,
        COUNT(*) AS category_count,

        ROW_NUMBER() OVER (
            PARTITION BY city, country, observed_hour
            ORDER BY COUNT(*) DESC, category_id
        ) AS row_number

    FROM incidents_base

    GROUP BY
        city,
        country,
        observed_hour,
        category_id

),

hourly_incidents AS (

    -- Aggregate incident records by city and hour.
    SELECT
        city,
        country,
        observed_hour,

        COUNT(*) AS total_incidents,

        COUNT(*) FILTER (WHERE category_id = 0) AS category_0_count,
        COUNT(*) FILTER (WHERE category_id = 1) AS category_1_count,
        COUNT(*) FILTER (WHERE category_id = 3) AS category_3_count,
        COUNT(*) FILTER (WHERE category_id = 4) AS category_4_count,
        COUNT(*) FILTER (WHERE category_id = 6) AS category_6_count,
        COUNT(*) FILTER (WHERE category_id = 7) AS category_7_count,
        COUNT(*) FILTER (WHERE category_id = 8) AS category_8_count,
        COUNT(*) FILTER (WHERE category_id = 9) AS category_9_count,

        COUNT(*) FILTER (WHERE road_from IS NOT NULL AND road_from <> '')  AS records_with_road_from,
        COUNT(*) FILTER (WHERE road_to IS NOT NULL AND road_to <> '')      AS records_with_road_to,
        COUNT(*) FILTER (WHERE started_at IS NOT NULL)                     AS records_with_started_at,
        COUNT(*) FILTER (WHERE ends_at IS NOT NULL)                        AS records_with_ends_at,
        COUNT(*) FILTER (WHERE delay_sec IS NOT NULL)                      AS records_with_delay,
        COUNT(*) FILTER (WHERE length_m IS NOT NULL)                       AS records_with_length,
        COUNT(*) FILTER (WHERE number_of_reports IS NOT NULL)              AS records_with_reports,

        MAX(observed_at)  AS latest_observed_at,
        MAX(extracted_at) AS latest_extracted_at

    FROM incidents_base

    GROUP BY
        city,
        country,
        observed_hour

)

-- Final hourly traffic incidents dataset per city.
SELECT
    h.city,
    h.country,
    h.observed_hour,

    h.total_incidents,

    c.category_id    AS most_common_category_id,
    c.category_count AS most_common_category_count,

    g.geometry_type  AS most_common_geometry_type,
    g.geometry_count AS most_common_geometry_count,

    h.category_0_count,
    h.category_1_count,
    h.category_3_count,
    h.category_4_count,
    h.category_6_count,
    h.category_7_count,
    h.category_8_count,
    h.category_9_count,

    h.records_with_road_from,
    h.records_with_road_to,
    h.records_with_started_at,
    h.records_with_ends_at,
    h.records_with_delay,
    h.records_with_length,
    h.records_with_reports,

    h.latest_observed_at,
    h.latest_extracted_at

FROM hourly_incidents h

LEFT JOIN category_summary c
    ON h.city = c.city
    AND h.country = c.country
    AND h.observed_hour = c.observed_hour
    AND c.row_number = 1

LEFT JOIN geometry_summary g
    ON h.city = g.city
    AND h.country = g.country
    AND h.observed_hour = g.observed_hour
    AND g.row_number = 1
