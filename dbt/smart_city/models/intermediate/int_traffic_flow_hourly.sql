{{
    config(
        materialized='incremental',
        unique_key=['city', 'country', 'observed_hour'],
        incremental_strategy='delete+insert'
    )
}}

-- Incremental hourly traffic flow facts per city.
-- Re-processes last 6 hours to deduplicate and handle late-arriving records.

WITH traffic_flow_base AS (

    SELECT
        city,
        country,
        DATE_TRUNC('hour', observed_at) AS observed_hour,

        road_class,
        road_closure,

        current_speed_kmh,
        free_flow_speed_kmh,
        current_travel_time_sec,
        free_flow_travel_time_sec,
        congestion_score,
        confidence,

        observed_at,
        extracted_at

    FROM {{ ref('stg_traffic_flow') }}

    {% if is_incremental() %}
    WHERE observed_at >= (
        SELECT COALESCE(MAX(observed_hour), NOW() - INTERVAL '7 days') - INTERVAL '24 hours'
        FROM {{ this }}
    )
    {% endif %}

),

latest_traffic_status AS (

    -- Rank traffic records inside each city-hour group.
    -- row_number = 1 is the latest record based on observed_at and extracted_at.
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY city, country, observed_hour
            ORDER BY observed_at DESC, extracted_at DESC
        ) AS row_number

    FROM traffic_flow_base

),

hourly_traffic_flow AS (

    -- Aggregate numeric traffic measurements by city and hour.
    SELECT
        city,
        country,
        observed_hour,

        ROUND(AVG(current_speed_kmh)::numeric, 2)         AS avg_current_speed_kmh,
        ROUND(AVG(free_flow_speed_kmh)::numeric, 2)       AS avg_free_flow_speed_kmh,
        ROUND(AVG(current_travel_time_sec)::numeric, 2)   AS avg_current_travel_time_sec,
        ROUND(AVG(free_flow_travel_time_sec)::numeric, 2) AS avg_free_flow_travel_time_sec,

        ROUND(AVG(congestion_score)::numeric, 2)          AS avg_congestion_score,
        MIN(congestion_score)                             AS min_congestion_score,
        MAX(congestion_score)                             AS max_congestion_score,

        ROUND(AVG(confidence)::numeric, 2)                AS avg_confidence,

        COUNT(*)                                          AS measurements_count,
        COUNT(*) FILTER (WHERE road_closure = true)       AS road_closure_count,

        MAX(observed_at)                                  AS latest_observed_at,
        MAX(extracted_at)                                 AS latest_extracted_at

    FROM traffic_flow_base

    GROUP BY
        city,
        country,
        observed_hour

)

-- Final hourly traffic flow dataset per city.
SELECT
    h.city,
    h.country,
    h.observed_hour,

    h.avg_current_speed_kmh,
    h.avg_free_flow_speed_kmh,
    h.avg_current_travel_time_sec,
    h.avg_free_flow_travel_time_sec,

    h.avg_congestion_score,
    h.min_congestion_score,
    h.max_congestion_score,

    CASE
        WHEN h.avg_congestion_score = 0              THEN 'No congestion'
        WHEN h.avg_congestion_score < 0.3            THEN 'Low congestion'
        WHEN h.avg_congestion_score < 0.6            THEN 'Medium congestion'
        ELSE                                              'High congestion'
    END AS congestion_level,

    h.avg_confidence,
    h.road_closure_count,

    l.road_class,
    l.road_closure,

    h.measurements_count,
    h.latest_observed_at,
    h.latest_extracted_at

FROM hourly_traffic_flow h

LEFT JOIN latest_traffic_status l
    ON h.city = l.city
    AND h.country = l.country
    AND h.observed_hour = l.observed_hour
    AND l.row_number = 1