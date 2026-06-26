{{ config(materialized='table') }}

-- fct_traffic_hourly: one row per city per date per hour of day.
-- Used for peak-hour analysis: which hours are consistently most congested?
-- Grain: (city, date_utc, hour_utc) — one row per city per hour per day.

WITH hourly_flow AS (
    SELECT
        city,
        date_utc,
        hour_utc,

        -- Speed (km/h)
        ROUND(AVG(current_speed_kmh)::numeric, 2)  AS avg_speed_kmh,

        -- Congestion score (0 = free flow, 1 = fully congested)
        ROUND(AVG(congestion_score)::numeric, 4)   AS avg_congestion_score,

        -- Human-readable congestion label derived from the score
        CASE
            WHEN AVG(congestion_score) < 0.25 THEN 'Low'
            WHEN AVG(congestion_score) < 0.50 THEN 'Moderate'
            WHEN AVG(congestion_score) < 0.75 THEN 'High'
            ELSE 'Severe'
        END AS congestion_level,

        -- True if any snapshot in this hour reported a road closure
        BOOL_OR(road_closure)  AS had_road_closure,

        COUNT(*) AS snapshot_count

    FROM {{ ref('int_city_hourly_traffic_flow') }}
    GROUP BY city, date_utc, hour_utc
),

hourly_incidents AS (
    SELECT
        city,
        date_utc,
        hour_utc,

        COUNT(DISTINCT incident_id)  AS incident_count,
        SUM(delay_sec)               AS total_delay_sec

    FROM {{ ref('int_city_hourly_traffic_incidents') }}
    GROUP BY city, date_utc, hour_utc
)

SELECT
    -- Surrogate key: unique identifier for each city + date + hour combination
    md5(f.city || '|' || f.date_utc::text || '|' || f.hour_utc::text)  AS city_hour_key,

    -- Foreign keys for star schema joins
    md5(f.city)                             AS city_key,
    TO_CHAR(f.date_utc, 'YYYYMMDD')::int   AS date_key,
    f.hour_utc                              AS hour_key,

    f.city,
    f.date_utc,
    f.hour_utc,

    -- Flow metrics
    f.avg_speed_kmh,
    f.avg_congestion_score,
    f.congestion_level,
    f.had_road_closure,
    f.snapshot_count,

    -- Incident metrics (LEFT JOIN: hours with no incidents get 0)
    COALESCE(i.incident_count, 0)   AS incident_count,
    COALESCE(i.total_delay_sec, 0)  AS total_delay_sec

FROM hourly_flow f
LEFT JOIN hourly_incidents i
    ON f.city = i.city
    AND f.date_utc = i.date_utc
    AND f.hour_utc = i.hour_utc

ORDER BY f.city, f.date_utc, f.hour_utc
