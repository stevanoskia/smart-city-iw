{{ config(materialized='table') }}

-- fct_traffic_daily: one row per city per UTC date.
-- Combines traffic flow and incident data into daily summaries.
-- Grain: (city, date_utc) — one row per city per day.

WITH daily_flow AS (
    SELECT
        city,
        date_utc,

        -- Speed (km/h)
        ROUND(AVG(current_speed_kmh)::numeric, 2)  AS avg_speed_kmh,
        ROUND(MIN(current_speed_kmh)::numeric, 2)  AS min_speed_kmh,

        -- Congestion score (0 = free flow, 1 = fully congested)
        ROUND(AVG(congestion_score)::numeric, 4)   AS avg_congestion_score,
        ROUND(MAX(congestion_score)::numeric, 4)   AS peak_congestion_score,

        -- True if any snapshot reported a road closure that day
        BOOL_OR(road_closure)                      AS had_road_closure,

        COUNT(*) AS snapshot_count

    FROM {{ ref('int_city_hourly_traffic_flow') }}
    GROUP BY city, date_utc
),

daily_incidents AS (
    SELECT
        city,
        date_utc,

        -- Incident counts and impact
        COUNT(DISTINCT incident_id)  AS total_incidents,
        SUM(delay_sec)               AS total_delay_sec,
        MAX(magnitude_of_delay)      AS max_incident_severity

    FROM {{ ref('int_city_hourly_traffic_incidents') }}
    GROUP BY city, date_utc
)

SELECT
    -- Surrogate key: unique identifier for each city + date combination
    md5(f.city || '|' || f.date_utc::text)  AS city_date_key,

    -- Foreign keys for star schema joins
    md5(f.city)                             AS city_key,
    TO_CHAR(f.date_utc, 'YYYYMMDD')::int   AS date_key,

    f.city,
    f.date_utc,

    -- Flow metrics
    f.avg_speed_kmh,
    f.min_speed_kmh,
    f.avg_congestion_score,
    f.peak_congestion_score,
    f.had_road_closure,
    f.snapshot_count,

    -- Incident metrics (LEFT JOIN: days with no incidents get 0)
    COALESCE(i.total_incidents, 0)   AS total_incidents,
    COALESCE(i.total_delay_sec, 0)   AS total_delay_sec,
    i.max_incident_severity

FROM daily_flow f
LEFT JOIN daily_incidents i
    ON f.city = i.city
    AND f.date_utc = i.date_utc

ORDER BY f.city, f.date_utc
