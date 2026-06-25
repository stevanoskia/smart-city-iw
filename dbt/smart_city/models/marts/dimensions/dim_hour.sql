{{
    config(
        materialized='table'
    )
}}

-- dim_hour: static lookup table with one row per hour of the day (0–23).
-- Used in marts to group by part of day (Morning, Afternoon, Evening, Night) without recalculating every time.

WITH hour_spine AS (
    -- GENERATE_SERIES(0, 23) creates integers 0 to 23 one row per hour.
    SELECT GENERATE_SERIES(0, 23) AS hour_of_day
)

SELECT
    -- hour_key: integer 0–23, used for JOIN with fact tables on EXTRACT(hour FROM timestamp)
    hour_of_day AS hour_key,

    -- hour_label: human-readable format like '06:00', '14:00' for dashboard display
    -- LPAD pads single digits with a leading zero: 6 → '06', 14 stays '14'
    LPAD(hour_of_day::text, 2, '0') || ':00'  AS hour_label,

    -- part_of_day: groups hours into 4 readable time blocks for aggregation and filtering
    CASE
        WHEN hour_of_day BETWEEN 0  AND 5  THEN 'Night'
        WHEN hour_of_day BETWEEN 6  AND 11 THEN 'Morning'
        WHEN hour_of_day BETWEEN 12 AND 17 THEN 'Afternoon'
        WHEN hour_of_day BETWEEN 18 AND 23 THEN 'Evening'
    END AS part_of_day

FROM hour_spine
ORDER BY hour_of_day