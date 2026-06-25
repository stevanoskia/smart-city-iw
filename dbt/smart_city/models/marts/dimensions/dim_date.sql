{{
    config(
    materialized='table'
)
}}

--dim_date: calendar table with one row per day (past 365 days to next 365)
--Used in marts to filter/group by year, month, quarter, weekday without recalculating every time

WITH date_spine AS (

    --Generate_series creates one row per day automatically no manual data entry needed
    --::date casts the timestamp to a plain date (no time part)

    SELECT
        GENERATE_SERIES(
            CURRENT_DATE - INTERVAL '365 days',
            CURRENT_DATE + INTERVAL '365 days',
            INTERVAL '1 day'
        )::date AS date_day


)

SELECT 
    --date_key: integer like 20260624 faster to JOIN on int than on date

    TO_CHAR(date_day, 'YYYYMMDD')::int AS date_key,

    date_day,

    --Calendar components extracted from the date
    EXTRACT(year FROM date_day)::int AS year,
    EXTRACT(month FROM date_day)::int AS month,
    EXTRACT(day FROM date_day)::int AS day,
    EXTRACT(quarter FROM date_day)::int AS quarter,

    --day_of_week: 0=Sunday, 6=Saturday (PostgreSQL convention)
    --used for is_weekend calculation below
    EXTRACT(dow FROM date_day)::int AS day_of_week,

    --day of week iso: 1=Monday, 7=Sunday (european)
    --use this for dashboard sorting so monday comes first
    EXTRACT(isodow FROM date_day)::int AS day_of_week_iso,

    --human readable labels for dashboard
    TRIM(TO_CHAR(date_day, 'Day')) AS day_name,
    TRIM(TO_cHAR(date_day, 'Month')) AS month_name,
 
    --is weekend: true for saturday and sunday good for dashboard traffic analysis

    EXTRACT(dow FROM date_day) IN (0,6) AS is_weekend

    FROM date_spine
    ORDER BY date_day