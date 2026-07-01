-- Date dimension: a contiguous daily spine bounded by the dates actually observed
-- in the facts (weather ∪ traffic). Built after the facts so the bounds exist.

with bounds as (
    -- span EVERY daily fact's dates so no fact has a date_key missing from dim_date
    select min(date_utc) as lo, max(date_utc) as hi
    from (
        select date_utc from {{ ref('fct_weather_daily') }}
        union
        select date_utc from {{ ref('fct_pollution_daily') }}
        union
        select date_utc from {{ ref('fct_traffic_daily') }}
    ) d
),

spine as (
    select generate_series(lo, hi, interval '1 day')::date as date_utc
    from bounds
)

select
    to_char(date_utc, 'YYYYMMDD')::int          as date_key,
    date_utc,
    extract(year    from date_utc)::int         as year,
    extract(quarter from date_utc)::int         as quarter,
    extract(month   from date_utc)::int         as month,
    trim(to_char(date_utc, 'Month'))            as month_name,
    extract(day     from date_utc)::int         as day,
    extract(isodow  from date_utc)::int         as day_of_week,
    trim(to_char(date_utc, 'Day'))              as day_name,
    extract(week    from date_utc)::int         as week_of_year,
    (extract(isodow from date_utc) >= 6)        as is_weekend
from spine
