-- Date dimension: an independent daily calendar spine (no dependency on the facts).
-- Lower bound is a fixed project-inception anchor so every observed fact date is
-- covered; upper bound extends past today for forward-looking (forecast) needs.
-- Built independently of the facts so the dims resolve first in the star build order.

with spine as (
    select generate_series(
        date '2026-01-01',                      -- anchor safely before first ingested data
        current_date + interval '365 days',     -- forward horizon for forecast-facing use
        interval '1 day'
    )::date as date_utc
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
