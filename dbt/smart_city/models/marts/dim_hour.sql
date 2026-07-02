-- Hour-of-day dimension: a static 24-row lookup for time-of-day analysis.
-- hour_utc is the join key from fct_traffic_hourly; day_part buckets the day.

with hours as (
    select generate_series(0, 23) as hour_utc
)

select
    hour_utc,
    lpad(hour_utc::text, 2, '0') || ':00'  as hour_label,   -- e.g. '06:00', '14:00'
    case
        when hour_utc between 0  and 5  then 'Night'
        when hour_utc between 6  and 11 then 'Morning'
        when hour_utc between 12 and 17 then 'Afternoon'
        else                                 'Evening'
    end as day_part
from hours
