-- Temperature trend + anomaly analytics: rolling baselines and day/week deltas
-- per city, built on the daily weather fact. Closes spec area #1.
--
-- Stays materialized=table (NOT incremental): the 7d/30d rolling averages and lag(7)
-- need the prior days as INPUT rows, so a recent-rows-only incremental batch would compute
-- truncated (wrong) baselines/deltas at the boundary. Daily grain × ~10 cities is tiny —
-- full rebuild is correct and cheap.

with daily as (
    select city, date_utc, avg_temp_celsius, min_temp_celsius, max_temp_celsius
    from {{ ref('fct_weather_daily') }}
),

windowed as (
    select *,
        round(avg(avg_temp_celsius) over (partition by city order by date_utc
            rows between 6 preceding and current row)::numeric, 2)  as temp_7d_avg,
        round(avg(avg_temp_celsius) over (partition by city order by date_utc
            rows between 29 preceding and current row)::numeric, 2) as temp_30d_avg,
        lag(avg_temp_celsius, 1) over (partition by city order by date_utc) as prev_day_temp,
        lag(avg_temp_celsius, 7) over (partition by city order by date_utc) as prev_week_temp
    from daily
)

select
    {{ dbt_utils.generate_surrogate_key(['city', 'date_utc']) }} as city_date_key,
    {{ dbt_utils.generate_surrogate_key(['city']) }}            as city_key,
    to_char(date_utc, 'YYYYMMDD')::int  as date_key,
    city, date_utc,
    avg_temp_celsius, min_temp_celsius, max_temp_celsius, temp_7d_avg, temp_30d_avg,
    round((avg_temp_celsius - temp_30d_avg)::numeric, 2)    as temp_anomaly,
    case
        when temp_30d_avg is null                  then 'Normal'
        when avg_temp_celsius - temp_30d_avg >  5  then 'Hot anomaly'
        when avg_temp_celsius - temp_30d_avg < -5  then 'Cold anomaly'
        else 'Normal' end                                  as temp_anomaly_label,
    round((avg_temp_celsius - prev_day_temp)::numeric, 2)   as dod_change,
    round((avg_temp_celsius - prev_week_temp)::numeric, 2)  as wow_change
from windowed
