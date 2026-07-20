-- Daily traffic fact: one row per (city, date_utc). Combines flow (congestion,
-- speeds) with incident counts via a LEFT JOIN so flow-only days aren't dropped.
--
-- Incremental (delete+insert on city_date_key): only the current UTC day is mutable
-- (its hourly observations still accumulate); past days are immutable. The 2-day
-- lookback is applied to BOTH source CTEs and replaces today's rows by key.

{{ config(
    materialized='incremental',
    unique_key='city_date_key',
    incremental_strategy='delete+insert'
) }}

with daily_flow as (
    select
        city,
        date_utc,
        round(avg(congestion_score)::numeric, 3)      as avg_congestion_score,
        round(max(congestion_score)::numeric, 3)      as max_congestion_score,
        round(avg(current_speed_kmh)::numeric, 1)     as avg_current_speed_kmh,
        round(avg(free_flow_speed_kmh)::numeric, 1)   as avg_free_flow_speed_kmh,
        round(
            count(*) filter (where congestion_score > 0.3)::numeric
            / nullif(count(*), 0), 3
        )                                              as pct_congested_snapshots,
        count(*) filter (where road_closure = true)    as road_closure_count,
        count(*)                                       as flow_observation_count
    from {{ ref('int_city_hourly_traffic_flow') }}
    {% if is_incremental() %}
    where date_utc >= (select max(date_utc) - interval '2 days' from {{ this }})
    {% endif %}
    group by city, date_utc
),

daily_incidents as (
    select
        city,
        date_utc,
        count(distinct incident_id)                                       as total_incidents,
        count(distinct incident_id) filter (where magnitude_of_delay = 3) as major_incidents,
        round(avg(delay_sec)::numeric, 0)                                 as avg_delay_sec
    from {{ ref('int_city_hourly_traffic_incidents') }}
    {% if is_incremental() %}
    where date_utc >= (select max(date_utc) - interval '2 days' from {{ this }})
    {% endif %}
    group by city, date_utc
)

select
    {{ dbt_utils.generate_surrogate_key(['f.city', 'f.date_utc']) }} as city_date_key,
    {{ dbt_utils.generate_surrogate_key(['f.city']) }}              as city_key,
    to_char(f.date_utc, 'YYYYMMDD')::int    as date_key,
    f.city,
    f.date_utc,
    f.avg_congestion_score,
    f.max_congestion_score,
    f.avg_current_speed_kmh,
    f.avg_free_flow_speed_kmh,
    f.pct_congested_snapshots,
    f.road_closure_count,
    f.flow_observation_count,
    coalesce(i.total_incidents, 0)  as total_incidents,
    coalesce(i.major_incidents, 0)  as major_incidents,
    i.avg_delay_sec
from daily_flow f
left join daily_incidents i
    on f.city = i.city and f.date_utc = i.date_utc
