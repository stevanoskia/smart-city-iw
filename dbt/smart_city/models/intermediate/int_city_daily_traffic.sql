-- Daily per-city traffic aggregates, rolled up from the hourly facts.
-- Dedupe already happened upstream in the hourly flow/incident models, so this
-- is a pure aggregation to one row per (city, date_utc).
-- NOTE: observed_at is the Airbyte sync time (TomTom has no real event
-- timestamp), so "daily" aggregates that day's hourly sync snapshots.

with daily_flow as (
    select
        city,
        date_utc,

        round(avg(congestion_score)::numeric, 3)          as avg_congestion_score,
        round(max(congestion_score)::numeric, 3)          as max_congestion_score,
        round(avg(current_speed_kmh)::numeric, 1)         as avg_current_speed_kmh,
        round(avg(free_flow_speed_kmh)::numeric, 1)       as avg_free_flow_speed_kmh,

        -- Share of snapshots with noticeable congestion (score > 0.3)
        round(
            count(*) filter (where congestion_score > 0.3)::numeric
            / nullif(count(*), 0),
            3
        )                                                  as pct_congested_snapshots,

        count(*) filter (where road_closure = true)       as road_closure_count,
        count(*)                                           as flow_observation_count
    from {{ ref('int_city_hourly_traffic_flow') }}
    group by city, date_utc
),

daily_incidents as (
    select
        city,
        date_utc,

        count(distinct incident_id)                                     as total_incidents,
        count(distinct incident_id) filter (where magnitude_of_delay = 3) as major_incidents,
        round(avg(delay_sec)::numeric, 0)                              as avg_delay_sec
    from {{ ref('int_city_hourly_traffic_incidents') }}
    group by city, date_utc
)

select
    md5(f.city || '|' || f.date_utc::text)              as city_date_key,
    f.city,
    f.date_utc,

    -- Flow metrics
    f.avg_congestion_score,
    f.max_congestion_score,
    f.avg_current_speed_kmh,
    f.avg_free_flow_speed_kmh,
    f.pct_congested_snapshots,
    f.road_closure_count,
    f.flow_observation_count,

    -- Incident metrics (0 when no incidents that day)
    coalesce(i.total_incidents, 0)                       as total_incidents,
    coalesce(i.major_incidents, 0)                       as major_incidents,
    i.avg_delay_sec
from daily_flow f
left join daily_incidents i
    on f.city = i.city
    and f.date_utc = i.date_utc
