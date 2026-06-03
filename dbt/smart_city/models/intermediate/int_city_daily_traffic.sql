with flow as (
    select * from {{ ref('stg_traffic_flow') }}
),

incidents as (
    select * from {{ ref('stg_traffic_incidents') }}
),

daily_flow as (
    select
        city,
        date_trunc('day', observed_at)::date                               as date_utc,

        -- Speed & congestion
        round(avg(congestion_score)::numeric, 3)                           as avg_congestion_score,
        round(max(congestion_score)::numeric, 3)                           as max_congestion_score,
        round(avg(current_speed_kmh)::numeric, 1)                          as avg_current_speed_kmh,
        round(avg(free_flow_speed_kmh)::numeric, 1)                        as avg_free_flow_speed_kmh,

        -- Share of road segments with noticeable congestion (score > 0.3)
        round(
            count(*) filter (where congestion_score > 0.3)::numeric
            / nullif(count(*), 0),
            3
        )                                                                   as pct_roads_congested,

        count(*) filter (where road_closure = true)                        as road_closure_count,
        count(*)                                                            as flow_observation_count

    from flow
    where city is not null
    group by city, date_trunc('day', observed_at)::date
),

daily_incidents as (
    select
        city,
        date_trunc('day', observed_at)::date                               as date_utc,

        count(*)                                                            as total_incidents,
        count(*) filter (where magnitude_of_delay = 3)                     as major_incidents,
        round(avg(delay_sec)::numeric, 0)                                  as avg_delay_sec

    from incidents
    where city is not null
    group by city, date_trunc('day', observed_at)::date
)

select
    f.city,
    f.date_utc,

    -- Flow metrics
    f.avg_congestion_score,
    f.max_congestion_score,
    f.avg_current_speed_kmh,
    f.avg_free_flow_speed_kmh,
    f.pct_roads_congested,
    f.road_closure_count,
    f.flow_observation_count,

    -- Incident metrics (null when no incidents that day)
    coalesce(i.total_incidents, 0)                                         as total_incidents,
    coalesce(i.major_incidents, 0)                                         as major_incidents,
    i.avg_delay_sec

from daily_flow f
left join daily_incidents i
    on f.city = i.city
    and f.date_utc = i.date_utc
