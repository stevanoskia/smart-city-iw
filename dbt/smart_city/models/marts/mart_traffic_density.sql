with source as (
    select * from {{ ref('int_city_daily_traffic') }}
),

with_labels as (
    select
        city,
        date_utc,

        -- Speed & congestion metrics
        avg_congestion_score,
        max_congestion_score,
        avg_current_speed_kmh,
        avg_free_flow_speed_kmh,
        pct_roads_congested,
        road_closure_count,
        total_incidents,
        major_incidents,
        avg_delay_sec,
        flow_observation_count,

        -- Human-readable congestion level
        case
            when avg_congestion_score < 0.2  then 'Low'
            when avg_congestion_score < 0.4  then 'Moderate'
            when avg_congestion_score < 0.6  then 'High'
            else                                  'Severe'
        end                                                         as congestion_label,

        -- Ratio of actual speed to free-flow speed (1.0 = perfectly free flow)
        round(
            avg_current_speed_kmh / nullif(avg_free_flow_speed_kmh, 0)::numeric,
            2
        )                                                           as speed_ratio,

        -- 7-day rolling average congestion
        round(
            avg(avg_congestion_score) over (
                partition by city order by date_utc
                rows between 6 preceding and current row
            )::numeric, 3
        )                                                           as rolling_7d_avg_congestion

    from source
)

select * from with_labels
