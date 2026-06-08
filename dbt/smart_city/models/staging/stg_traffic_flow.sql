with source as (
    select * from {{ source('airbyte_raw', 'traffic_flow') }}
),

renamed as (
    select
        _airbyte_raw_id                                             as raw_id,
        _airbyte_extracted_at                                       as extracted_at,

        -- Location (injected by Airbyte AddFields from source config)
        city                                                        as city,

        -- Road classification (FRC0=motorway ... FRC7=local road)
        frc                                                         as road_class,
        "roadClosure"                                               as road_closure,

        -- Speed (km/h)
        "currentSpeed"                                              as current_speed_kmh,
        "freeFlowSpeed"                                             as free_flow_speed_kmh,

        -- Travel time (seconds)
        "currentTravelTime"                                         as current_travel_time_sec,
        "freeFlowTravelTime"                                        as free_flow_travel_time_sec,

        -- Congestion score: 0 = free flow, 1 = fully congested
        round(
            1.0 - ("currentSpeed"::numeric / nullif("freeFlowSpeed", 0)::numeric),
            2
        )                                                           as congestion_score,

        -- Data quality
        confidence                                                  as confidence,

        -- Sync timestamp used as observed_at (TomTom flow has no dt field)
        _airbyte_extracted_at                                       as observed_at

    from source
)

select * from renamed
