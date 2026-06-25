with source as (
    select * from {{ source('airbyte_raw', 'traffic_incidents') }}
),

renamed as (
    select
        _airbyte_raw_id                                                     as raw_id,
        _airbyte_extracted_at                                               as extracted_at,

        -- Location (injected by Airbyte AddFields from source config)
        city                                                                as city,

        -- Incident identity
        (properties->>'id')::text                                           as incident_id,
        type                                                                as feature_type,

        -- Location description
        (properties->>'from')::text                                         as road_from,
        (properties->>'to')::text                                           as road_to,

        -- Timing
        (properties->>'startTime')::timestamp with time zone               as started_at,
        (properties->>'endTime')::timestamp with time zone                 as ends_at,
        (properties->>'timeValidity')::text                                 as time_validity,

        -- Severity
        (properties->>'magnitudeOfDelay')::integer                         as magnitude_of_delay,
        -- 0=Unknown, 1=Minor, 2=Moderate, 3=Major, 4=Undefined
        case (properties->>'magnitudeOfDelay')::integer
            when 0 then 'Unknown'
            when 1 then 'Minor'
            when 2 then 'Moderate'
            when 3 then 'Major'
            when 4 then 'Undefined'
        end                                                                 as delay_severity,

        -- Impact
        (properties->>'delay')::integer                                     as delay_sec,
        (properties->>'length')::numeric                                    as length_m,
        (properties->>'iconCategory')::integer                              as category_id,
        (properties->>'numberOfReports')::integer                           as number_of_reports,
        (properties->>'probabilityOfOccurrence')::text                      as probability,

        -- Geometry (coordinates as JSONB for mapping)
        geometry                                                            as geometry,

        -- City metadata (added by ingest.py)
        country                                                             as country,

        -- Sync timestamp used as observed_at (TomTom incidents have no dt field).
        -- Cast to naive UTC so downstream DATE_TRUNC is always true UTC, not session-local.
        (_airbyte_extracted_at at time zone 'UTC')                          as observed_at

    from source
)

select * from renamed
