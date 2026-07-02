with source as (
    select * from {{ source('airbyte_raw', 'current_weather') }}
),

renamed as (
    select
        _airbyte_raw_id                                         as raw_id,
        _airbyte_extracted_at                                   as extracted_at,

        -- Location (city injected by Airbyte AddFields from source config;
        -- name is the API's locality/district, kept for reference)
        city                                                    as city,
        name                                                    as api_locality,
        (coord->>'lat')::numeric                               as latitude,
        (coord->>'lon')::numeric                               as longitude,
        (sys->>'country')::text                                as country,

        -- Temperature (Celsius — units=metric set in Airbyte connector)
        (main->>'temp')::numeric                               as temp_celsius,
        (main->>'feels_like')::numeric                         as feels_like_celsius,
        (main->>'temp_min')::numeric                           as temp_min_celsius,
        (main->>'temp_max')::numeric                           as temp_max_celsius,

        -- Atmospheric
        (main->>'humidity')::integer                           as humidity_pct,
        (main->>'pressure')::integer                           as pressure_hpa,
        (main->>'sea_level')::integer                          as sea_level_pressure_hpa,
        (main->>'grnd_level')::integer                         as ground_level_pressure_hpa,

        -- Wind
        (wind->>'speed')::numeric                              as wind_speed_ms,
        (wind->>'deg')::integer                                as wind_direction_deg,
        (wind->>'gust')::numeric                               as wind_gust_ms,

        -- Weather condition
        (weather->0->>'main')::text                            as weather_main,
        (weather->0->>'description')::text                     as weather_description,

        -- Clouds & visibility
        (clouds->>'all')::integer                              as cloudiness_pct,
        visibility                                              as visibility_m,

        -- Precipitation
        (rain->>'1h')::numeric                                 as rain_1h_mm,
        (snow->>'1h')::numeric                                 as snow_1h_mm,

        -- Timestamps
        to_timestamp(dt) at time zone 'UTC'                    as observed_at,
        to_timestamp((sys->>'sunrise')::integer) at time zone 'UTC' as sunrise_at,
        to_timestamp((sys->>'sunset')::integer) at time zone 'UTC'  as sunset_at,
        timezone                                                as timezone_offset_sec

    from source
)

select * from renamed
