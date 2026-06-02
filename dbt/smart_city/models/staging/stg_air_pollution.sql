with source as (
    select * from {{ source('airbyte_raw', 'air_pollution') }}
),

renamed as (
    select
        _airbyte_raw_id                         as raw_id,
        _airbyte_extracted_at                   as extracted_at,

        -- AQI (OpenWeather scale 1=Good, 2=Fair, 3=Moderate, 4=Poor, 5=Very Poor)
        (main->>'aqi')::integer                 as aqi,
        case (main->>'aqi')::integer
            when 1 then 'Good'
            when 2 then 'Fair'
            when 3 then 'Moderate'
            when 4 then 'Poor'
            when 5 then 'Very Poor'
        end                                     as aqi_label,

        -- Pollutants (μg/m³)
        (components->>'co')::numeric            as co_ug_m3,
        (components->>'no')::numeric            as no_ug_m3,
        (components->>'no2')::numeric           as no2_ug_m3,
        (components->>'o3')::numeric            as o3_ug_m3,
        (components->>'so2')::numeric           as so2_ug_m3,
        (components->>'pm2_5')::numeric         as pm2_5_ug_m3,
        (components->>'pm10')::numeric          as pm10_ug_m3,
        (components->>'nh3')::numeric           as nh3_ug_m3,

        -- Timestamp
        to_timestamp(dt) at time zone 'UTC'     as observed_at

    from source
)

select * from renamed
