with source as (
    select * from {{ source('airbyte_raw', 'weather_forecast') }}
),

renamed as (
    select
        _airbyte_raw_id                                         as raw_id,
        _airbyte_extracted_at                                   as extracted_at,

        -- Forecast timestamp
        to_timestamp(dt) at time zone 'UTC'                    as forecast_at,
        dt_txt                                                  as forecast_dt_txt,

        -- Rain probability (0.0 – 1.0 → stored as decimal)
        pop                                                     as precipitation_probability,
        round((pop * 100)::numeric, 0)::integer                as precipitation_probability_pct,

        -- Temperature (Celsius — units=metric)
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
        (rain->>'3h')::numeric                                 as rain_3h_mm,
        (snow->>'3h')::numeric                                 as snow_3h_mm,

        -- Day/night indicator
        (sys->>'pod')::text                                    as day_or_night  -- 'd' or 'n'

    from source
)

select * from renamed
