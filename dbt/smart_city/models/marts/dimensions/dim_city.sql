{{ 
    config(
        materialized='table'
    )
}}



WITH city_base AS (
    SELECT DISTINCT
        city,
        country,
        latitude,
        longitude,
        city_timezone,
        has_traffic_data
        FROM {{ref('stg_current_weather')}}
)

SELECT * FROM city_base
ORDER BY city