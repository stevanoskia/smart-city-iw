{{config(materialized='table')}}

--mart_forecast_latest: most recent forecast for each city and future time slot
--filters out past forecast and keeps only the latest issued prediction per slot
--grain: one row per city per 3 hour forecast slot

WITH latest AS (
    SELECT *,
            ROW_NUMBER() OVER (
                PARTITION BY city, forecast_at
                ORDER BY issued_at DESC
            ) AS rn
        FROM {{ref('int_city_weather_forecast')}}
        WHERE forecast_at>NOW()
       
)

SELECT
    forecast_key,
    city,
    forecast_at,
    issued_at,
    lead_time_hours,
    lead_time_bucket,
    forecast_date_utc,
    forecast_hour_utc,

--Predicted measures
    temp_celsius,
    feels_like_celsius,
    precipitation_probability,
    weather_main,
    wind_speed_ms,
    humidity_pct,
    rain_3h_mm

FROM latest
WHERE rn=1
ORDER BY city, forecast_at