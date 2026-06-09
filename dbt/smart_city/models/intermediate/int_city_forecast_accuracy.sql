-- Forecast accuracy: every past prediction scored against what actually happened.
-- Joins the durable forecast issue-history (predictions) to the observed hourly
-- weather facts (actuals) on (city, hour). One row per scoreable prediction, with
-- error metrics sliced by lead time — so you can see accuracy degrade as the
-- prediction looked further ahead.

with predictions as (
    select *
    from {{ ref('int_city_weather_forecast') }}
    -- target time has passed (scoreable) AND was a genuine forward prediction
    -- (lead >= 0; OpenWeather sometimes includes the just-started slot). Compare
    -- in naive UTC to match forecast_at.
    where forecast_at < (now() at time zone 'UTC')
      and lead_time_hours >= 0
),

-- Observed weather aggregated to one row per (city, hour) so it lines up with the
-- forecast's 3-hour target marks.
actuals as (
    select
        city,
        date_trunc('hour', observed_at)         as obs_hour,
        round(avg(temp_celsius)::numeric, 2)    as actual_temp_celsius,
        -- rain_1h is omitted by OpenWeather when dry → coalesce to 0 so the rain
        -- metrics are never NULL.
        round(coalesce(sum(rain_1h_mm), 0)::numeric, 2) as actual_rain_mm,
        mode() within group (order by weather_main) as actual_weather_main
    from {{ ref('int_city_hourly_weather') }}
    group by city, date_trunc('hour', observed_at)
)

select
    p.forecast_key,
    p.city,
    p.forecast_at,
    p.issued_at,
    p.lead_time_hours,
    p.lead_time_bucket,

    -- Temperature: predicted vs actual
    p.temp_celsius                                          as predicted_temp_celsius,
    a.actual_temp_celsius,
    round((p.temp_celsius - a.actual_temp_celsius)::numeric, 2)      as temp_bias,
    round(abs(p.temp_celsius - a.actual_temp_celsius)::numeric, 2)   as temp_abs_error,

    -- Rain: predicted probability vs whether it actually rained
    p.precipitation_probability,
    (p.precipitation_probability >= 0.5)                    as predicted_rain,
    (a.actual_rain_mm > 0)                                  as actually_rained,
    ((p.precipitation_probability >= 0.5) = (a.actual_rain_mm > 0)) as rain_correct,
    a.actual_rain_mm,

    -- Condition: predicted vs observed dominant condition
    p.weather_main                                          as predicted_weather_main,
    a.actual_weather_main,
    (p.weather_main = a.actual_weather_main)                as condition_correct,

    -- 1/0 helper columns so BI tools get a hit-rate via a plain AVERAGE()
    -- (AVG of 1s/0s = proportion correct). temp_within_2c = within ±2°C tolerance.
    (((p.precipitation_probability >= 0.5) = (a.actual_rain_mm > 0)))::int as rain_correct_int,
    ((p.weather_main = a.actual_weather_main))::int                        as condition_correct_int,
    ((abs(p.temp_celsius - a.actual_temp_celsius) <= 2))::int             as temp_within_2c
from predictions p
join actuals a
    on a.city = p.city
    and a.obs_hour = date_trunc('hour', p.forecast_at)
