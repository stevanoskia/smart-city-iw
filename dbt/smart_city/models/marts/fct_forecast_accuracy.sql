-- Forecast accuracy fact: joins each past prediction to what actually happened.
-- predictions = forecast issuances whose target time has now passed;
-- actuals = the observed hourly weather at that target hour.

with predictions as (
    select *
    from {{ ref('int_city_weather_forecast') }}
    where forecast_at < (now() at time zone 'UTC')
      and lead_time_hours >= 0
),

actuals as (
    select
        city,
        date_trunc('hour', observed_at)                 as obs_hour,
        round(avg(temp_celsius)::numeric, 2)            as actual_temp_celsius,
        round(coalesce(sum(rain_1h_mm), 0)::numeric, 2) as actual_rain_mm,
        mode() within group (order by weather_main)     as actual_weather_main
    from {{ ref('int_city_hourly_weather') }}
    group by city, date_trunc('hour', observed_at)
)

select
    p.forecast_key,
    md5(p.city)                                            as city_key,
    to_char(p.forecast_at::date, 'YYYYMMDD')::int          as date_key,
    p.city,
    p.forecast_at,
    p.issued_at,
    p.lead_time_hours,
    p.lead_time_bucket,
    p.temp_celsius                                         as predicted_temp_celsius,
    a.actual_temp_celsius,
    round((p.temp_celsius - a.actual_temp_celsius)::numeric, 2)    as temp_bias,
    round(abs(p.temp_celsius - a.actual_temp_celsius)::numeric, 2) as temp_abs_error,
    p.precipitation_probability,
    (p.precipitation_probability >= 0.5)                  as predicted_rain,
    (a.actual_rain_mm > 0)                                as actually_rained,
    ((p.precipitation_probability >= 0.5) = (a.actual_rain_mm > 0)) as rain_correct,
    a.actual_rain_mm,
    p.weather_main                                        as predicted_weather_main,
    a.actual_weather_main,
    (p.weather_main = a.actual_weather_main)              as condition_correct,
    (((p.precipitation_probability >= 0.5) = (a.actual_rain_mm > 0)))::int as rain_correct_int,
    ((p.weather_main = a.actual_weather_main))::int                        as condition_correct_int,
    ((abs(p.temp_celsius - a.actual_temp_celsius) <= 2))::int             as temp_within_2c
from predictions p
join actuals a
    on a.city = p.city
    and a.obs_hour = date_trunc('hour', p.forecast_at)
