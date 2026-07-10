-- Forward-looking severe-weather alerts: one row per (city, forecast_at, alert_type)
-- that breaches a threshold, built from the latest forecast. Closes spec area #3.

with fc as (
    select * from {{ ref('mart_forecast_latest') }}
),

alerts as (
    select city, forecast_at, lead_time_hours, 'High wind' as alert_type,
           wind_speed_ms as trigger_value, 'm/s' as trigger_unit,
           case when wind_speed_ms >= 17.2 then 'Severe' else 'Warning' end as severity
    from fc where wind_speed_ms >= 10.8
    union all
    select city, forecast_at, lead_time_hours, 'Heavy rain',
           precipitation_probability, 'prob', 'Warning'
    from fc where precipitation_probability >= 0.7
    union all
    select city, forecast_at, lead_time_hours, 'Extreme heat',
           temp_celsius, '°C', case when temp_celsius >= 40 then 'Severe' else 'Warning' end
    from fc where temp_celsius >= 35
    union all
    select city, forecast_at, lead_time_hours, 'Extreme cold',
           temp_celsius, '°C', case when temp_celsius <= -10 then 'Severe' else 'Warning' end
    from fc where temp_celsius <= -5
    union all
    select city, forecast_at, lead_time_hours, 'Severe condition',
           null::numeric, weather_main, 'Severe'
    from fc where weather_main in ('Thunderstorm', 'Snow', 'Tornado')
)

select
    {{ dbt_utils.generate_surrogate_key(['city', 'forecast_at', 'alert_type']) }} as alert_key,
    {{ dbt_utils.generate_surrogate_key(['city']) }}                              as city_key,
    to_char(forecast_at::date, 'YYYYMMDD')::int               as date_key,
    city, forecast_at, lead_time_hours, alert_type, severity, trigger_value, trigger_unit
from alerts
