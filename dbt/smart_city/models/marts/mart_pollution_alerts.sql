-- Air-quality alerts: one row per (city, observed_at, alert_type) that breaches a
-- threshold, built from real hourly readings in fct_pollution_hourly (not a forecast —
-- unlike weather, pollution here is measured, not predicted). Mirrors mart_weather_alerts.
-- Thresholds: AQI 4-5 = OpenWeather's own "Poor"/"Very Poor" bands; PM2.5/PM10/NO2 use
-- WHO 24h/1h guideline ballpark figures — easy to retune later if the mentor wants
-- stricter/looser bands.

{{ config(
    materialized='incremental',
    unique_key='alert_key',
    incremental_strategy='delete+insert'
) }}

with pol as (
    -- Incremental (delete+insert on alert_key): built from measured, immutable hourly
    -- readings, so past alerts never change. The 12h observed_at lookback recomputes only
    -- recent hours and replaces their alerts by key (matches fct_pollution_hourly's window).
    select * from {{ ref('fct_pollution_hourly') }}
    {% if is_incremental() %}
    where observed_at > (select max(observed_at) - interval '12 hours' from {{ this }})
    {% endif %}
),

alerts as (
    select city, observed_at, 'Poor air quality' as alert_type,
           aqi::numeric as trigger_value, 'AQI (1-5)' as trigger_unit,
           case when aqi = 5 then 'Severe' else 'Warning' end as severity
    from pol where aqi >= 4
    union all
    select city, observed_at, 'High PM2.5',
           pm2_5_ug_m3, 'µg/m³', case when pm2_5_ug_m3 >= 50 then 'Severe' else 'Warning' end
    from pol where pm2_5_ug_m3 >= 25
    union all
    select city, observed_at, 'High PM10',
           pm10_ug_m3, 'µg/m³', case when pm10_ug_m3 >= 100 then 'Severe' else 'Warning' end
    from pol where pm10_ug_m3 >= 50
    union all
    select city, observed_at, 'High NO2',
           no2_ug_m3, 'µg/m³', case when no2_ug_m3 >= 400 then 'Severe' else 'Warning' end
    from pol where no2_ug_m3 >= 200
)

select
    {{ dbt_utils.generate_surrogate_key(['city', 'observed_at', 'alert_type']) }} as alert_key,
    {{ dbt_utils.generate_surrogate_key(['city']) }}                              as city_key,
    to_char(observed_at::date, 'YYYYMMDD')::int                                   as date_key,
    city, observed_at, alert_type, severity, trigger_value, trigger_unit
from alerts
