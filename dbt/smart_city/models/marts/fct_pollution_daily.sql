-- Daily air-quality fact: one row per (city, date_utc), rolled up from the hourly
-- pollution facts. Star-schema fact — city_key / date_key join to the dims.
--
-- Incremental (delete+insert on city_date_key): only the current UTC day is mutable
-- (its hourly observations still accumulate); past days are immutable. The 2-day
-- source lookback re-aggregates today (+ yesterday for safety) and replaces by key.

{{ config(
    materialized='incremental',
    unique_key='city_date_key',
    incremental_strategy='delete+insert'
) }}

with daily as (
    select
        city,
        date_utc,
        round(avg(aqi)::numeric, 2)            as avg_aqi,
        max(aqi)                               as max_aqi,
        min(aqi)                               as min_aqi,
        round(avg(pm2_5_ug_m3)::numeric, 2)    as avg_pm2_5_ug_m3,
        round(max(pm2_5_ug_m3)::numeric, 2)    as max_pm2_5_ug_m3,
        round(avg(pm10_ug_m3)::numeric, 2)     as avg_pm10_ug_m3,
        round(max(pm10_ug_m3)::numeric, 2)     as max_pm10_ug_m3,
        round(avg(no2_ug_m3)::numeric, 2)      as avg_no2_ug_m3,
        round(avg(o3_ug_m3)::numeric, 2)       as avg_o3_ug_m3,
        round(avg(co_ug_m3)::numeric, 2)       as avg_co_ug_m3,
        round(avg(so2_ug_m3)::numeric, 2)      as avg_so2_ug_m3,
        count(*) filter (where aqi >= 4)       as hours_poor_air,
        count(*)                               as observation_count
    from {{ ref('int_city_hourly_pollution') }}
    {% if is_incremental() %}
    where date_utc >= (select max(date_utc) - interval '2 days' from {{ this }})
    {% endif %}
    group by city, date_utc
)

select
    {{ dbt_utils.generate_surrogate_key(['city', 'date_utc']) }} as city_date_key,
    {{ dbt_utils.generate_surrogate_key(['city']) }}            as city_key,
    to_char(date_utc, 'YYYYMMDD')::int  as date_key,
    daily.*
from daily
