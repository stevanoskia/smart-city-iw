-- Daily per-city air-quality aggregates, rolled up from the hourly facts.
-- Dedupe already happened upstream in int_city_hourly_pollution, so this is a
-- pure aggregation to one row per (city, date_utc) with a surrogate key.

with daily as (
    select
        city,
        date_utc,

        -- AQI aggregates (OpenWeather scale 1=Good ... 5=Very Poor)
        round(avg(aqi)::numeric, 2)                     as avg_aqi,
        max(aqi)                                        as max_aqi,
        min(aqi)                                        as min_aqi,

        -- Particulate matter
        round(avg(pm2_5_ug_m3)::numeric, 2)            as avg_pm2_5_ug_m3,
        round(max(pm2_5_ug_m3)::numeric, 2)            as max_pm2_5_ug_m3,
        round(avg(pm10_ug_m3)::numeric, 2)             as avg_pm10_ug_m3,
        round(max(pm10_ug_m3)::numeric, 2)             as max_pm10_ug_m3,

        -- Other pollutants
        round(avg(no2_ug_m3)::numeric, 2)              as avg_no2_ug_m3,
        round(avg(o3_ug_m3)::numeric, 2)               as avg_o3_ug_m3,
        round(avg(co_ug_m3)::numeric, 2)               as avg_co_ug_m3,
        round(avg(so2_ug_m3)::numeric, 2)              as avg_so2_ug_m3,

        -- Hours where air quality was Poor or worse (aqi >= 4)
        count(*) filter (where aqi >= 4)               as hours_poor_air,

        count(*)                                        as observation_count
    from {{ ref('int_city_hourly_pollution') }}
    group by city, date_utc
)

select
    md5(city || '|' || date_utc::text)                  as city_date_key,
    daily.*
from daily
