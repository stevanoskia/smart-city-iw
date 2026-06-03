with source as (
    select * from {{ source('postgres_staging', 'stg_air_pollution') }}
),

daily as (
    select
        city,
        date_trunc('day', observed_at)::date                           as date_utc,

        -- AQI aggregates
        round(avg(aqi)::numeric, 2)                                    as avg_aqi,
        max(aqi)                                                        as max_aqi,
        min(aqi)                                                        as min_aqi,

        -- PM2.5 (fine particulate matter)
        round(avg(pm2_5_ug_m3)::numeric, 2)                            as avg_pm2_5_ug_m3,
        round(max(pm2_5_ug_m3)::numeric, 2)                            as max_pm2_5_ug_m3,

        -- PM10 (coarse particulate matter)
        round(avg(pm10_ug_m3)::numeric, 2)                             as avg_pm10_ug_m3,
        round(max(pm10_ug_m3)::numeric, 2)                             as max_pm10_ug_m3,

        -- Other pollutants
        round(avg(no2_ug_m3)::numeric, 2)                              as avg_no2_ug_m3,
        round(avg(o3_ug_m3)::numeric, 2)                               as avg_o3_ug_m3,
        round(avg(co_ug_m3)::numeric, 2)                               as avg_co_ug_m3,
        round(avg(so2_ug_m3)::numeric, 2)                              as avg_so2_ug_m3,

        -- Hours where air quality was Poor or worse (aqi >= 4)
        -- Used by mart_aqi_monitoring for the 3-consecutive-hour alert rule
        count(*) filter (where aqi >= 4)                               as hours_poor_air,

        count(*)                                                        as observation_count

    from source
    where city is not null
    group by city, date_trunc('day', observed_at)::date
)

select * from daily
