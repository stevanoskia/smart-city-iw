-- City dimension: one row per city, with the surrogate key every fact joins on.
-- DERIVED from observed data (no seed):
--   • city list + country  → the durable hourly weather facts (survive raw pruning)
--   • latitude / longitude → the weather staging view (only place coords land)
--   • coverage flags       → whether the city appears in the weather / traffic facts
-- Trade-off: the dimension reflects whatever the pipeline actually collected. A city
-- only exists here once it has weather data; has_traffic_data flips on the moment any
-- traffic row appears (see the Madrid note in docs/marts_implementation_plan.md §6.2).

with weather_cities as (
    -- membership + country from intermediate
    select
        city,
        mode() within group (order by country) as country
    from {{ ref('int_city_hourly_weather') }}
    where city is not null
    group by city
),

coords as (
    -- lat/lon from staging 
    select
        city,
        round(avg(latitude)::numeric, 4)  as latitude,
        round(avg(longitude)::numeric, 4) as longitude
    from {{ ref('stg_current_weather') }}
    where city is not null
    group by city
),

traffic_cities as (
    select distinct city
    from {{ ref('int_city_hourly_traffic_flow') }}
    where city is not null
)

select
    md5(w.city)              as city_key,
    w.city,
    w.country,
    c.latitude,
    c.longitude,
    true                     as has_weather_data,   -- derived base = weather cities
    (t.city is not null)     as has_traffic_data     -- true iff city appears in traffic facts
from weather_cities w
left join coords c         on c.city = w.city
left join traffic_cities t on t.city = w.city
