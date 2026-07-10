-- The headline OBT: one wide row per (city, date), LEFT-joining the three daily
-- facts onto a city×date base so every city appears every day (traffic NULL for
-- weather-only cities). Adds a comfort_index, labels, and rolling trend columns.

with base as (
    select distinct city, date_utc from (
        select city, date_utc from {{ ref('fct_weather_daily') }}
        union
        select city, date_utc from {{ ref('fct_pollution_daily') }}
        union
        select city, date_utc from {{ ref('fct_traffic_daily') }}
    ) u
),

joined as (
    select
        base.city, base.date_utc, d.country,
        w.avg_temp_celsius, w.min_temp_celsius, w.max_temp_celsius,
        w.avg_humidity_pct, w.total_rain_mm, w.dominant_weather_main,
        p.avg_aqi, p.max_aqi, p.avg_pm2_5_ug_m3, p.avg_pm10_ug_m3, p.hours_poor_air,
        t.avg_congestion_score, t.avg_current_speed_kmh, t.total_incidents, t.major_incidents,
        least(greatest(w.avg_temp_celsius / 30.0, 0), 1)    as norm_temp,
        (p.avg_aqi - 1.0) / 4.0                             as norm_aqi,
        1.0 - coalesce(t.avg_congestion_score, 0.5)        as norm_traffic
    from base
    left join {{ ref('dim_city') }}            d on d.city = base.city
    left join {{ ref('fct_weather_daily') }}   w on w.city = base.city and w.date_utc = base.date_utc
    left join {{ ref('fct_pollution_daily') }} p on p.city = base.city and p.date_utc = base.date_utc
    left join {{ ref('fct_traffic_daily') }}   t on t.city = base.city and t.date_utc = base.date_utc
),

scored as (
    select *,
        round((0.4 * norm_temp + 0.4 * (1.0 - norm_aqi) + 0.2 * norm_traffic)::numeric, 3) as comfort_index
    from joined
),

windowed as (
    select *,
        round(avg(comfort_index) over (partition by city order by date_utc
            rows between 6 preceding and current row)::numeric, 3)  as rolling_7d_comfort,
        round(avg(comfort_index) over (partition by city order by date_utc
            rows between 13 preceding and 7 preceding)::numeric, 3) as prior_7d_comfort
    from scored
)

select
    {{ dbt_utils.generate_surrogate_key(['city', 'date_utc']) }} as city_date_key,
    {{ dbt_utils.generate_surrogate_key(['city']) }}            as city_key,
    to_char(date_utc, 'YYYYMMDD')::int                  as date_key,
    city, country, date_utc,
    avg_temp_celsius, min_temp_celsius, max_temp_celsius, avg_humidity_pct, total_rain_mm,
    dominant_weather_main, avg_aqi, max_aqi, avg_pm2_5_ug_m3, avg_pm10_ug_m3, hours_poor_air,
    avg_congestion_score, avg_current_speed_kmh, total_incidents, major_incidents,
    round(norm_temp::numeric, 3)    as norm_temp,
    round(norm_aqi::numeric, 3)     as norm_aqi,
    round(norm_traffic::numeric, 3) as norm_traffic,
    comfort_index,
    comfort_index as livability_score,
    case
        when comfort_index is null then 'No data'
        when comfort_index >= 0.75 then 'Excellent'
        when comfort_index >= 0.50 then 'Good'
        when comfort_index >= 0.25 then 'Fair'
        else 'Poor' end                                 as comfort_index_label,
    case
        when avg_congestion_score is null then 'No data'
        when avg_congestion_score < 0.2   then 'Low'
        when avg_congestion_score < 0.4   then 'Moderate'
        when avg_congestion_score < 0.6   then 'High'
        else 'Severe' end                               as congestion_label,
    (hours_poor_air >= 3)                               as aqi_alert,
    rolling_7d_comfort, prior_7d_comfort,
    case
        when prior_7d_comfort is null then 'Insufficient data'
        when rolling_7d_comfort > prior_7d_comfort + 0.02 then 'Improving'
        when rolling_7d_comfort < prior_7d_comfort - 0.02 then 'Worsening'
        else 'Stable' end                               as comfort_trend
from windowed
