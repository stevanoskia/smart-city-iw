-- Hourly traffic fact (grain: city × date × hour) for peak-hour analysis.
-- Thin wrap of the hourly flow facts + the star keys (city_key/date_key/hour_utc).
-- NOTE: TomTom flow has no event timestamp — hour_utc ≈ the sync hour, so "peak
-- hour" is really "peak sync hour" (sync-time approximate). Covers traffic cities only.

select
    city_hour_key,
    md5(city)                           as city_key,    -- FK → dim_city
    to_char(date_utc, 'YYYYMMDD')::int  as date_key,     -- FK → dim_date
    hour_utc,                                            -- FK → dim_hour
    city,
    date_utc,
    observed_at,
    congestion_score,
    current_speed_kmh,
    free_flow_speed_kmh,
    road_closure,
    confidence
from {{ ref('int_city_hourly_traffic_flow') }}
