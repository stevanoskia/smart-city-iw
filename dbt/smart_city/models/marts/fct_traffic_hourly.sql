-- Hourly traffic fact (grain: city × date × hour) for peak-hour analysis.
-- Thin wrap of the hourly flow facts + the star keys (city_key/date_key/hour_utc).
-- NOTE: TomTom flow has no event timestamp — hour_utc ≈ the sync hour, so "peak
-- hour" is really "peak sync hour" (sync-time approximate). Covers traffic cities only.
--
-- Incremental (delete+insert on city_hour_key): a deterministic 1:1 passthrough of the
-- append-only intermediate, so re-pulling only recent hours and replacing by key converges.
-- The 12h observed_at lookback safely covers the intermediate's 6h re-sync window.

{{ config(
    materialized='incremental',
    unique_key='city_hour_key',
    incremental_strategy='delete+insert'
) }}

select
    city_hour_key,
    {{ dbt_utils.generate_surrogate_key(['city']) }} as city_key,    -- FK → dim_city
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
{% if is_incremental() %}
where observed_at > (select max(observed_at) - interval '12 hours' from {{ this }})
{% endif %}
