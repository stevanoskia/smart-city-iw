-- Durable hourly per-city traffic-flow facts (one snapshot per clock hour).
-- Incremental + append-only: accumulates clean, deduped history forever,
-- independent of staging (raw) retention. The daily rollup is built from this.
-- NOTE: TomTom flow has no event timestamp — observed_at is the Airbyte sync
-- time, so a clock hour = the latest sync snapshot in that hour.

{{ config(
    materialized='incremental',
    unique_key='city_hour_key',
    incremental_strategy='delete+insert'
) }}

with new_rows as (
    select *
    from {{ ref('stg_traffic_flow') }}
    where city is not null
    {% if is_incremental() %}
      -- 6h lookback absorbs late/re-synced rows; the dedupe + delete+insert below
      -- makes reprocessing idempotent (no duplicates land).
      and extracted_at > (select max(extracted_at) - interval '6 hours' from {{ this }})
    {% endif %}
),

deduped as (
    select *,
           row_number() over (
               partition by city, date_trunc('hour', observed_at)   -- one snapshot per clock hour
               order by observed_at desc, extracted_at desc          -- latest snapshot in the hour wins
           ) as _rn
    from new_rows
)

select
    {{ dbt_utils.generate_surrogate_key(['city', "date_trunc('hour', observed_at)"]) }} as city_hour_key,
    city,
    observed_at,
    date_trunc('day', observed_at)::date            as date_utc,   -- for daily rollups
    extract(hour from observed_at)::int             as hour_utc,   -- for time-of-day analysis
    extracted_at,

    -- Road classification
    road_class,
    road_closure,

    -- Speed (km/h)
    current_speed_kmh,
    free_flow_speed_kmh,

    -- Travel time (seconds)
    current_travel_time_sec,
    free_flow_travel_time_sec,

    -- Congestion (0 = free flow, 1 = fully congested)
    congestion_score,

    -- Data quality
    confidence
from deduped
where _rn = 1
