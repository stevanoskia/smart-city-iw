-- Durable hourly per-city traffic-incident facts (one raw incident feature per sync).
-- Incremental + append-only: accumulates history forever, independent of
-- airbyte_raw retention. The daily rollup is built from this.
--
-- ⚠ UPSTREAM LIMITATION: the TomTom incidents connector currently returns only
-- `iconCategory` + geometry per feature — NOT id/magnitudeOfDelay/delay/from/to/
-- startTime. So incident_id (and the other detail fields below) are always NULL
-- until the connector's `fields` parameter is fixed. With no incident id to key
-- or dedupe on, the grain is one raw feature per sync, keyed on raw_id (the only
-- guaranteed-unique, non-null identifier). Once the connector returns real
-- incident ids, switch the key/dedupe back to (city, incident_id, observed_at).

{{ config(
    materialized='incremental',
    unique_key='city_incident_key',
    incremental_strategy='delete+insert'
) }}

with new_rows as (
    select *
    from {{ ref('stg_traffic_incidents') }}
    where city is not null
    {% if is_incremental() %}
      -- 6h lookback absorbs late/re-synced rows; raw_id-keyed delete+insert below
      -- makes reprocessing idempotent (no duplicates land).
      and extracted_at > (select max(extracted_at) - interval '6 hours' from {{ this }})
    {% endif %}
),

deduped as (
    select *,
           row_number() over (
               partition by raw_id                  -- raw_id is unique; one row per raw feature
               order by extracted_at desc
           ) as _rn
    from new_rows
)

select
    md5(raw_id::text)                               as city_incident_key,
    city,
    incident_id,
    observed_at,
    date_trunc('day', observed_at)::date            as date_utc,   -- for daily rollups
    extract(hour from observed_at)::int             as hour_utc,   -- for time-of-day analysis
    extracted_at,

    -- Incident identity / location
    feature_type,
    road_from,
    road_to,

    -- Timing
    started_at,
    ends_at,

    -- Severity & impact
    magnitude_of_delay,
    delay_severity,
    delay_sec,
    length_m,
    category_id,
    number_of_reports
from deduped
where _rn = 1
