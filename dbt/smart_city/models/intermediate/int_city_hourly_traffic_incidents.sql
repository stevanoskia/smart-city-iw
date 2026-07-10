-- Durable hourly per-city traffic-incident facts (one incident per sync).
-- Incremental + append-only: accumulates history forever, independent of
-- staging (raw) retention. The daily rollup is built from this.
--
-- TomTom incidents have no event timestamp — observed_at is the Airbyte sync
-- time, so the grain is (city, incident_id, observed_at). Rows without an
-- incident_id are excluded: some TomTom features (and all pre-`fields`-fix rows)
-- lack an id and can't be identified or keyed.

{{ config(
    materialized='incremental',
    unique_key='city_incident_key',
    incremental_strategy='delete+insert'
) }}

with new_rows as (
    select *
    from {{ ref('stg_traffic_incidents') }}
    where city is not null
      and incident_id is not null
    {% if is_incremental() %}
      -- 6h lookback absorbs late/re-synced rows; the dedupe + delete+insert below
      -- makes reprocessing idempotent (no duplicates land).
      and extracted_at > (select max(extracted_at) - interval '6 hours' from {{ this }})
    {% endif %}
),

deduped as (
    select *,
           row_number() over (
               partition by city, incident_id, observed_at   -- one row per incident per sync
               order by extracted_at desc
           ) as _rn
    from new_rows
)

select
    {{ dbt_utils.generate_surrogate_key(['city', 'incident_id', 'observed_at']) }}  as city_incident_key,
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
