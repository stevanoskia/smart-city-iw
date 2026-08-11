-- Incident-grain traffic fact: ONE ROW PER INCIDENT, not per observation or per day.
--
-- WHY THIS MODEL EXISTS
-- fct_traffic_daily.total_incidents is count(distinct incident_id) per city-day, which is
-- correct at ITS grain — but a BI measure summing it across days yields incident-DAYS, not
-- incidents (the Power BI card read 141K against ~60K real incidents). Counting incidents
-- needs a model at incident grain; this is it.
--
-- WHY NOT JUST count(distinct incident_id)
-- TomTom's incident_id embeds a `TTI-<uuid>` prefix that it ROTATES roughly weekly: each
-- uuid covers a distinct, non-overlapping date range, and no id is ever seen under two
-- uuids. The same physical roadworks is therefore re-issued a brand-new id every week, so a
-- plain distinct count still over-counts long-running incidents (96,872 vs ~60,641 here).
-- Identity has to come from WHERE the incident is, not from the vendor's id.
--
-- IDENTITY = (city, road_from, road_to, feature_type) + a session number
-- Location alone would merge a June jam and an August jam on the same stretch into one
-- incident, so the location is "sessionised": a new incident begins when the location goes
-- unobserved.
--
-- ⚠ SESSIONS COUNT COLLECTION DAYS, NOT CALENDAR DAYS
-- Airflow only runs while the dev machine is on — only ~58% of calendar days were collected.
-- Splitting on a wall-clock gap would start a new incident every time the PIPELINE was down
-- rather than when the incident ended (that approach swung the count 85K -> 42K on threshold
-- choice alone). So each city gets a dense index of the days it ACTUALLY collected, and a
-- session breaks only when the location is missing on a day we genuinely looked.
--
-- started_at IS DELIBERATELY NOT USED for identity or date_key: it is unstable (up to 20
-- distinct values for a single id) and 18.5% of incidents started before we first observed
-- them (earliest 2020), which would scatter rows outside the observation window. date_key
-- comes from first observation instead. Both timestamps are carried for reference.
--
-- materialized='table' ON PURPOSE: session boundaries are a window over each location's full
-- history, so an incremental recent-rows batch would compute them wrong at the boundary —
-- the same reasoning that keeps mart_city_daily and mart_temperature_trends as tables.

{{ config(materialized='table') }}

{% set gap = var('incident_session_gap_days', 1) %}

with observations as (
    select
        city, road_from, road_to, feature_type, category_id,
        incident_id, observed_at, date_utc,
        started_at, ends_at,
        magnitude_of_delay, delay_sec, length_m
    from {{ ref('int_city_hourly_traffic_incidents') }}
    where city is not null
      and road_from is not null
      and road_to is not null
),

-- Per-city dense index of days we actually collected. day_ix advances by exactly 1 per
-- collected day, so a difference > gap means the location was absent on a day we looked.
collection_days as (
    select
        city,
        date_utc,
        dense_rank() over (partition by city order by date_utc) as day_ix
    from (select distinct city, date_utc from observations) d
),

-- One row per location per collected day: the resolution session breaks are detected at.
location_days as (
    select distinct
        o.city, o.road_from, o.road_to, o.feature_type,
        o.date_utc, c.day_ix
    from observations o
    join collection_days c
      on c.city = o.city and c.date_utc = o.date_utc
),

flagged as (
    select
        *,
        case
            when lag(day_ix) over (
                     partition by city, road_from, road_to, feature_type
                     order by day_ix
                 ) is null
              or day_ix - lag(day_ix) over (
                     partition by city, road_from, road_to, feature_type
                     order by day_ix
                 ) > {{ gap }}
            then 1 else 0
        end as is_new_session
    from location_days
),

sessioned as (
    select
        *,
        sum(is_new_session) over (
            partition by city, road_from, road_to, feature_type
            order by day_ix
            rows unbounded preceding
        ) as session_no
    from flagged
),

-- Attach the session number back to every underlying observation.
observations_sessioned as (
    select o.*, s.session_no
    from observations o
    join sessioned s
      on  s.city         = o.city
      and s.road_from    = o.road_from
      and s.road_to      = o.road_to
      and s.feature_type = o.feature_type
      and s.date_utc     = o.date_utc
)

select
    {{ dbt_utils.generate_surrogate_key([
        'city', 'road_from', 'road_to', 'feature_type', 'session_no'
    ]) }}                                            as incident_key,
    {{ dbt_utils.generate_surrogate_key(['city']) }} as city_key,       -- FK → dim_city
    to_char(min(date_utc), 'YYYYMMDD')::int          as date_key,       -- FK → dim_date (first seen)

    city,
    road_from,
    road_to,
    feature_type,
    max(category_id)                                 as category_id,
    session_no,

    -- Lifecycle, as WE observed it
    min(observed_at)                                 as first_observed_at,
    max(observed_at)                                 as last_observed_at,
    min(date_utc)                                    as first_date_utc,
    max(date_utc)                                    as last_date_utc,
    count(distinct date_utc)                         as days_observed,
    count(*)                                         as observation_count,

    -- How many rotating TomTom ids this one incident absorbed. >1 is the fingerprint of the
    -- weekly id rotation described in the header, and the reason this model exists.
    count(distinct incident_id)                      as tomtom_id_count,

    -- Severity / impact, at their worst over the incident's life
    max(magnitude_of_delay)                          as max_magnitude_of_delay,
    (max(magnitude_of_delay) = 3)                    as is_major,
    max(delay_sec)                                   as max_delay_sec,
    round(avg(delay_sec)::numeric, 0)                as avg_delay_sec,
    max(length_m)                                    as max_length_m,

    -- TomTom's own timestamps — reference only, see the header for why they aren't identity
    min(started_at)                                  as min_started_at,
    max(ends_at)                                     as max_ends_at

from observations_sessioned
group by city, road_from, road_to, feature_type, session_no
