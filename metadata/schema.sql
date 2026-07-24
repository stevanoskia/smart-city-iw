-- ============================================================================
-- config schema — metadata-driven pipeline configuration (single source of truth)
-- ============================================================================
-- Everything the pipeline needs to know about WHAT to ingest, WHERE it lands,
-- HOW each field is parsed, and the quality RULES that gate it, lives here — so
-- adding a source/stream/city/field is an INSERT, not a code change.
--
-- Idempotent: safe to re-run (create ... if not exists, drop-then-create triggers).
-- Checked into git so a rebuilt machine can recreate the schema; the row data is
-- (re)loaded by metadata/seed_config.py.
--
--   psql "host=localhost dbname=smart_city user=postgres" -f metadata/schema.sql
-- ============================================================================

create schema if not exists config;

-- ── updated_at auto-touch (feeds the optional reconcile watermark) ───────────
create or replace function config.set_updated_at() returns trigger as $$
begin
    new.updated_at := now();
    return new;
end;
$$ language plpgsql;

-- ── sources — one row per API (WHAT/WHERE/WHEN at the connector level) ────────
create table if not exists config.sources (
    source_id      serial primary key,
    source_name    text not null unique,                 -- 'openweather', 'tomtom'
    connector_name text not null,                        -- Airbyte connector display name
    api_key_env    text,                                 -- env var holding the API key value
    api_key_field  text,                                 -- connector config key for the key ('appid'/'api_key')
    schedule_cron  text,                                 -- informational; Airflow owns scheduling
    is_active      boolean not null default true,
    created_at     timestamptz not null default now(),
    updated_at     timestamptz not null default now()
);
-- Idempotent add for DBs created before api_key_field existed.
alter table config.sources add column if not exists api_key_field text;
drop trigger if exists sources_touch on config.sources;
create trigger sources_touch before update on config.sources
    for each row execute function config.set_updated_at();

-- ── streams — one row per stream per source (WHERE it lands, sync mode) ───────
create table if not exists config.streams (
    stream_id     serial primary key,
    source_id     integer not null references config.sources(source_id) on delete cascade,
    stream_name   text not null,                         -- 'current_weather'
    target_schema text not null default 'staging',       -- raw landing schema (Airbyte)
    target_table  text not null,                         -- raw table Airbyte writes
    sync_mode     text not null default 'full_refresh_append',
    is_active     boolean not null default true,
    created_at    timestamptz not null default now(),
    updated_at    timestamptz not null default now(),
    unique (source_id, stream_name)
);
drop trigger if exists streams_touch on config.streams;
create trigger streams_touch before update on config.streams
    for each row execute function config.set_updated_at();

-- ── locations — one row per city ─────────────────────────────────────────────
create table if not exists config.locations (
    location_id serial primary key,
    city        text not null unique,
    latitude    numeric not null,
    longitude   numeric not null,
    is_active   boolean not null default true,
    updated_at  timestamptz not null default now()
);
drop trigger if exists locations_touch on config.locations;
create trigger locations_touch before update on config.locations
    for each row execute function config.set_updated_at();

-- ── source_locations — which cities each source ingests (+ per-source params) ─
-- Weather covers all cities with just lat/lon; TomTom covers a subset and needs
-- a bounding box (min/max lat/lon). One join row per (source, city).
create table if not exists config.source_locations (
    source_id   integer not null references config.sources(source_id)   on delete cascade,
    location_id integer not null references config.locations(location_id) on delete cascade,
    min_lat     numeric,
    min_lon     numeric,
    max_lat     numeric,
    max_lon     numeric,
    is_active   boolean not null default true,
    updated_at  timestamptz not null default now(),
    primary key (source_id, location_id)
);
drop trigger if exists source_locations_touch on config.source_locations;
create trigger source_locations_touch before update on config.source_locations
    for each row execute function config.set_updated_at();

-- ── field_mappings — THE contract: expected field → typed column (HOW) ────────
-- source_expr is a SQL expression over the raw Airbyte row (JSON path, quoted
-- camelCase column, function, or a full computed/CASE expression). data_type is
-- an optional cast; NULL means source_expr already yields the final type. The
-- generic staging engine emits:  source_expr [::data_type] as target_column
create table if not exists config.field_mappings (
    mapping_id    serial primary key,
    stream_id     integer not null references config.streams(stream_id) on delete cascade,
    target_column text not null,
    source_expr   text not null,
    data_type     text,
    is_required   boolean not null default false,        -- true → validation gate stops on absence/all-NULL
    is_active     boolean not null default true,         -- false → engine omits the column (API dropped it)
    ordinal       integer not null default 0,            -- output column order
    description   text,
    updated_at    timestamptz not null default now(),
    unique (stream_id, target_column)
);
drop trigger if exists field_mappings_touch on config.field_mappings;
create trigger field_mappings_touch before update on config.field_mappings
    for each row execute function config.set_updated_at();

-- ── validation_rules — quality thresholds (STEP 03 "Define Rules") ────────────
-- target_column NULL = a stream-level rule (min_row_count, freshness_minutes).
-- severity 'error' stops the pipeline; 'warn' only logs to validation_runs.
create table if not exists config.validation_rules (
    rule_id       serial primary key,
    stream_id     integer not null references config.streams(stream_id) on delete cascade,
    target_column text,
    rule_type     text not null,
    rule_value    text,                                  -- scalar, or JSON array for accepted_values
    severity      text not null default 'error',
    is_active     boolean not null default true,
    description   text,
    updated_at    timestamptz not null default now(),
    constraint validation_rules_severity_chk check (severity in ('error', 'warn')),
    constraint validation_rules_type_chk check (rule_type in (
        'not_null', 'min', 'max', 'accepted_values',
        'max_null_pct', 'min_row_count', 'freshness_minutes'
    )),
    -- NULLS NOT DISTINCT (PG15+) so a NULL target_column still conflicts on re-seed,
    -- keeping the seed loader's ON CONFLICT upsert idempotent for stream-level rules.
    unique nulls not distinct (stream_id, target_column, rule_type)
);
drop trigger if exists validation_rules_touch on config.validation_rules;
create trigger validation_rules_touch before update on config.validation_rules
    for each row execute function config.set_updated_at();

-- ── validation_runs — audit log + certification (STEP 05 "Monitor & Validate") ─
-- One row per check per stream per run (pass AND fail), committed BEFORE the gate
-- raises, so the reason a run stopped is always queryable.
create table if not exists config.validation_runs (
    run_id         bigserial primary key,
    run_ts         timestamptz not null default now(),
    airflow_run_id text,
    stream_name    text not null,
    target_column  text,
    check_type     text not null,                        -- 'required' or a rule_type
    status         text not null,                        -- ok|missing|null|below_threshold|certified|config_warning
    rows_checked   integer,
    null_count     integer,
    detail         text
);
create index if not exists validation_runs_run_ts_idx on config.validation_runs (run_ts desc);
create index if not exists validation_runs_status_idx on config.validation_runs (status);
create index if not exists validation_runs_stream_idx on config.validation_runs (stream_name);

-- ── City helpers — convenience wrappers over locations + source_locations ──────
-- Pure ergonomics: the two-table design is unchanged, these just do both inserts
-- (with the id lookups) in one call so you can't do half of it by mistake.

-- Add a city: master row + source links in one call. Every city gets weather
-- (openweather). Pass a full bounding box (all four p_*) to ALSO enable TomTom traffic.
--   select config.add_city('Zagreb', 45.8150, 15.9819);                         -- weather only
--   select config.add_city('Zagreb', 45.8150, 15.9819, 45.75,15.85,45.88,16.05); -- + traffic
create or replace function config.add_city(
    p_city    text,
    p_lat     numeric,
    p_lon     numeric,
    p_min_lat numeric default null,
    p_min_lon numeric default null,
    p_max_lat numeric default null,
    p_max_lon numeric default null
) returns void
language plpgsql as $$
declare
    v_loc_id int;
begin
    insert into config.locations (city, latitude, longitude)
    values (p_city, p_lat, p_lon)
    on conflict (city) do update
        set latitude = excluded.latitude, longitude = excluded.longitude
    returning location_id into v_loc_id;

    -- weather: every city
    insert into config.source_locations (source_id, location_id)
    select source_id, v_loc_id from config.sources where source_name = 'openweather'
    on conflict (source_id, location_id) do nothing;

    -- traffic: only when a bounding box is supplied
    if p_min_lat is not null then
        insert into config.source_locations
            (source_id, location_id, min_lat, min_lon, max_lat, max_lon)
        select source_id, v_loc_id, p_min_lat, p_min_lon, p_max_lat, p_max_lon
        from config.sources where source_name = 'tomtom'
        on conflict (source_id, location_id) do update
            set min_lat = excluded.min_lat, min_lon = excluded.min_lon,
                max_lat = excluded.max_lat, max_lon = excluded.max_lon;
    end if;
end;
$$;

-- Pause / resume a city everywhere (keeps rows + history; reversible). is_active is
-- honoured by setup_airbyte (it reads only active locations + links).
--   select config.set_city_active('Ohrid', false);   -- stop ingesting
--   select config.set_city_active('Ohrid', true);    -- resume
create or replace function config.set_city_active(p_city text, p_active boolean)
returns void
language plpgsql as $$
begin
    update config.locations set is_active = p_active where city = p_city;
    update config.source_locations set is_active = p_active
     where location_id = (select location_id from config.locations where city = p_city);
end;
$$;

-- Permanently delete a city (source_locations rows cascade via FK). Prefer
-- set_city_active(..., false) if you might want it back.
--   select config.remove_city('Zagreb');
create or replace function config.remove_city(p_city text) returns void
language plpgsql as $$
begin
    delete from config.locations where city = p_city;
end;
$$;
