# `metadata/` — the `config` schema (config-driven pipeline)

The **single source of truth** for what the pipeline ingests and how it is validated is a `config`
schema in the `smart_city` Postgres DB. The pipeline is a **generic engine** driven by those tables —
adding a source, stream, city, or field is an **`INSERT`**, not a code change.

```
        CONFIGURATION  (What? / Where? / How? / When?)   ← the config.* tables
               │
               ▼
   INPUT  ──►  PIPELINE          ──►  OUTPUT
  Airbyte     dbt "same engine"      typed staging, validated & certified
```

> ### ⚠️ The database owns this schema — there is no DDL in this repo
> There is no `schema.sql`, no `migrations.sql`, and no seeder. Tables are **created and altered
> directly in Postgres** (psql / pgAdmin), and this document is the only human-readable description
> of them. Two consequences you must internalise:
>
> - **The `config` schema is host state, like `.env` and `pg_hba.conf`.** It is not reproducible from
>   a clone. A rebuilt machine restores it from a backup (see [Backup & restore](#backup--restore)) or
>   rebuilds it by hand from the [table reference](#table-reference) below.
> - **Keep this file current.** An `alter table` that isn't reflected here leaves the schema
>   undocumented anywhere. Update the table reference in the same sitting as the DDL.
>
> The pre-2026-08-10 DDL files remain recoverable from git history if you ever want them as a
> starting point: `git show 62e63a1:config/schema.sql`.

---

## The lifecycle (edit config → the next hourly run does the rest)

| Step | What you do | What runs |
|---|---|---|
| **01 Identify** | decide a new data need | — |
| **02 Add Config** | `INSERT` into `config.sources` / `config.streams` (+ city rows) | — |
| **03 Define Rules** | `INSERT` `config.field_mappings` (parse logic) + `config.validation_rules` (thresholds) | — |
| **04 Auto-Detect** | *(nothing)* | `reconcile_airbyte` applies it to Airbyte; the dbt `build_staging` macro reads the new fields |
| **05 Monitor & Validate** | *(nothing)* | `validate_contract` gate + `config.validation_runs` audit → run certified; ingestion failures land in `config.load_errors` |

## The tables at a glance

| Table | Holds | Key flags |
|---|---|---|
| `config.sources` | one row per API (`openweather`, `tomtom`) | `is_active` |
| `config.streams` | one row per stream per source (+ target table, sync mode) | `is_active` |
| `config.locations` | one row per city (lat/lon) | `is_active` |
| `config.source_locations` | which cities each source ingests (+ TomTom bbox) | `is_active` |
| `config.field_mappings` | **the contract**: `source_expr` → `target_column` (+ `data_type`) | `is_required`, `is_active` |
| `config.validation_rules` | quality thresholds (min/max/accepted_values/…) | `severity`, `is_active` |
| `config.validation_runs` | audit log of every validation check (pass **and** fail) | `resolved` |
| `config.load_errors` | ingestion failures — one row per failed combination per run | `resolved` |

---

## Table reference

Everything needed to recreate the schema by hand. All tables live in schema `config`.
`updated_at` columns are maintained by a `*_touch` `before update` trigger calling
`config.set_updated_at()` — present on every table below **except** `validation_runs` and
`load_errors` (append-only logs, no `updated_at`).

### `config.sources` — one row per API

| Column | Type | Notes |
|---|---|---|
| `source_id` | `serial` | **PK** |
| `source_name` | `text not null` | **unique** — `'openweather'`, `'tomtom'` |
| `connector_name` | `text not null` | Airbyte connector display name |
| `api_key_env` | `text` | env var holding the API key value |
| `api_key_field` | `text` | connector config key for the key (`'appid'` / `'api_key'`) |
| `schedule_cron` | `text` | informational only — Airflow owns scheduling |
| `is_active` | `boolean not null default true` | |
| `created_at` / `updated_at` | `timestamptz not null default now()` | |

### `config.streams` — one row per stream per source

| Column | Type | Notes |
|---|---|---|
| `stream_id` | `serial` | **PK** |
| `source_id` | `integer not null` | **FK** → `sources(source_id)` `on delete cascade` |
| `stream_name` | `text not null` | `'current_weather'`, … |
| `target_schema` | `text not null default 'staging'` | raw landing schema (Airbyte-written) |
| `target_table` | `text not null` | raw table Airbyte writes |
| `sync_mode` | `text not null default 'full_refresh_append'` | |
| `is_active` | `boolean not null default true` | |
| `created_at` / `updated_at` | `timestamptz not null default now()` | |

**Constraint:** `unique (source_id, stream_name)`

### `config.locations` — one row per city

| Column | Type | Notes |
|---|---|---|
| `location_id` | `serial` | **PK** |
| `city` | `text not null` | **unique** |
| `latitude` / `longitude` | `numeric not null` | |
| `is_active` | `boolean not null default true` | |
| `updated_at` | `timestamptz not null default now()` | |

### `config.source_locations` — which cities each source ingests

Weather covers all cities with just lat/lon; TomTom covers a subset and needs a bounding box.
One join row per `(source, city)`.

| Column | Type | Notes |
|---|---|---|
| `source_id` | `integer not null` | **PK part**, **FK** → `sources` `on delete cascade` |
| `location_id` | `integer not null` | **PK part**, **FK** → `locations` `on delete cascade` |
| `min_lat` / `min_lon` / `max_lat` / `max_lon` | `numeric` | TomTom bounding box; NULL for weather-only |
| `is_active` | `boolean not null default true` | |
| `updated_at` | `timestamptz not null default now()` | |

**Constraint:** `primary key (source_id, location_id)`

> **Grain is `(source, city)` — not `(source, stream, city)`.** A city is on or off for a whole
> source, never per-stream. Deliberate: no use case needs Barcelona *flow* on while Barcelona
> *incidents* is off, and the finer grain would force per-stream partition routing in both
> connector YAMLs.

### `config.field_mappings` — **the contract**

`source_expr` is a SQL expression over the raw Airbyte row (JSON path, quoted camelCase column,
function, or a full computed/`CASE` expression). `data_type` is an optional cast; NULL means
`source_expr` already yields the final type. The generic staging engine emits, per active row:

```
source_expr [::data_type] as target_column
```

| Column | Type | Notes |
|---|---|---|
| `mapping_id` | `serial` | **PK** |
| `stream_id` | `integer not null` | **FK** → `streams(stream_id)` `on delete cascade` |
| `target_column` | `text not null` | output column name |
| `source_expr` | `text not null` | SQL expression over the raw row |
| `data_type` | `text` | optional cast; NULL = already typed |
| `is_required` | `boolean not null default false` | true → validation gate stops on absence/all-NULL |
| `is_active` | `boolean not null default true` | false → engine omits the column (API dropped it) |
| `ordinal` | `integer not null default 0` | output column order |
| `description` | `text` | |
| `updated_at` | `timestamptz not null default now()` | |

**Constraints:**
- `unique (stream_id, target_column)`
- `field_mappings_required_active_chk`: `check (not (is_required and not is_active))`

> **Why that CHECK exists.** A key/grain column (`city`, `observed_at`, `incident_id`) is
> `is_required = true`; deactivating it would drop the column from the generated staging SELECT and
> blow up `dbt_intermediate` later with a cryptic *"column does not exist"*. The constraint stops
> that at edit time — to retire a required field, clear `is_required` first (a deliberate two-step).

`raw_id` / `extracted_at` are emitted automatically by the engine and are **not** in this table.

### `config.validation_rules` — quality thresholds

`target_column` NULL = a stream-level rule (`min_row_count`, `freshness_minutes`).
`severity = 'error'` stops the pipeline; `'warn'` only logs.

| Column | Type | Notes |
|---|---|---|
| `rule_id` | `serial` | **PK** |
| `stream_id` | `integer not null` | **FK** → `streams(stream_id)` `on delete cascade` |
| `target_column` | `text` | NULL = stream-level rule |
| `rule_type` | `text not null` | see constraint below |
| `rule_value` | `text` | scalar, or JSON array for `accepted_values` |
| `severity` | `text not null default 'error'` | |
| `is_active` | `boolean not null default true` | |
| `description` | `text` | |
| `updated_at` | `timestamptz not null default now()` | |

**Constraints:**
- `validation_rules_severity_chk`: `severity in ('error','warn')`
- `validation_rules_type_chk`: `rule_type in ('not_null','min','max','accepted_values','max_null_pct','min_row_count','freshness_minutes')`
- `unique nulls not distinct (stream_id, target_column, rule_type)` — PG15+; `nulls not distinct` so
  a NULL `target_column` still conflicts, keeping stream-level rules idempotent on re-insert

### `config.validation_runs` — validation audit log

One row per check per stream per run (pass **and** fail), committed **before** the gate raises, so
the reason a run stopped is always queryable.

| Column | Type | Notes |
|---|---|---|
| `run_id` | `bigserial` | **PK** |
| `run_ts` | `timestamptz not null default now()` | |
| `airflow_run_id` | `text` | |
| `stream_name` | `text not null` | |
| `target_column` | `text` | |
| `check_type` | `text not null` | `'required'` or a `rule_type` |
| `status` | `text not null` | `ok · missing · null · below_threshold · certified · config_warning` |
| `rows_checked` / `null_count` | `integer` | |
| `detail` | `text` | |
| `resolved` | `boolean not null default false` | triage flag |
| `resolved_at` | `timestamptz` | |
| `resolved_note` | `text` | |

**Indexes:** `(run_ts desc)` · `(status)` · `(stream_name)` · partial `(run_ts desc) where not resolved and status <> 'ok' and status <> 'certified'`

### `config.load_errors` — ingestion failure log

One row per failed combination per run, written by the Airflow sync task from Airbyte's own failure
payload. Answers *"which city/stream failed to ingest, and when?"* in SQL — previously that detail
existed only in Airbyte job logs and a transient alert email.

| Column | Type | Notes |
|---|---|---|
| `error_id` | `bigserial` | **PK** |
| `run_ts` | `timestamptz not null default now()` | |
| `airflow_run_id` | `text` | |
| `source_name` | `text not null` | connection name — `'openweather'` / `'tomtom'` |
| `stream_name` | `text` | NULL when the failure isn't stream-attributable |
| `city` | `text` | NULL when the failure isn't city-attributable |
| `job_id` | `bigint` | Airbyte job id, for cross-referencing its logs |
| `failure_origin` | `text` | Airbyte `failureOrigin` (`source` / `destination` / `replication`) |
| `failure_type` | `text` | Airbyte `failureType` (`config_error` / `system_error` / …) |
| `message` | `text` | |
| `resolved` / `resolved_at` / `resolved_note` | | triage, same idiom as `validation_runs` |

**Indexes:** `(run_ts desc)` · `(source_name)` · partial `(run_ts desc) where not resolved`

### Views

| View | Returns |
|---|---|
| `config.open_validation_failures` | unresolved rows with `status in ('missing','null','below_threshold','config_warning')`, newest first |
| `config.open_load_errors` | unresolved `load_errors`, newest first |

### Functions

| Function | Returns | Purpose |
|---|---|---|
| `config.set_updated_at()` | `trigger` | backs the six `*_touch` triggers |
| `config.add_city(city, lat, lon [, min_lat, min_lon, max_lat, max_lon])` | `void` | master row + source links in one call; bbox also enables TomTom |
| `config.set_city_active(city, active)` | `void` | pause/resume a city everywhere (keeps history) |
| `config.remove_city(city)` | `void` | hard delete (`source_locations` cascade) |
| `config.resolve_validation(run_id [, note])` | `void` | mark one validation failure handled |
| `config.resolve_failures(stream [, note])` | `integer` | mark all open failures for a stream handled; returns count |
| `config.resolve_load_error(error_id [, note])` | `void` | mark one load error handled |
| `config.resolve_load_errors(source [, note])` | `integer` | mark all open load errors for a source handled; returns count |

---

## Inspecting the live schema

With no DDL file, the database is the reference. From `psql`:

```sql
\dn                            -- schemas
\dt config.*                   -- tables
\d  config.field_mappings      -- full definition of one table (columns, indexes, constraints, triggers)
\df config.*                   -- functions with signatures
\dv config.*                   -- views
```

Or portably, without `psql`:

```sql
select table_name   from information_schema.tables    where table_schema  = 'config' order by 1;
select routine_name from information_schema.routines  where routine_schema = 'config' order by 1;
select conrelid::regclass, conname, pg_get_constraintdef(oid)
  from pg_constraint where connamespace = 'config'::regnamespace order by 1;
```

## Backup & restore

The `config` schema is not in git, so **the dump is your only automatic recovery path**. Re-run it
after any material config change:

```bash
"/c/Program Files/PostgreSQL/18/bin/pg_dump.exe" -h localhost -p 5432 -U postgres \
  -d smart_city --schema=config \
  -f "/c/Users/Andrej/Documents/smart_city_config_backups/config_$(date +%Y%m%d).sql"
```

Restore onto a rebuilt machine (structure **and** rows):

```bash
psql -h localhost -p 5432 -U postgres -d smart_city -f config_<date>.sql
```

Sanity-check a dump before trusting it — it should contain 8 `CREATE TABLE`s, 8 `CREATE FUNCTION`s,
2 `CREATE VIEW`s and a `COPY config.field_mappings` block:

```bash
grep -c "^CREATE TABLE" config_<date>.sql
```

If no dump exists, rebuild by hand from the [table reference](#table-reference) above, then reload
rows: sources/streams/locations are a handful of inserts, and `field_mappings` (88 rows) is the
laborious part — recover those from the newest dump or from
`git show 62e63a1:config/seed_config.py`.

---

## `field_mappings.source_expr` — a SQL expression, not just a JSON path

| Shape | `source_expr` | `data_type` |
|---|---|---|
| direct column | `city` | *(null)* |
| JSON path + cast | `(main->>'temp')` | `numeric` |
| nested array | `(weather->0->>'main')` | `text` |
| quoted camelCase | `"currentSpeed"` | *(null)* |
| function | `to_timestamp(dt) at time zone 'UTC'` | *(null)* |
| computed / CASE | `round(1.0 - ("currentSpeed"::numeric / nullif("freeFlowSpeed",0)::numeric), 2)` | *(null)* |

### The two flags (the core ask)
- **`is_active = false`** → the engine **omits** the field (staging drops the column; the validator
  ignores it). Flip this when an API stops returning a field — no code change, no pipeline break.
- **`is_required = true`** (and active) → the **validation gate stops the pipeline** if that field
  is absent from the raw payload or entirely NULL in the latest batch.

---

## Common changes (all pure SQL)

```sql
-- Add a city — one call (helper does the locations + source_locations inserts for you).
-- Weather (openweather) is always added; pass a bounding box to ALSO enable TomTom traffic.
select config.add_city('Zagreb', 45.8150, 15.9819);                             -- weather only
select config.add_city('Zagreb', 45.8150, 15.9819, 45.75, 15.85, 45.88, 16.05); -- + traffic

-- Pause / resume a city everywhere (keeps history), or delete it permanently
select config.set_city_active('Ohrid', false);   -- stop ingesting  (true to resume)
select config.remove_city('Zagreb');             -- hard delete (source_locations cascade)

-- Turn a field off because the API stopped returning it (no pipeline break, column disappears)
update config.field_mappings set is_active = false
where target_column = 'wind_gust_ms'
  and stream_id = (select stream_id from config.streams where stream_name = 'current_weather');

-- Make a field required (pipeline stops if it goes missing/all-NULL)
update config.field_mappings set is_required = true
where target_column = 'pm10_ug_m3'
  and stream_id = (select stream_id from config.streams where stream_name = 'air_pollution');

-- Add a quality threshold (severity 'error' stops the pipeline; 'warn' only logs)
insert into config.validation_rules (stream_id, target_column, rule_type, rule_value, severity, description)
select stream_id, 'pm2_5_ug_m3', 'max', '500', 'warn', 'Implausibly high PM2.5'
from config.streams where stream_name = 'air_pollution';

-- Pause an entire source
update config.sources set is_active = false where source_name = 'tomtom';

-- Triage validation failures: see what's open, then mark handled (keeps the audit row)
select * from config.open_validation_failures;                            -- unresolved, newest first
select config.resolve_validation(12345, 'fixed bad coords for Ohrid');    -- one row, by run_id
select config.resolve_failures('air_pollution', 'API outage, recovered'); -- all open for a stream

-- Triage ingestion failures: same idiom, different table
select * from config.open_load_errors;                                  -- unresolved, newest first
select config.resolve_load_error(42, 'transient TomTom 503');           -- one row, by error_id
select config.resolve_load_errors('tomtom', 'API outage, recovered');   -- all open for a source
```

`rule_type` ∈ `not_null · min · max · accepted_values · max_null_pct · min_row_count ·
freshness_minutes`. For `accepted_values`, `rule_value` is a JSON array, e.g. `[1,2,3,4,5]`.
Stream-level rules (`min_row_count`, `freshness_minutes`) leave `target_column` NULL.

> **Typo guard:** `validation_rules.target_column` is free text (not a foreign key), so a
> misspelled column would silently never fire. The validator catches this — each run flags such a
> rule as a **non-blocking** `config_warning` row in `config.validation_runs` (a known-but-disabled
> field stays quiet). `validation_runs.status` ∈
> `ok · missing · null · below_threshold · certified · config_warning`.

---

## What still requires code

Config-driven does **not** mean zero-code, and pretending otherwise is how people get stuck. The
boundary:

| Change | Code? |
|---|---|
| New city | ❌ no — `select config.add_city(...)` |
| New field on an existing stream | ❌ no — `insert into config.field_mappings` |
| Retire a field the API dropped | ❌ no — `is_active = false` |
| New quality threshold | ❌ no — `insert into config.validation_rules` |
| Pause a city / stream / source | ❌ no — `is_active = false` |
| New **stream** on an existing source | ⚠️ partly — config rows, **plus** the stream must exist in the Airbyte connector YAML |
| New **source / API** | ✅ **yes** — a new Airbyte connector YAML in `ingestion/connections/`, with its own request shape, auth and partition routing |

> ### ⚠️ Editing a connector YAML in this repo does nothing on its own
> The Airbyte connectors are **published artifacts**. A change to
> `ingestion/connections/*.yaml` only takes effect once the connector is **republished in the
> Airbyte Builder UI**. This has bitten us before: the TomTom `incidentDetails` `fields` parameter
> was correct in the repo YAML for some time while the live connector still returned only
> `iconCategory` + geometry.

---

## Where the engine reads this

| Consumer | Reads / writes |
|---|---|
| `ingestion/scripts/setup_airbyte.py` (and the DAG's `reconcile_airbyte` task) | reads `sources`, `streams`, `locations`, `source_locations` |
| dbt macro `build_staging` → every `stg_*` model | reads `field_mappings` (active, ordered) |
| Airflow `validate_contract` gate → `airflow/dags/config_utils.py` | reads `field_mappings` (required) + `validation_rules`; writes `validation_runs` |
| Airflow `sync_*` tasks → `airflow/dags/airbyte_utils.py` | writes `load_errors` on sync failure |
