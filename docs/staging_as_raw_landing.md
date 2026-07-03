# Staging as the Raw Landing Zone (`airbyte_raw` collapse)

> **Change date:** 2026-07-03
> **Status:** ✅ implemented & verified live (full `dbt build` = 85 nodes, 0 errors)

## TL;DR

We removed the separate `airbyte_raw` schema. **Airbyte now lands raw JSON directly into the
`staging` schema**, and the dbt `stg_*` JSON-parsing models were switched from **views** to
**ephemeral** — so they compile inline as CTEs and create no database object. The result:

```
airbyte → staging (raw JSON) → intermediate (typed, deduped, durable) → marts
```

Three schemas instead of four. `staging` now physically holds **only** Airbyte's raw JSON tables.

---

## Why we did this

The pipeline used to have two overlapping landing layers:

| Old layer | What it was | Problem |
|---|---|---|
| `airbyte_raw` | Airbyte-written tables of raw JSON (short buffer) | — |
| `staging` | dbt **views** parsing that JSON into typed columns | Duplicated the landing concept: raw JSON in one schema, a thin typed mirror in another |

We wanted a single raw landing schema so the flow reads cleanly as **airbyte → staging →
intermediate → marts**.

**The constraint:** Airbyte only ever emits **JSON**, and that JSON must be parsed into typed
columns somewhere. So we couldn't delete the parsing step — we had to relocate it so it no longer
occupies a physical schema.

---

## The key idea: ephemeral staging models

A dbt model's `materialized` config decides whether it becomes a real DB object:

- **`view`** → a physical view in the database → occupies the `staging` schema.
- **`ephemeral`** → *not* built in the database. dbt inlines the model's SQL as a CTE inside
  whatever model references it (`ref('stg_...')`).

By changing the 5 `stg_*` models to `materialized: ephemeral` ([dbt_project.yml](../dbt/smart_city/dbt_project.yml)),
the parsing logic still runs — but **inside** the intermediate models at compile time. The
`staging` schema is left holding only Airbyte's raw JSON tables. That's how "staging becomes the
new raw" without losing the parse step.

The `ref('stg_*')` calls in `int_*` and `marts/dim_city` were **not touched** — ephemeral models
are referenced exactly like any other model.

---

## Architecture: before → after

**Before**
```
Airbyte → airbyte_raw (raw JSON)
              │
              ▼  staging = dbt VIEWS (typed, 1:1 with raw)   ← a physical schema of views
              ▼
        intermediate (incremental) → marts
```

**After**
```
Airbyte → staging (raw JSON tables: current_weather, air_pollution,
              │      weather_forecast, traffic_flow, traffic_incidents)
              │
              │  stg_* ephemeral models parse JSON → typed (inline CTEs, NO DB object)
              ▼
        intermediate (incremental) → marts
```

---

## How JSON becomes data ready for intermediate

This is the heart of the transformation. Weather as the worked example.

**1. Airbyte lands raw JSON** in `staging.current_weather`. Nested columns are JSON blobs:
```json
coord:   {"lat": 41.99, "lon": 21.43}
main:    {"temp": 24.3, "feels_like": 24.1, "humidity": 45, "pressure": 1017}
wind:    {"speed": 3.6, "deg": 120, "gust": 5.1}
weather: [{"main": "Clouds", "description": "scattered clouds"}]
dt:      1751540293          ← unix epoch seconds
city:    "Skopje"            ← injected by Airbyte AddFields from source config
```

**2. The ephemeral `stg_current_weather` model parses it** ([stg_current_weather.sql](../dbt/smart_city/models/staging/stg_current_weather.sql))
using Postgres JSON operators (`->>` = extract field as text, `->0` = index into an array) plus
type casts:
```sql
(main->>'temp')::numeric              as temp_celsius,
(main->>'humidity')::integer          as humidity_pct,
(wind->>'speed')::numeric             as wind_speed_ms,
(weather->0->>'main')::text           as weather_main,
(coord->>'lat')::numeric              as latitude,
to_timestamp(dt) at time zone 'UTC'   as observed_at,   -- epoch → real UTC timestamp
_airbyte_extracted_at                 as extracted_at
-- ...reads from {{ source('staging', 'current_weather') }}
```
Nested JSON → flat, typed, analytics-ready row.

**3. Because it's ephemeral, that SQL compiles straight into the intermediate model.** When dbt
builds [int_city_hourly_weather](../dbt/smart_city/models/intermediate/int_city_hourly_weather.sql),
the compiled SQL becomes (verified during the migration):
```sql
with __dbt__cte__stg_current_weather as (
    ...the JSON-parsing SELECT above...
    from "smart_city"."staging"."current_weather"
),
new_rows as (
    select * from __dbt__cte__stg_current_weather
    where city is not null
      and extracted_at > (select max(extracted_at) - interval '6 hours' from {{ this }})  -- incremental lookback
),
deduped as (
    select *, row_number() over (
        partition by city, date_trunc('hour', observed_at)   -- one row per clock hour
        order by observed_at desc, extracted_at desc           -- freshest reading in the hour wins
    ) as _rn from new_rows
)
select md5(city || '|' || date_trunc('hour', observed_at)::text) as city_hour_key, ...
from deduped where _rn = 1
```

**Transformation chain:** raw JSON → *(parse + cast in ephemeral CTE)* → *(filter to new rows +
dedupe to one row per city-hour)* → typed, deduped **incremental** table. The intermediate table is
`delete+insert` incremental with a 6-hour lookback, so re-runs are idempotent and history
accumulates forever — independent of how short a buffer `staging` keeps.

The other streams follow the same pattern (incidents key on `city|incident_id|observed_at`;
forecast keys on issuance time — see [int_city_weather_forecast.sql](../dbt/smart_city/models/intermediate/int_city_weather_forecast.sql)).

---

## What changed (files)

| File | Change |
|---|---|
| [ingestion/config/connections.yml](../ingestion/config/connections.yml) | destination `schema: airbyte_raw` → `staging` |
| [ingestion/scripts/setup_airbyte.py](../ingestion/scripts/setup_airbyte.py) | connection `namespaceFormat: "airbyte_raw"` → `"staging"` |
| [dbt/smart_city/models/staging/sources.yml](../dbt/smart_city/models/staging/sources.yml) | source renamed to `staging`, `schema: staging` |
| 5× `stg_*.sql` | `source('airbyte_raw', …)` → `source('staging', …)` |
| [dbt/smart_city/dbt_project.yml](../dbt/smart_city/dbt_project.yml) | staging `+materialized: view` → `ephemeral` |
| [airflow/dags/dag_smart_city_maintenance.py](../airflow/dags/dag_smart_city_maintenance.py) | prune `staging.{table}` instead of `airbyte_raw` |
| `CLAUDE.md`, `README.md`, `ingestion/README.md`, `dbt/smart_city/README.md`, `.gitignore` | docs + ignore the backup `.dump` |

**No changes** to the intermediate/marts model SQL, the `generate_schema_name` macro, or profiles.

---

## Migration steps executed (order matters — avoids data loss)

The intermediate tables are `incremental` + append-only — they are the **durable archive** and were
never dropped. The one rule: **never `--full-refresh` them** (that would rebuild from the
now-short-buffer `staging` and lose history).

1. **Catch-up** — captured a ~2,300-row traffic gap into intermediate first (temporarily toggled the
   dbt source back to `airbyte_raw`). 27/27 tests pass.
2. **Backup** — `pg_dump -Fc` of all 5 intermediate tables → `intermediate_backup_2026-07-03.dump`
   (9.5 MB; gitignored). Insurance only.
3. **Repointed Airbyte** — updated both connections' namespace to `staging` via a one-off
   `connections/update` API call (**UUIDs preserved**, so Airflow's `connection_ids.yml` stayed
   valid). Triggered both syncs → succeeded → fresh rows landed in `staging`.
4. **Rebuilt intermediate** incrementally from `staging` (**not** `--full-refresh`) — 27/27 pass.
5. **Rebuilt marts** — 58/58 pass.
6. **Cleanup** — dropped the 5 orphaned `stg_*` views + `DROP SCHEMA airbyte_raw CASCADE`.
7. **Final full `dbt build --select staging intermediate marts`** — 85 nodes, 0 errors.

**No data lost** — every intermediate table grew above baseline:

| Table | Before | After |
|---|---|---|
| int_city_hourly_weather | 481 | 492 |
| int_city_hourly_pollution | 543 | 553 |
| int_city_hourly_traffic_flow | 360 | 366 |
| int_city_hourly_traffic_incidents | 167,503 | 172,067 |
| int_city_weather_forecast | 32,160 | 32,960 |

---

## ⚠️ One non-obvious gotcha: the setup script skips existing connections

[setup_airbyte.py](../ingestion/scripts/setup_airbyte.py) is **idempotent by skipping** — when a
connection already exists it returns early (only sets the manual schedule); it never *edits* the
existing connection. So **re-running the script does not repoint live connections.**

- That's why the migration repointed the two existing connections via a **direct API call**, not by
  re-running the script.
- The `namespaceFormat: "staging"` edit in the script only takes effect for connections created
  **fresh** — e.g. after a full teardown (`docker compose down -v` + delete connections) and a
  clean re-run.
- The connections running today are already correctly pointed at `staging` and will stay that way.

**Analogy:** the script is a setup wizard that only installs what isn't installed yet. Changing the
installer's defaults doesn't reconfigure software that's already installed — you change that
directly. Same distinction here: "install fresh" vs. "already running."
