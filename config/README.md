# `config/` — Config-Driven Pipeline (the `config` schema)

This folder holds the **single source of truth** for what the pipeline ingests and how it is
validated: a `config` schema in the `smart_city` Postgres DB. The pipeline is a **generic engine**
driven by these tables — adding a source, stream, city, or field is an **`INSERT`**, not a code
change.

```
        CONFIGURATION  (What? / Where? / How? / When?)   ← the config.* tables
               │
               ▼
   INPUT  ──►  PIPELINE          ──►  OUTPUT
  Airbyte     dbt "same engine"      typed staging, validated & certified
```

## The lifecycle (edit config → the next hourly run does the rest)

| Step | What you do | What runs |
|---|---|---|
| **01 Identify** | decide a new data need | — |
| **02 Add Config** | `INSERT` into `config.sources` / `config.streams` (+ city rows) | — |
| **03 Define Rules** | `INSERT` `config.field_mappings` (parse logic) + `config.validation_rules` (thresholds) | — |
| **04 Auto-Detect** | *(nothing)* | `reconcile_airbyte` applies it to Airbyte; the dbt `build_staging` macro reads the new fields |
| **05 Monitor & Validate** | *(nothing)* | `validate_contract` gate + `config.validation_runs` audit → run certified |

## The tables

| Table | Holds | Key flags |
|---|---|---|
| `config.sources` | one row per API (`openweather`, `tomtom`) | `is_active` |
| `config.streams` | one row per stream per source (+ target table, sync mode) | `is_active` |
| `config.locations` | one row per city (lat/lon) | `is_active` |
| `config.source_locations` | which cities each source ingests (+ TomTom bbox) | `is_active` |
| `config.field_mappings` | **the contract**: `source_expr` → `target_column` (+ `data_type`) | `is_required`, `is_active` |
| `config.validation_rules` | quality thresholds (min/max/accepted_values/…) | `severity`, `is_active` |
| `config.validation_runs` | audit log of every validation check (pass **and** fail); `resolved` flag + `config.open_validation_failures` view for triage | — |

### `field_mappings.source_expr` — a SQL expression, not just a JSON path
The staging engine emits, per active row: **`source_expr [::data_type] as target_column`**. So
`source_expr` can be anything valid over the raw Airbyte row:

| Shape | `source_expr` | `data_type` |
|---|---|---|
| direct column | `city` | *(null)* |
| JSON path + cast | `(main->>'temp')` | `numeric` |
| nested array | `(weather->0->>'main')` | `text` |
| quoted camelCase | `"currentSpeed"` | *(null)* |
| function | `to_timestamp(dt) at time zone 'UTC'` | *(null)* |
| computed / CASE | `round(1.0 - ("currentSpeed"::numeric / nullif("freeFlowSpeed",0)::numeric), 2)` | *(null)* |

`raw_id` / `extracted_at` are emitted automatically by the engine and are **not** in this table.

### The two flags (the core ask)
- **`is_active = false`** → the engine **omits** the field (staging drops the column; the validator
  ignores it). Flip this when an API stops returning a field — no code change, no pipeline break.
- **`is_required = true`** (and active) → the **validation gate stops the pipeline** if that field
  is absent from the raw payload or entirely NULL in the latest batch.

## Create + seed (first time, or on a rebuilt machine)

Run from the repo root, using **venv313** (it has `psycopg2`/`pyyaml`/`python-dotenv` via
`dbt-postgres`). Needs `POSTGRES_*` in `.env`.

```bash
# 1. Create the schema + tables (idempotent)
./venv313/Scripts/python.exe -c "import os,psycopg2;from pathlib import Path;from dotenv import load_dotenv;load_dotenv('.env');c=psycopg2.connect(host=os.getenv('POSTGRES_HOST'),port=int(os.getenv('POSTGRES_PORT','5432')),dbname=os.getenv('POSTGRES_DB'),user=os.getenv('POSTGRES_USER'),password=os.getenv('POSTGRES_PASSWORD'));c.autocommit=True;c.cursor().execute(Path('config/schema.sql').read_text(encoding='utf-8'))"

# (or, if you have psql:  psql "$DATABASE_URL" -f config/schema.sql)

# 2. Load the rows from the legacy YAML + transcribed field mappings (idempotent)
./venv313/Scripts/python.exe config/seed_config.py
```

`seed_config.py` is the **one-time initial load**. Re-running is safe — it refreshes the
*definition* columns (`source_expr`, `data_type`, thresholds, descriptions) but **preserves** the
operational flags (`is_active`, `is_required`) so live toggles survive. **After the first load, the
DB is the source of truth — make changes with SQL, not by editing YAML** (the YAML files are
retained only as the seed input and are otherwise retired).

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
select * from config.open_validation_failures;                       -- unresolved failures, newest first
select config.resolve_validation(12345, 'fixed bad coords for Ohrid'); -- one row, by run_id
select config.resolve_failures('air_pollution', 'API outage, recovered'); -- all open for a stream
```

`rule_type` ∈ `not_null · min · max · accepted_values · max_null_pct · min_row_count ·
freshness_minutes`. For `accepted_values`, `rule_value` is a JSON array, e.g. `[1,2,3,4,5]`.
Stream-level rules (`min_row_count`, `freshness_minutes`) leave `target_column` NULL.

> **Typo guard:** `validation_rules.target_column` is free text (not a foreign key), so a
> misspelled column would silently never fire. The validator catches this — each run flags such a
> rule as a **non-blocking** `config_warning` row in `config.validation_runs` (a known-but-disabled
> field stays quiet). `validation_runs.status` ∈
> `ok · missing · null · below_threshold · certified · config_warning`.

## Where the engine reads this

| Consumer | Reads |
|---|---|
| `ingestion/scripts/setup_airbyte.py` (and the DAG's `reconcile_airbyte` task) | `sources`, `streams`, `locations`, `source_locations` |
| dbt macro `build_staging` → every `stg_*` model | `field_mappings` (active, ordered) |
| Airflow `validate_contract` gate → `config_utils.py` | `field_mappings` (required), `validation_rules`, writes `validation_runs` |
