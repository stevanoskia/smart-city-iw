# Smart City Analytics Pipeline

End-to-end ELT platform that automatically ingests weather, air pollution, and transportation
data from public APIs, parses it with dbt through **ephemeral** staging models (inline CTEs, no DB
object) into durable **intermediate** tables (incremental hourly facts + forecast issue history),
and orchestrates the flow with Airflow.
Everything runs in one PostgreSQL database: Airbyte → `staging` (raw JSON) → dbt `intermediate`
(hourly facts + forecast history) → dbt `marts` (star schema + OBT + analytics). The `stg_*`
JSON-parsing models are ephemeral (compile inline as CTEs), so `staging` holds only raw Airbyte tables.

---

## Architecture

```
OpenWeather API  --+
TomTom API  -------+--> Airbyte --> PostgreSQL --> dbt intermediate --> dbt marts
                   |   (1 partition-  staging (raw)  (hourly facts +      (star schema
                   |    routed conn    stg_* ephemeral  forecast history)  + OBT + analytics)
                   |    per API)
                   +-----------------------------------------------------------
                       Airflow: smart_city_pipeline (@hourly ELT)
                                smart_city_maintenance (@daily raw cleanup)
```

| Layer | Tool |
|---|---|
| Ingestion | Airbyte (abctl / Kubernetes)
| Landing DB | PostgreSQL 18 (local, port 5432)
| Transformation | dbt-postgres (staging ephemeral parsing + intermediate tables + marts tables)
| Orchestration | Apache Airflow (Docker, port 8080)
| Reporting | Power BI (dashboards built on the `marts` layer)

---

## Data Sources

| Source | Streams | Cities |
|---|---|---|
| OpenWeather Free 2.5 | current weather, air pollution, 5-day forecast | Skopje, Berlin, London, Amsterdam, Belgrade, Brussels, Barcelona, Prilep, Bitola, Ohrid (10) |
| TomTom Traffic | traffic flow, traffic incidents | London, Berlin, Amsterdam, Belgrade, Brussels, Barcelona (6) |

Each provider is one Airbyte connection, partition-routed over its city list — add cities in
`ingestion/config/sources.yml`, no new connections.

---

## Quick Start

### Prerequisites
- Python 3.13, PostgreSQL 18, Docker Desktop, abctl

### 1. Clone and set up Python environment
```bash
git clone https://github.com/stevanoskia/smart-city-iw.git
cd smart-city-iw
py -3.13 -m venv venv313
source venv313/Scripts/activate
pip install dbt-postgres==1.8.2 psycopg2-binary \
            python-dotenv requests pyyaml
cp .env.example .env   # fill in credentials
```

### 2. Configure dbt profiles
See `~/.dbt/profiles.yml` — one target: `staging` (PostgreSQL).
Full config in CLAUDE.md.

### 3. Install and start Airbyte
```powershell
# First-time install (takes ~5 min, downloads Kind cluster + Airbyte pods)
abctl local install
# UI: localhost:8000  — get credentials with:
abctl local credentials
```

### 4. Configure Airbyte connections
```bash
# Add AIRBYTE_CLIENT_ID, AIRBYTE_CLIENT_SECRET, AIRBYTE_WORKSPACE_ID to .env
# (get client_id / client_secret from Airbyte UI → User → Applications)
python ingestion/scripts/setup_airbyte.py
# Creates ingestion/config/connection_ids.yml
```

### 5. Run dbt manually
```bash
cd dbt/smart_city
dbt deps                                           # install pinned dbt_utils (from package-lock.yml)
dbt run   --select staging      --target staging   # ephemeral parse — no DB object
dbt build --select intermediate --target staging   # hourly facts + forecast history + tests
dbt build --select marts        --target staging   # star schema + OBT + analytics + tests
```
> `dbt deps` is required once (and after any `packages.yml` change) — it installs `dbt_utils`,
> which every model's surrogate keys (`dbt_utils.generate_surrogate_key`) depend on.

### 6. Start Airflow
```bash
cd airflow
# First time only — initialises the Airflow DB and creates the admin user
docker compose run --rm airflow-init
docker compose up -d
# UI: localhost:8080  (admin / admin)
# Enable DAGs: smart_city_pipeline, smart_city_maintenance
```

---

## What's Built

### Pipeline
- **5 dbt staging models** (ephemeral — inline CTEs, no DB object), one per Airbyte source stream
- **4 dbt intermediate hourly facts** (incremental tables) — deduped to one row per clock hour; preserve time-of-day + history independent of raw pruning
- **1 dbt forecast model** — `int_city_weather_forecast`, incremental issue history (every prediction as issued, for later accuracy scoring)
- **12 dbt marts models** — star schema (dims + facts), the `mart_city_daily` OBT, and analytics marts; `relationships`/`unique`/`accepted_values` tests enforce FK→dimension integrity
- **Airflow DAG** `smart_city_pipeline` (@hourly) — triggers 2 Airbyte syncs in parallel (one partition-routed connection per API), then runs **dbt deps** (install pinned `dbt_utils`) → dbt staging → dbt intermediate → dbt marts (build + test)
- **Surrogate keys** — all keys (`city_key`, `city_hour_key`, `city_date_key`, `forecast_key`, …) are generated with **`dbt_utils.generate_surrogate_key`** (NULL-safe, consistent), pinned to `dbt_utils` 1.4.1 via `package-lock.yml`. Migration how-to for the old hand-written `md5` keys: `docs/surrogate_key_migration.md`
- **Airflow DAG** `smart_city_maintenance` (@daily) — prunes old `staging` (raw JSON) rows per retention policy
- **Email alerts** — both DAGs email `ALERT_EMAIL` on task failure (which step + error) and on success (whole-pipeline / daily-cleanup done), via Gmail SMTP configured through `AIRFLOW__SMTP__*` env vars (App Password). Guarded by `ALERT_EMAIL`, so unset = disabled
- **Airbyte setup script** — `ingestion/scripts/setup_airbyte.py` creates one partition-routed source/connection per API; add cities via config, no UI

### Staging (ephemeral `stg_*` parsers — no DB object)
| Model | Description |
|---|---|
| `stg_current_weather` | Typed current weather fields per city from OpenWeather |
| `stg_air_pollution` | Typed AQI + pollutant fields per city from OpenWeather |
| `stg_weather_forecast` | 5-day / 3-hour forecast records from OpenWeather |
| `stg_traffic_flow` | Road-segment speeds and congestion from TomTom |
| `stg_traffic_incidents` | Active traffic incidents from TomTom |

### Intermediate — hourly facts (PostgreSQL `intermediate` schema — incremental tables, one row per clock hour)
| Table | Description |
|---|---|
| `int_city_hourly_weather` | Hourly temp/wind/humidity/precip/condition per city |
| `int_city_hourly_pollution` | Hourly AQI + pollutant concentrations per city |
| `int_city_hourly_traffic_flow` | Per-sync congestion/speed snapshots per city |
| `int_city_hourly_traffic_incidents` | Per-sync incident detail (id, delay, magnitude, from/to) per city |

Each dedupes to **one row per clock hour** — it partitions on `(city, date_trunc('hour', observed_at))`
and keeps the freshest reading in the hour (`order by observed_at desc, extracted_at desc`); the
surrogate `city_hour_key` (built with `dbt_utils.generate_surrogate_key`) is hour-truncated too, so
two syncs in the same clock hour collapse to a single row (idempotent across runs). Incidents key on
`(city, incident_id, observed_at)` instead.
Each is **incremental** (`delete+insert`, 6h lookback) — required because Airbyte runs
`full_refresh_append` (appends a fresh full snapshot every hour). Append-only, so they accumulate
clean hourly history forever, independent of raw pruning. Carry `date_utc` + `hour_utc`.

### Intermediate — forecast (PostgreSQL `intermediate` schema)
Models the 5-day / 3-hour forecast (two timestamps: `forecast_at` = predicted time,
`issued_at` = when predicted; `lead_time` = the difference).
| Table | Description |
|---|---|
| `int_city_weather_forecast` | Incremental, append-only **issue history** — one row per prediction issuance; persists forecasts as issued for later accuracy scoring |

### Marts (PostgreSQL `marts` schema — tables)
Star schema + derived OBT + analytics marts. Daily facts and the OBT share the grain
`(city, date_utc)`; star keys are `city_key` = `dbt_utils.generate_surrogate_key(['city'])` and
`date_key = YYYYMMDD::int`, with `relationships` tests enforcing FK→dimension integrity.
| Model | Kind | Description |
|---|---|---|
| `dim_city` | dimension | One row per city, **derived** from data (no seed): city/country + coords + weather/traffic coverage flags |
| `dim_date` | dimension | **Independent** calendar spine (fixed 2026-01-01 anchor → `current_date + 365d`) with year/quarter/month/weekday/is_weekend attributes |
| `dim_hour` | dimension | 24 static rows (0–23) with `hour_label` (`'06:00'`) + `day_part` (Night/Morning/Afternoon/Evening) |
| `fct_weather_daily` | fact | Daily weather rollup per city |
| `fct_pollution_daily` | fact | Daily AQI + pollutant rollup per city |
| `fct_traffic_daily` | fact | Daily flow + incident rollup per city |
| `fct_traffic_hourly` | fact | Per-hour-of-day flow + incidents for peak-hour analysis |
| `fct_forecast_accuracy` | fact | Prediction-vs-actual scoring from the forecast issue history |
| `mart_city_daily` | OBT | One wide row per `(city, date_utc)` — weather + pollution + traffic LEFT-joined (weather-only cities get NULL traffic) |
| `mart_forecast_latest` | analytics | Latest issued forecast per city / future slot |
| `mart_temperature_trends` | analytics | Temperature trend + anomaly detection |
| `mart_weather_alerts` | analytics | Severe-weather flags |

Design + step-by-step build guide live in `docs/marts_implementation_plan.md` and
`docs/marts_build_guide.md`.

### Reporting — Power BI
Business reporting is done in **Power BI** (`smart_city_dashboard.pbip`), connected to the
PostgreSQL `marts` schema (Import mode) — a star-schema model with KPI measures and an Executive
Overview page (KPI cards, Azure Map, air-quality + forecast visuals). Build log and current status
live in `docs/powerbi_dashboard.md`. *(In progress — the model + 7 KPI cards are built; a
data-refresh issue is being resolved.)*

---

## Restarting after a reboot

PostgreSQL starts automatically with Windows. For everything else:

```powershell
# 1. Airbyte — Kind container exits on reboot; starting it brings the pods back
docker start airbyte-abctl-control-plane
# (give pods ~1-2 min; only if the UI still won't come up: abctl local install)
# Check: abctl local status

# 2. Airflow
cd airflow
docker compose up -d
# UI: localhost:8080
```

---

## Services

| Service | URL | Credentials |
|---|---|---|
| Airbyte | http://localhost:8000 | email + password |
| Airflow | http://localhost:8080 | admin / admin |
| PostgreSQL | localhost:5432 | postgres / (from .env) |

---

## Project Structure

```
smart-city-iw/
├── ingestion/
│   ├── config/          <- city configs + connection IDs for Airflow
│   ├── connections/     <- Airbyte connector YAMLs
│   └── scripts/         <- setup_airbyte.py (config-driven Airbyte setup)
├── airflow/
│   ├── Dockerfile       <- extends apache/airflow:2.9.3 with dbt
│   ├── docker-compose.yml
│   └── dags/
│       ├── airbyte_utils.py                 <- OAuth trigger/wait helpers
│       ├── dag_smart_city_pipeline.py       <- hourly ELT DAG
│       └── dag_smart_city_maintenance.py    <- daily raw-cleanup DAG
├── dbt/smart_city/      <- dbt project root (run all dbt commands here)
│   ├── packages.yml     <- dbt package deps (dbt_utils); package-lock.yml pins 1.4.1
│   ├── macros/          <- incl. backfill_surrogate_keys.sql (one-off key migration)
│   └── models/
│       ├── staging/      -> ephemeral (5 stg_* parsers, no DB object)
│       ├── intermediate/ -> PostgreSQL (4 hourly facts + 1 forecast issue history)
│       └── marts/         -> PostgreSQL (12 tables: dims + facts + OBT + analytics)
├── venv313/             <- Python 3.13 venv (always use this)
└── .env                 <- secrets (not committed)
```
