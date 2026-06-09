# Smart City Analytics Pipeline

End-to-end ELT platform that automatically ingests weather, air pollution, and transportation
data from public APIs, cleans it into PostgreSQL **staging** views and **intermediate** tables
(incremental hourly facts + daily rollups) with dbt, and orchestrates the flow with Airflow.
Everything runs in one PostgreSQL database: Airbyte → `airbyte_raw` → dbt `staging` (views) →
dbt `intermediate` (hourly facts + daily rollups).

---

## Architecture

```
OpenWeather API  --+
TomTom API  -------+--> Airbyte --> PostgreSQL --> dbt staging --> dbt intermediate
                   |                airbyte_raw     (views)         (hourly facts + daily rollups)
                   +-----------------------------------------------------------
                       Airflow: smart_city_pipeline (@hourly ELT)
                                smart_city_maintenance (@daily raw cleanup)
```

| Layer | Tool |
|---|---|
| Ingestion | Airbyte (abctl / Kubernetes)
| Landing DB | PostgreSQL 18 (local, port 5432)
| Transformation | dbt-postgres (staging views + intermediate tables)
| Orchestration | Apache Airflow (Docker, port 8080)

---

## Data Sources

| Source | Streams | Cities |
|---|---|---|
| OpenWeather Free 2.5 | current weather, air pollution, 5-day forecast | Skopje, Berlin, London |
| TomTom Traffic | traffic flow, traffic incidents | London, Berlin, Amsterdam |

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
dbt run   --select staging      --target staging   # cleaned views
dbt build --select intermediate --target staging   # hourly facts + daily rollups + tests
```

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
- **5 dbt staging models** (PostgreSQL views), one per Airbyte source stream
- **4 dbt intermediate hourly facts** (incremental tables) — deduped to one row per observation; preserve time-of-day + history independent of raw pruning
- **3 dbt intermediate daily rollups** (tables) — aggregated *from* the hourly facts to one row per `(city, date_utc)`, with uniqueness/not_null tests
- **3 dbt forecast models** — issue history (incremental), current forecast, and a prediction-vs-actual accuracy model
- **Airflow DAG** `smart_city_pipeline` (@hourly) — triggers 6 Airbyte syncs in parallel, then runs dbt staging → dbt intermediate (build + test)
- **Airflow DAG** `smart_city_maintenance` (@daily) — prunes old `airbyte_raw` rows per retention policy
- **Airbyte setup script** — `ingestion/scripts/setup_airbyte.py` adds new cities from config without UI

### Staging (PostgreSQL `staging` schema — views)
| View | Description |
|---|---|
| `stg_current_weather` | Typed current weather fields per city from OpenWeather |
| `stg_air_pollution` | Typed AQI + pollutant fields per city from OpenWeather |
| `stg_weather_forecast` | 5-day / 3-hour forecast records from OpenWeather |
| `stg_traffic_flow` | Road-segment speeds and congestion from TomTom |
| `stg_traffic_incidents` | Active traffic incidents from TomTom |

### Intermediate — hourly facts (PostgreSQL `intermediate` schema — incremental tables, one row per observation)
| Table | Description |
|---|---|
| `int_city_hourly_weather` | Hourly temp/wind/humidity/precip/condition per city |
| `int_city_hourly_pollution` | Hourly AQI + pollutant concentrations per city |
| `int_city_hourly_traffic_flow` | Per-sync congestion/speed snapshots per city |
| `int_city_hourly_traffic_incidents` | Per-sync incident detail (id, delay, magnitude, from/to) per city |

Each dedupes its staging source on the stream's business key (keeping the latest `extracted_at`)
and is **incremental** (`delete+insert`, 6h lookback) — required because Airbyte runs
`full_refresh_append` (appends a fresh full snapshot every hour). Append-only, so they accumulate
clean hourly history forever, independent of raw pruning. Carry `date_utc` + `hour_utc`.

### Intermediate — daily rollups (PostgreSQL `intermediate` schema — tables, one row per city/day)
| Table | Description |
|---|---|
| `int_city_daily_weather` | Daily temp/wind/precip/dominant-condition per city |
| `int_city_daily_pollution` | Daily AQI + pollutant averages, `hours_poor_air` |
| `int_city_daily_traffic` | Daily congestion/speed + incident counts per city |

Aggregated *from* the hourly facts (no re-dedup), keyed on `city_date_key = md5(city|date_utc)`
with `unique` + `not_null` tests.

### Intermediate — forecast (PostgreSQL `intermediate` schema)
Models the 5-day / 3-hour forecast (two timestamps: `forecast_at` = predicted time,
`issued_at` = when predicted; `lead_time` = the difference).
| Table | Description |
|---|---|
| `int_city_weather_forecast` | Incremental, append-only **issue history** — one row per prediction issuance; persists forecasts as issued for later scoring |
| `int_city_forecast_latest` | Latest prediction per future slot = the current 5-day forecast |
| `int_city_forecast_accuracy` | Past predictions scored vs observed weather — temp error, rain hit/miss, condition match, by lead time (with 1/0 helper cols for BI hit-rates) |

---

## Restarting after a reboot

PostgreSQL starts automatically with Windows. For everything else:

```powershell
# 1. Airbyte — Kind container exits on reboot, restart it then reinstall pods
docker start airbyte-abctl-control-plane
abctl local install
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
│   └── models/
│       ├── staging/      -> PostgreSQL (5 views)
│       └── intermediate/ -> PostgreSQL (4 hourly facts + 3 daily rollups + 3 forecast)
├── venv313/             <- Python 3.13 venv (always use this)
└── .env                 <- secrets (not committed)
```
