# Smart City Analytics Pipeline — Project Guide

## Project Purpose

End-to-end ELT data engineering platform that automatically ingests weather, air pollution,
and transportation data from public APIs, transforms it into analytical models, and serves
interactive smart city dashboards. Simulates a real-world smart city analytics solution.

---

## Current Status (as of 2026-06-03)

### Infrastructure
| Component | Status | Notes |
|---|---|---|
| PostgreSQL 18 | ✅ Running | localhost:5432, DB: smart_city |
| Airbyte (abctl) | ✅ Running | localhost:8000, Kind/Kubernetes |
| Airbyte destination | ✅ Configured | smart_city_postgres → airbyte_raw schema |
| Metabase | ✅ Running | localhost:3000, connected to smart_city DB |
| Airflow | 🔜 Pending | docker-compose ready at airflow/, DAGs not written |

### Data Ingestion (APIs)
| API / Stream | Status | Cities | Notes |
|---|---|---|---|
| OpenWeather current weather | ✅ Working | Skopje, Berlin, London | hourly sync |
| OpenWeather air pollution | ✅ Working | Skopje, Berlin, London | hourly sync |
| OpenWeather 5-day forecast | ✅ Working | Skopje, Berlin, London | hourly sync |
| TomTom traffic flow | ✅ Working | London, Berlin, Amsterdam | hourly sync |
| TomTom traffic incidents | ✅ Working | London, Berlin, Amsterdam | hourly sync |

### dbt Transformation (37 tests passing)
| Layer | Model | Status |
|---|---|---|
| Staging | `stg_current_weather` | ✅ Built |
| Staging | `stg_air_pollution` | ✅ Built |
| Staging | `stg_weather_forecast` | ✅ Built |
| Staging | `stg_traffic_flow` | ✅ Built |
| Staging | `stg_traffic_incidents` | ✅ Built |
| Intermediate | `int_city_daily_weather` | ✅ Built |
| Intermediate | `int_city_daily_pollution` | ✅ Built |
| Intermediate | `int_city_daily_traffic` | ✅ Built |
| Intermediate | `int_composite_city_score` | ✅ Built |
| Marts | `mart_temperature_trends` | ✅ Built |
| Marts | `mart_aqi_monitoring` | ✅ Built |
| Marts | `mart_traffic_density` | ✅ Built |
| Marts | `mart_city_comparison` | ✅ Built |
| Marts | `mart_smart_city_kpis` | ✅ Built |
| Marts | `mart_weather_forecast` | ❌ Pending (forecast data model) |

### Dashboard
| Feature | Status |
|---|---|
| Metabase connected to smart_city DB | ✅ Done |
| Mart tables visible in Metabase | 🔜 Schema sync pending |
| Temperature trend charts | ❌ Not built |
| AQI monitoring widgets | ❌ Not built |
| Traffic density heatmaps | ❌ Not built |
| City comparison view | ❌ Not built |
| Smart city KPI cards | ❌ Not built |
| Smart alerts (bonus) | ❌ Not started |
| AI-generated summaries (bonus) | ❌ Not started |

---

## Architecture

```
                        ┌─────────────────────────────────────────┐
                        │           Apache Airflow                  │
                        │     (orchestrates every step below)       │
                        └──────┬──────────────────┬────────────────┘
                               │ triggers sync      │ triggers dbt
                               ▼                    ▼
┌──────────────────────┐   ┌───────────┐   ┌──────────────────┐
│ OpenWeather API      │   │           │   │  PostgreSQL 18   │
│  - current weather   │──►│  Airbyte  │──►│  airbyte_raw     │
│  - air pollution     │   │           │   │  staging         │──► dbt ──► Metabase
│  - forecast          │   └───────────┘   │  intermediate    │
│ TomTom Traffic API   │     :8000          │  marts           │
│  - traffic flow      │                   └──────────────────┘
│  - traffic incidents │                        :5432
└──────────────────────┘
```

**ELT (not ETL):**
- **E**xtract: Airbyte pulls raw data from APIs
- **L**oad: Airbyte writes as-is to `airbyte_raw` schema in PostgreSQL
- **T**ransform: dbt builds staging → intermediate → marts inside PostgreSQL

| Layer | Tool | Port | Purpose |
|---|---|---|---|
| Ingestion | Airbyte (abctl) | 8000 | API connectors, raw data load |
| Storage | PostgreSQL 18 (local) | 5432 | All schemas: raw, staging, intermediate, marts |
| Transformation | dbt (Python venv) | — | SQL models, tests, documentation |
| Orchestration | Airflow (Docker) | 8080 | DAG scheduling, retries, monitoring |
| Visualization | Metabase (Docker) | 3000 | Dashboards, KPI widgets, alerts |

---

## APIs

**OpenWeather Free 2.5 API** (key: `OPENWEATHER_API_KEY`)
| Endpoint | Stream | Fields |
|---|---|---|
| `/data/2.5/weather` | `current_weather` | temp_celsius, humidity, wind_speed, pressure, weather_main, rain_1h, visibility |
| `/data/2.5/air_pollution` | `air_pollution` | aqi (1-5), pm2_5, pm10, co, no2, o3, so2, nh3 |
| `/data/2.5/forecast` | `weather_forecast` | forecast_dt, temp, pop (rain probability), weather_main |

**TomTom Traffic API** (key: `TOMTOM_API_KEY`)
| Endpoint | Stream | Fields |
|---|---|---|
| `/traffic/services/4/flowSegmentData` | `traffic_flow` | currentSpeed, freeFlowSpeed, congestion_score, frc, roadClosure |
| `/traffic/services/5/incidentDetails` | `traffic_incidents` | id, from, to, delay, magnitudeOfDelay, geometry |

---

## Database Layout

### One database, four schemas — no separate destination DB needed

| Schema | Owner | Purpose |
|---|---|---|
| `airbyte_raw` | Airbyte | Raw API responses — never edit manually |
| `staging` | dbt | Typed views over raw tables (1:1 mapping) |
| `intermediate` | dbt | Daily aggregations per city (views) |
| `marts` | dbt | Final analytics tables consumed by Metabase (materialized tables) |

### `airbyte_raw` tables

| Table | City field | Source |
|---|---|---|
| `current_weather` | from API `name` field | OpenWeather /weather |
| `air_pollution` | injected via AddFields | OpenWeather /air_pollution |
| `weather_forecast` | injected via AddFields | OpenWeather /forecast |
| `traffic_flow` | injected via AddFields | TomTom flowSegmentData |
| `traffic_incidents` | injected via AddFields | TomTom incidentDetails |

Airbyte metadata columns on every table: `_airbyte_raw_id`, `_airbyte_extracted_at`, `_airbyte_meta`, `_airbyte_generation_id`

dbt project root: `dbt/smart_city/`
Profiles: `~/.dbt/profiles.yml`
Schema macro: `macros/generate_schema_name.sql` (prevents `staging_staging` duplication)

---

## Airbyte Connector Setup

### Deployment
- Installed via `abctl` (Kubernetes/Kind), not docker-compose
- UI: `localhost:8000`
- Kind node container: `airbyte-abctl-control-plane`
- Kubeconfig: `~/.airbyte/abctl/abctl.kubeconfig`
- Syncs run as Kubernetes pods inside the Kind cluster

### Config-Driven Setup (recommended)

All sources, destinations, and connections are defined in config files and created
by a single idempotent Python script. No manual UI clicking required.

```bash
# Install dependencies
pip install requests pyyaml python-dotenv

# Set AIRBYTE_PG_HOST in .env to your machine's LAN IP (see Environment Variables)

# Run once to create everything, or re-run safely to skip existing resources
python ingestion/scripts/setup_airbyte.py
```

Config files (no secrets):
- `ingestion/config/sources.yml` — city list, lat/lon, connector mapping
- `ingestion/config/connections.yml` — sync schedule, destination schema

Output:
- `ingestion/config/connection_ids.yml` — Airbyte connection UUIDs (git-ignored, used by Airflow DAGs)

### Destination
- Connector: PostgreSQL v3.0.13
- Host: `AIRBYTE_PG_HOST` (host LAN IP — NOT localhost, NOT host.docker.internal)
- Port: `5432` | Database: `smart_city` | Schema: `airbyte_raw`
- SSL: disabled
- pg_hba.conf entry required: `host all all 10.2.0.0/16 scram-sha-256`

### Connector YAMLs
- `ingestion/connections/open_weather_free_2_5.yaml` — OpenWeather connector (3 streams)
- `ingestion/connections/tomtom_traffic.yaml` — TomTom connector (2 streams)
- Both use `AddFields` transformation to inject `city` from source config into every record

### Known quirks
- After updating connector YAML: must go to Connection → Schema tab → Refresh source schema
  (or delete + recreate the connection if schema refresh returns 403)
- Destination host must be LAN IP — Airbyte sync pods run inside Kind cluster
- `city` column not present in old rows synced before connector update — intermediate models
  filter these out with `WHERE city IS NOT NULL`

---

## Folder Structure

```
smart-city-iw/
├── ingestion/
│   ├── config/
│   │   ├── sources.yml              ← city/coordinate config (no secrets)
│   │   ├── connections.yml          ← sync schedule, destination schema
│   │   └── connection_ids.yml       ← auto-generated, git-ignored
│   ├── connections/
│   │   ├── open_weather_free_2_5.yaml
│   │   └── tomtom_traffic.yaml
│   ├── scripts/
│   │   └── setup_airbyte.py         ← idempotent setup script
│   └── README.md
├── airflow/
│   ├── docker-compose.yml
│   ├── dags/                        ← DAGs to be written
│   └── plugins/
├── dbt/
│   └── smart_city/                  ← dbt project root (run dbt commands here)
│       ├── dbt_project.yml
│       ├── macros/
│       │   └── generate_schema_name.sql
│       └── models/
│           ├── staging/             ← 5 models, all built
│           ├── intermediate/        ← 4 models, all built
│           └── marts/               ← 5 models, all built
├── dashboard/
├── docs/
├── requirements.txt
├── .env
└── .env.example
```

---

## Airflow DAG Design (next to build)

Each pipeline DAG follows this task pattern:
```
trigger_airbyte_sync(connection_id) >> wait_for_sync >> run_dbt_staging >> run_dbt_intermediate >> run_dbt_marts
```

Connection IDs are read from `ingestion/config/connection_ids.yml` (generated by setup script).

DAGs to build:
- `dag_weather_pipeline.py` — OpenWeather sync + dbt run, every hour
- `dag_traffic_pipeline.py` — Traffic sync + dbt run, every hour
- `dag_daily_aggregations.py` — Full dbt run for mart models, daily at midnight

---

## Business Logic

### Temperature
- Connector uses `units=metric` → Celsius delivered directly from API
- Aggregations: daily avg/min/max, 7-day rolling avg, anomaly detection (> 2 std dev from 30-day avg)
- Implemented in: `mart_temperature_trends`

### AQI (OpenWeather scale 1–5)
| Value | Level | Action |
|---|---|---|
| 1 | Good | — |
| 2 | Fair | — |
| 3 | Moderate | Monitor |
| 4 | Poor | Alert |
| 5 | Very Poor | Alert + notify |

Alert rule: `hours_poor_air >= 3` (3+ hours of AQI ≥ 4 in a single day).
Implemented in: `mart_aqi_monitoring.aqi_alert`

### Smart City KPIs
- **Comfort Index**: `0.4 * norm_temp + 0.4 * (1 - norm_aqi) + 0.2 * norm_traffic`
- **Congestion Score**: `1 - (avg_speed / free_flow_speed)` (0=free flow, 1=standstill)
- **norm_temp**: `LEAST(GREATEST(avg_temp_celsius / 30.0, 0), 1)`
- **norm_aqi**: `(avg_aqi - 1) / 4.0`
- Implemented in: `int_composite_city_score`, `mart_smart_city_kpis`

### Rain Probability
- Source: forecast stream `pop` field (probability of precipitation, 0.0–1.0)
- Alert threshold: `pop > 0.7` for next 6 hours

---

## Environment Variables

```
# PostgreSQL — used by dbt and host applications
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=smart_city
POSTGRES_USER=postgres
POSTGRES_PASSWORD=<your password>

# APIs
OPENWEATHER_API_KEY=<from openweathermap.org>
TOMTOM_API_KEY=<from developer.tomtom.com>

# Airbyte — AIRBYTE_PG_HOST must be the LAN IP (not localhost)
AIRBYTE_PG_HOST=10.2.x.x
AIRBYTE_URL=http://localhost:8001
AIRBYTE_USERNAME=airbyte
AIRBYTE_PASSWORD=password
```

---

## Key Constraints

- PostgreSQL runs locally (not Docker) on port 5432
- `AIRBYTE_PG_HOST` must be the machine's LAN IP — Airbyte sync pods run inside Kind cluster
- `POSTGRES_HOST=localhost` is for dbt/scripts on the host only
- Airflow runs in Docker (not natively supported on Windows)
- dbt runs in Python venv on the host machine
- All timestamps stored as UTC
- Never manually edit `airbyte_raw` tables — Airbyte owns that schema
- One Airbyte source connection per city (lat/lon fixed per source config)
- `city` column is injected by Airbyte `AddFields` — rows synced before this change have NULL city
  and are filtered out in intermediate models
