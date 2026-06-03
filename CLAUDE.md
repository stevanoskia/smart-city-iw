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
| PostgreSQL 18 | ✅ Running | localhost:5432, DB: smart_city — ingestion/landing DB |
| Airbyte (abctl) | ✅ Running | localhost:8000, Kind/Kubernetes |
| Airbyte destination | ✅ Configured | smart_city_postgres → airbyte_raw schema |
| DuckDB warehouse | ✅ Running | warehouse/smart_city.duckdb — analytics warehouse |
| Power BI Desktop | 🔜 Pending | ODBC driver install needed |
| Airflow | 🔜 Pending | docker-compose ready at airflow/, DAGs not written |

### Data Ingestion (APIs)
| API / Stream | Status | Cities | Notes |
|---|---|---|---|
| OpenWeather current weather | ✅ Working | Skopje, Berlin, London | hourly sync |
| OpenWeather air pollution | ✅ Working | Skopje, Berlin, London | hourly sync |
| OpenWeather 5-day forecast | ✅ Working | Skopje, Berlin, London | hourly sync |
| TomTom traffic flow | ✅ Working | London, Berlin, Amsterdam | hourly sync |
| TomTom traffic incidents | ✅ Working | London, Berlin, Amsterdam | hourly sync |

### dbt Transformation
| Layer | DB | Model | Status |
|---|---|---|---|
| Staging | PostgreSQL | `stg_current_weather` | ✅ Built |
| Staging | PostgreSQL | `stg_air_pollution` | ✅ Built |
| Staging | PostgreSQL | `stg_weather_forecast` | ✅ Built |
| Staging | PostgreSQL | `stg_traffic_flow` | ✅ Built |
| Staging | PostgreSQL | `stg_traffic_incidents` | ✅ Built |
| Intermediate | DuckDB | `int_city_daily_weather` | ✅ Built |
| Intermediate | DuckDB | `int_city_daily_pollution` | ✅ Built |
| Intermediate | DuckDB | `int_city_daily_traffic` | ✅ Built |
| Intermediate | DuckDB | `int_composite_city_score` | ✅ Built |
| Marts | DuckDB | `mart_temperature_trends` | ✅ Built |
| Marts | DuckDB | `mart_aqi_monitoring` | ✅ Built |
| Marts | DuckDB | `mart_traffic_density` | ✅ Built |
| Marts | DuckDB | `mart_city_comparison` | ✅ Built |
| Marts | DuckDB | `mart_smart_city_kpis` | ✅ Built |

### Dashboards
| Feature | Status |
|---|---|
| Power BI connected to DuckDB | 🔜 ODBC driver pending |
| Temperature trend charts | ❌ Not built |
| AQI monitoring widgets | ❌ Not built |
| Traffic density charts | ❌ Not built |
| City comparison view | ❌ Not built |
| Smart city KPI cards | ❌ Not built |
| Smart alerts (bonus) | ❌ Not started |
| AI-generated summaries (bonus) | ❌ Not started |

---

## Architecture

```
                        ┌──────────────────────────────────────┐
                        │          Apache Airflow               │
                        │   (orchestrates every step below)     │
                        └──────┬───────────────┬───────────────┘
                               │ triggers sync  │ triggers dbt
                               ▼               ▼
┌──────────────────┐    ┌───────────┐    ┌────────────────────┐
│ OpenWeather API  │    │           │    │  PostgreSQL 18     │
│ TomTom API       │───►│  Airbyte  │───►│  airbyte_raw       │
└──────────────────┘    │           │    │  staging           │
                        └───────────┘    └─────────┬──────────┘
                             :8000               reads via
                                                postgres ATTACH
                                                     ▼
                                        ┌────────────────────┐
                                        │  DuckDB warehouse  │
                                        │  intermediate      │
                                        │  marts             │──► Power BI
                                        └────────────────────┘
```

**Two-tier ELT:**
- **PostgreSQL** — ingestion/landing database. Airbyte writes raw data here. dbt builds staging views here.
- **DuckDB** — analytics warehouse. dbt reads PostgreSQL staging via postgres ATTACH extension and materializes intermediate views + mart tables here. Power BI reads from marts.

| Layer | Tool | Location | Purpose |
|---|---|---|---|
| Ingestion | Airbyte (abctl) | localhost:8000 | API connectors, raw data load |
| Landing DB | PostgreSQL 18 | localhost:5432 | airbyte_raw + staging schemas |
| Warehouse | DuckDB | warehouse/smart_city.duckdb | intermediate + marts schemas |
| Transformation | dbt (Python venv313) | — | SQL models, tests |
| Orchestration | Airflow (Docker) | localhost:8080 | DAG scheduling (pending) |
| Visualization | Power BI Desktop | — | Dashboards (pending) |

---

## Python Environment

**Always use `venv313` (Python 3.13) — NOT the old `venv` (Python 3.8).**
The old venv does not have dbt-duckdb and will error on startup.

```bash
# Activate from project root
source venv313/Scripts/activate

# Or with full path from anywhere
source /c/Users/Andrej/Desktop/IWCONNECT-PRAKSA/smart-city-iw/venv313/Scripts/activate
```

---

## Running dbt

Always run from `dbt/smart_city/`. Two separate commands, two targets.

```bash
cd dbt/smart_city

# Step 1 — staging in PostgreSQL
dbt run --select staging --target staging

# Step 2 — intermediate + marts in DuckDB
dbt run --select intermediate marts --target warehouse

# Full pipeline in one line
dbt run --select staging --target staging && dbt run --select intermediate marts --target warehouse

# Tests
dbt test --select staging --target staging
dbt test --select intermediate marts --target warehouse
```

---

## APIs

**OpenWeather Free 2.5** (`OPENWEATHER_API_KEY`)
| Endpoint | Stream | Fields |
|---|---|---|
| `/data/2.5/weather` | `current_weather` | temp_celsius, humidity, wind_speed, pressure, weather_main, rain_1h |
| `/data/2.5/air_pollution` | `air_pollution` | aqi (1-5), pm2_5, pm10, co, no2, o3, so2, nh3 |
| `/data/2.5/forecast` | `weather_forecast` | forecast_dt, temp, pop (rain probability), weather_main |

**TomTom Traffic** (`TOMTOM_API_KEY`)
| Endpoint | Stream | Fields |
|---|---|---|
| `/traffic/services/4/flowSegmentData` | `traffic_flow` | currentSpeed, freeFlowSpeed, congestion_score, frc |
| `/traffic/services/5/incidentDetails` | `traffic_incidents` | id, delay, magnitudeOfDelay, geometry |

---

## Database Layout

### PostgreSQL — ingestion/landing

| Schema | Tables | Owner |
|---|---|---|
| `airbyte_raw` | current_weather, air_pollution, weather_forecast, traffic_flow, traffic_incidents | Airbyte |
| `staging` | stg_current_weather, stg_air_pollution, stg_weather_forecast, stg_traffic_flow, stg_traffic_incidents | dbt |

### DuckDB — analytics warehouse (`warehouse/smart_city.duckdb`)

| Schema | Objects | Type |
|---|---|---|
| `intermediate` | int_city_daily_weather, int_city_daily_pollution, int_city_daily_traffic, int_composite_city_score | views |
| `marts` | mart_temperature_trends, mart_aqi_monitoring, mart_traffic_density, mart_city_comparison, mart_smart_city_kpis | tables |
| `postgres_staging` | (attached read-only view of PostgreSQL staging) | attached |

dbt project root: `dbt/smart_city/`
Profiles: `~/.dbt/profiles.yml` (targets: `staging` → PostgreSQL, `warehouse` → DuckDB)
Schema macro: `macros/generate_schema_name.sql`

---

## Airbyte Setup

### Deployment
- Installed via `abctl` (Kubernetes/Kind), not docker-compose
- UI: `localhost:8000`
- Kubeconfig: `~/.airbyte/abctl/abctl.kubeconfig`

### Config-Driven Setup (recommended)

```bash
# First add AIRBYTE_PG_HOST=<your LAN IP> to .env
pip install requests pyyaml
python ingestion/scripts/setup_airbyte.py
```

Outputs `ingestion/config/connection_ids.yml` with connection UUIDs for Airflow.

Config files: `ingestion/config/sources.yml`, `ingestion/config/connections.yml`
Connector YAMLs: `ingestion/connections/open_weather_free_2_5.yaml`, `ingestion/connections/tomtom_traffic.yaml`

### Known quirks
- Destination host must be LAN IP (`AIRBYTE_PG_HOST`) — not localhost (sync pods run in Kind)
- Schema refresh may 403 on connector version change — delete and recreate the connection instead
- `city` column injected via `AddFields` — old rows synced before connector update have NULL city (filtered in intermediate models with `WHERE city IS NOT NULL`)

---

## Power BI Connection

1. Download **DuckDB ODBC driver for Windows**: https://duckdb.org/docs/api/odbc/windows
2. Run the installer
3. Open **Power BI Desktop** → Get Data → ODBC
4. Connection string: `Driver={DuckDB Driver};Database=C:\Users\Andrej\Desktop\IWCONNECT-PRAKSA\smart-city-iw\warehouse\smart_city.duckdb`
5. Navigate to `marts` schema → load the 5 mart tables

---

## Airflow DAG Design (next to build)

Connection IDs from `ingestion/config/connection_ids.yml` (run `setup_airbyte.py` first).

```
trigger_airbyte_sync(connection_id) >> wait_for_sync >> dbt_staging >> dbt_warehouse
```

DAGs to build:
- `dag_weather_pipeline.py` — OpenWeather sync + dbt, every hour
- `dag_traffic_pipeline.py` — Traffic sync + dbt, every hour
- `dag_daily_aggregations.py` — Full dbt run, daily at midnight

---

## Business Logic

### AQI Alert Rule
`aqi_alert = hours_poor_air >= 3` (3+ hours of AQI ≥ 4 in a single day)
Implemented in `mart_aqi_monitoring.aqi_alert`

### Comfort Index
`0.4 * norm_temp + 0.4 * (1 - norm_aqi) + 0.2 * norm_traffic`
- `norm_temp = LEAST(GREATEST(avg_temp_celsius / 30.0, 0), 1)`
- `norm_aqi = (avg_aqi - 1) / 4.0`
- `norm_traffic = 1.0 - COALESCE(avg_congestion_score, 0.5)`
Implemented in `int_composite_city_score`, surfaced in `mart_smart_city_kpis`

---

## Environment Variables

```
# PostgreSQL (used by dbt staging target + host applications)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=smart_city
POSTGRES_USER=postgres
POSTGRES_PASSWORD=<your password>

# APIs
OPENWEATHER_API_KEY=<from openweathermap.org>
TOMTOM_API_KEY=<from developer.tomtom.com>

# Airbyte
AIRBYTE_PG_HOST=10.2.x.x   # LAN IP — NOT localhost
AIRBYTE_URL=http://localhost:8001
AIRBYTE_USERNAME=airbyte
AIRBYTE_PASSWORD=password
```

---

## Key Constraints

- Always use `venv313` (Python 3.13) — old `venv` (Python 3.8) is incompatible with dbt-duckdb
- PostgreSQL runs locally (not Docker) on port 5432
- `AIRBYTE_PG_HOST` must be LAN IP — Airbyte pods can't reach host `localhost`
- DuckDB warehouse is a local file — not committed to git
- Airflow runs in Docker (not natively on Windows)
- dbt runs in `venv313` on the host machine
- All timestamps stored as UTC
- Never manually edit `airbyte_raw` tables — Airbyte owns that schema
- `city` column injected by Airbyte `AddFields` — rows before this change have NULL city (filtered out)

---

## Folder Structure

```
smart-city-iw/
├── ingestion/
│   ├── config/
│   │   ├── sources.yml          ← city/coordinate config
│   │   ├── connections.yml      ← sync schedule, destination
│   │   └── connection_ids.yml   ← auto-generated, git-ignored
│   ├── connections/
│   │   ├── open_weather_free_2_5.yaml
│   │   └── tomtom_traffic.yaml
│   ├── scripts/
│   │   └── setup_airbyte.py
│   └── README.md
├── airflow/
│   ├── docker-compose.yml
│   └── dags/                    ← DAGs to be written
├── dbt/
│   └── smart_city/              ← dbt project root (run dbt here)
│       ├── dbt_project.yml
│       ├── macros/
│       └── models/
│           ├── staging/         ← 5 models → PostgreSQL
│           ├── intermediate/    ← 4 models → DuckDB
│           └── marts/           ← 5 tables → DuckDB (Power BI reads here)
├── warehouse/
│   └── smart_city.duckdb        ← git-ignored, rebuilt by dbt
├── venv313/                     ← Python 3.13 venv (use this one)
├── venv/                        ← Python 3.8 venv (legacy, do not use)
├── requirements.txt
├── .env
└── .env.example
```
