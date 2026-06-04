# Smart City Analytics Pipeline

End-to-end ELT platform that automatically ingests weather, air pollution, and transportation
data from public APIs, transforms it with dbt, orchestrates with Airflow, and serves
smart city dashboards through Power BI.

---

## Architecture

```
OpenWeather API  --+
TomTom API  -------+--> Airbyte --> PostgreSQL (landing) --> dbt --> DuckDB (warehouse) --> Power BI
                   |                    staging                      intermediate + marts
                   +-----------------------------------------------------------
                               Airflow (smart_city_pipeline DAG, @hourly)
```

| Layer | Tool |
|---|---|
| Ingestion | Airbyte (abctl / Kubernetes)
| Landing DB | PostgreSQL 18 (local, port 5432) 
| Transformation | dbt-postgres + dbt-duckdb
| Warehouse | DuckDB (warehouse/smart_city.duckdb)
| Orchestration | Apache Airflow (Docker, port 8080)
| Visualization | Power BI Desktop (ODBC to DuckDB)

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
pip install dbt-postgres==1.8.2 dbt-duckdb==1.8.4 psycopg2-binary \
            python-dotenv requests pyyaml
cp .env.example .env   # fill in credentials
```

### 2. Configure dbt profiles
See `~/.dbt/profiles.yml` — two targets: `staging` (PostgreSQL) and `warehouse` (DuckDB).
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
dbt run --select staging --target staging
dbt run --select intermediate marts --target warehouse
```

### 6. Start Airflow
```bash
cd airflow
# First time only — initialises the Airflow DB and creates the admin user
docker compose run --rm airflow-init
docker compose up -d
# UI: localhost:8080  (admin / admin)
# Enable DAG: smart_city_pipeline
```

### 7. Connect Power BI
- Install DuckDB ODBC driver from duckdb.org/docs/api/odbc/windows
- Get Data -> ODBC -> `Driver={DuckDB Driver};Database=<path>\warehouse\smart_city.duckdb;access_mode=read_only`
- Load 5 tables from the `marts` schema

---

## What's Built

### Pipeline
- **14 dbt models** across 3 layers, 37 tests passing
- **Airflow DAG** `smart_city_pipeline` — triggers 6 Airbyte syncs in parallel, then runs dbt staging + warehouse
- **Airbyte setup script** — `ingestion/scripts/setup_airbyte.py` adds new cities from config without UI

### Analytics (DuckDB `marts` schema)
| Table | Description |
|---|---|
| `mart_temperature_trends` | Daily temp per city with 7/30-day rolling avg and anomaly flags |
| `mart_aqi_monitoring` | AQI labels, 3-hour poor-air alerts, 7-day trend |
| `mart_traffic_density` | Congestion labels, speed ratio, rolling avg |
| `mart_city_comparison` | All cities ranked daily by comfort, AQI, temp, congestion |
| `mart_smart_city_kpis` | Headline comfort index, livability score, alert flags |

### Business Logic
- **Comfort Index**: `0.4 * norm_temp + 0.4 * (1 - norm_aqi) + 0.2 * norm_traffic` (0-1)
- **AQI Alert**: triggered when a city has 3+ hours of AQI >= 4 in a single day
- **Anomaly detection**: temperature > 2 standard deviations from 30-day rolling mean

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
| DuckDB | warehouse/smart_city.duckdb | file-based |

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
│       ├── airbyte_utils.py            <- OAuth trigger/wait helpers
│       └── dag_smart_city_pipeline.py  <- main hourly DAG
├── dbt/smart_city/      <- dbt project root (run all dbt commands here)
│   └── models/
│       ├── staging/     -> PostgreSQL (5 views)
│       ├── intermediate/ -> DuckDB (4 views)
│       └── marts/       -> DuckDB (5 tables, Power BI reads here)
├── warehouse/           <- DuckDB file lives here (git-ignored)
├── venv313/             <- Python 3.13 venv (always use this)
└── .env                 <- secrets (not committed)
```
