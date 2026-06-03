# Smart City Analytics Pipeline

End-to-end ELT platform that collects weather, air pollution, and transportation data
from public APIs, transforms it with dbt, and serves smart city dashboards through Power BI.

---

## Architecture

```
OpenWeather API  ──┐
TomTom API  ───────┼──► Airbyte ──► PostgreSQL (landing) ──► dbt ──► DuckDB (warehouse) ──► Power BI
                   │                    staging                        intermediate + marts
                   └───────────────────────────────────────────────────────────────────────
                                     Airflow (orchestration — pending)
```

| Layer | Tool | Status |
|---|---|---|
| Ingestion | Airbyte (abctl / Kubernetes) | ✅ Running |
| Landing DB | PostgreSQL 18 (local) | ✅ Running |
| Transformation | dbt-postgres + dbt-duckdb | ✅ All models built |
| Warehouse | DuckDB (local file) | ✅ Running |
| Orchestration | Apache Airflow | 🔜 Pending |
| Visualization | Power BI Desktop | 🔜 ODBC setup pending |

---

## Data Sources

| Source | Streams | Cities | Status |
|---|---|---|---|
| OpenWeather Free 2.5 | current weather, air pollution, forecast | Skopje, Berlin, London | ✅ Flowing |
| TomTom Traffic | traffic flow, traffic incidents | London, Berlin, Amsterdam | ✅ Flowing |

---

## Quick Start

### Prerequisites
- Python 3.13
- PostgreSQL 18 (local, port 5432)
- Docker Desktop
- abctl (Airbyte CLI)

### Setup

```bash
git clone https://github.com/stevanoskia/smart-city-iw.git
cd smart-city-iw

# Create venv with Python 3.13
py -3.13 -m venv venv313
source venv313/Scripts/activate

# Install dependencies
pip install dbt-postgres==1.8.2 dbt-duckdb==1.8.4 psycopg2-binary \
            python-dotenv requests pyyaml

# Configure credentials
cp .env.example .env
# Fill in POSTGRES_PASSWORD, OPENWEATHER_API_KEY, TOMTOM_API_KEY, AIRBYTE_PG_HOST
```

### Configure dbt profiles

Add to `~/.dbt/profiles.yml`:

```yaml
smart_city:
  outputs:
    staging:
      type: postgres
      host: localhost
      port: 5432
      dbname: smart_city
      user: postgres
      pass: <your password>
      schema: staging
      threads: 4
    warehouse:
      type: duckdb
      path: "<absolute path>/warehouse/smart_city.duckdb"
      schema: marts
      threads: 1
      extensions:
        - postgres
      attach:
        - path: "host=localhost port=5432 dbname=smart_city user=postgres password=<your password>"
          alias: pg_landing
          type: postgres
          read_only: true
  target: staging
```

### Run the pipeline

```bash
cd dbt/smart_city

# Build staging in PostgreSQL
dbt run --select staging --target staging

# Build intermediate + marts in DuckDB
dbt run --select intermediate marts --target warehouse
```

### Connect Power BI

1. Download the **DuckDB ODBC driver for Windows** from https://duckdb.org/docs/api/odbc/windows
2. Run the installer
3. Power BI Desktop → Get Data → ODBC
4. Connection string: `Driver={DuckDB Driver};Database=<path>\warehouse\smart_city.duckdb`
5. Load tables from the `marts` schema

---

## Services & Ports

| Service | URL | Credentials |
|---|---|---|
| Airbyte | http://localhost:8000 | airbyte / password |
| Airflow | http://localhost:8080 | admin / admin (pending) |
| PostgreSQL | localhost:5432 | postgres / (from .env) |
| DuckDB | warehouse/smart_city.duckdb | file-based |

---

## Database Schemas

| Database | Schema | Purpose |
|---|---|---|
| PostgreSQL | `airbyte_raw` | Raw API data — written by Airbyte, never edit |
| PostgreSQL | `staging` | Typed dbt views, 1:1 with raw tables |
| DuckDB | `intermediate` | Daily aggregations per city (dbt views) |
| DuckDB | `marts` | Final analytics tables — Power BI reads here |

---

## Airbyte Setup (config-driven)

```bash
# Add AIRBYTE_PG_HOST=<your LAN IP> to .env first
python ingestion/scripts/setup_airbyte.py
```

Creates all sources, destinations, and connections from `ingestion/config/sources.yml`.
Outputs `ingestion/config/connection_ids.yml` with UUIDs needed for Airflow DAGs.

---

## Project Structure

```
smart-city-iw/
├── ingestion/         # Airbyte connector YAMLs + config-driven setup script
├── airflow/           # Airflow docker-compose + DAGs (pending)
├── dbt/smart_city/    # dbt project root — run all dbt commands here
│   └── models/
│       ├── staging/       → PostgreSQL
│       ├── intermediate/  → DuckDB
│       └── marts/         → DuckDB (Power BI reads here)
├── warehouse/         # DuckDB file lives here (git-ignored)
├── venv313/           # Python 3.13 venv (always use this)
└── .env               # secrets (not committed)
```
