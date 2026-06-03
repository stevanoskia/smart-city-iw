# Smart City Analytics Pipeline

End-to-end ELT platform that collects weather, air pollution, and transportation data
from public APIs, transforms it with dbt, orchestrates with Airflow, and serves
interactive smart city dashboards through Metabase.

---

## Architecture

```
OpenWeather API  ──┐
Traffic API (TBD)──┼──► Airbyte ──► PostgreSQL ──► dbt ──► Metabase
                   │      ↑              ↑
                   └──────┘         Airflow (orchestration)
```

| Layer | Tool | Status |
|---|---|---|
| Ingestion | Airbyte (abctl / Kubernetes) | ✅ Running |
| Storage | PostgreSQL 18 (local) | ✅ Running |
| Transformation | dbt-postgres | ✅ Staging models built |
| Orchestration | Apache Airflow | 🔜 Pending |
| Dashboard | Metabase | ✅ Running, dashboards pending |

---

## Data Sources

| Source | Data | Status |
|---|---|---|
| OpenWeather Current Weather | temperature, humidity, wind, pressure, rain | ✅ Flowing (Skopje, hourly) |
| OpenWeather Air Pollution | AQI, PM2.5, PM10, CO, NO2, O3 | ✅ Flowing (Skopje, hourly) |
| OpenWeather Forecast | 5-day forecast, rain probability | ❌ Not yet configured |
| Traffic API (TomTom/HERE) | congestion, speed, density | ❌ Not yet configured |

---

## Prerequisites

- Python 3.8+
- PostgreSQL 18 installed locally on port 5432
- Docker Desktop
- abctl (Airbyte CLI)
- Git

---

## Starting the Stack

### 1. Airbyte
```bash
abctl local deploy
# UI: http://localhost:8000  (takes ~2 min to be ready)
```

### 2. PostgreSQL
Starts automatically with Windows. Verify:
```bash
psql -U postgres -d smart_city -c "SELECT version();"
```
If not running: **Windows Services → postgresql-x64-18 → Start**

### 3. Metabase
```bash
docker start metabase
# or first time:
docker run -d --name metabase -p 3000:3000 metabase/metabase
# UI: http://localhost:3000
```

### 4. Airflow _(coming soon)_
```bash
cd airflow && docker compose up -d
# UI: http://localhost:8080  (admin / admin)
```

---

## Local Development Setup

```bash
# Clone
git clone https://github.com/stevanoskia/smart-city-iw.git
cd smart-city-iw

# Virtual environment
python -m venv venv
source venv/Scripts/activate        # Git Bash / WSL
# venv\Scripts\activate             # CMD / PowerShell

# Install dependencies
pip install --upgrade pip
pip install psycopg2-binary==2.9.9 --only-binary=psycopg2-binary
pip install dbt-postgres python-dotenv

# Credentials
cp .env.example .env
# Fill in POSTGRES_PASSWORD and OPENWEATHER_API_KEY
```

---

## Running dbt

```bash
cd dbt/smart_city
dbt debug                        # verify connection
dbt run --select staging         # run staging models
dbt run                          # run all models
dbt test                         # run data quality tests
```

---

## Services & Ports

| Service | URL | Credentials |
|---|---|---|
| Airbyte | http://localhost:8000 | airbyte / password |
| Airflow | http://localhost:8080 | admin / admin |
| Metabase | http://localhost:3000 | set during setup |
| PostgreSQL | localhost:5432 | postgres / (from .env) |

---

## Database Schemas

| Schema | Purpose |
|---|---|
| `airbyte_raw` | Raw API data loaded by Airbyte — never edit manually |
| `staging` | Cleaned, typed dbt views — 1:1 with raw tables |
| `intermediate` | Joined and aggregated dbt models |
| `marts` | Final analytics tables consumed by Metabase |

---

## What's been built

- Airbyte custom connector for OpenWeather free 2.5 API (YAML at `ingestion/connections/`)
- PostgreSQL destination wired to `airbyte_raw` schema
- Hourly sync of weather + air pollution data for Skopje
- dbt staging models: `stg_current_weather`, `stg_air_pollution`
- Metabase connected to `smart_city` database

## What's next

1. Add OpenWeather Forecast stream (5-day forecast, rain probability)
2. Add Traffic API source (TomTom or HERE)
3. Add more cities as additional Airbyte sources
4. Build dbt intermediate + mart models
5. Write Airflow DAGs
6. Build Metabase dashboards

---

## Project Structure

```
smart-city-iw/
├── ingestion/
│   ├── connections/                  # Airbyte connector YAML configs
│   └── README.md                     # Airbyte setup guide
├── airflow/
│   ├── docker-compose.yml
│   └── dags/
├── dbt/
│   └── smart_city/                   # dbt project root
│       ├── dbt_project.yml
│       ├── macros/
│       └── models/
│           ├── staging/
│           ├── intermediate/
│           └── marts/
├── dashboard/
├── .env                              # secrets (not committed)
└── .env.example
```
