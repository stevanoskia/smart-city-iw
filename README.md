# Smart City Analytics Pipeline

Real-time data pipeline for weather, air quality and traffic analytics across European cities.

---

## Architecture

```
OpenWeather API  ──┐
                   ├──► ingest.py ──► PostgreSQL (airbyte_raw)
TomTom API       ──┘         │
                             │
                        Airflow DAG (@hourly)
                             │
                        dbt staging models
                             │
                        dbt intermediate models
                             │
                        dbt marts (reporting)
```

---

## Tech Stack

| Tool | Purpose |
|------|---------|
| Python (`ingest.py`) | Fetches data from APIs |
| Apache Airflow (Docker) | Orchestration — runs pipeline every hour |
| dbt | Data transformation (staging → intermediate → marts) |
| PostgreSQL | Data warehouse |
| DBeaver | Database client |

---

## Cities Monitored

| City | Lat | Lon |
|------|-----|-----|
| London | 51.5074 | -0.1278 |
| Amsterdam | 52.3676 | 4.9041 |
| Berlin | 52.52 | 13.405 |
| Madrid | 40.4168 | -3.7038 |

---

## Data Collected (per city, every hour)

| Source | Data | Table |
|--------|------|-------|
| OpenWeather | Current weather | `airbyte_raw.current_weather` |
| OpenWeather | Air pollution / AQI | `airbyte_raw.air_pollution` |
| OpenWeather | 5-day forecast | `airbyte_raw.weather_forecast` |
| TomTom | Traffic flow | `airbyte_raw.traffic_flow` |
| TomTom | Traffic incidents | `airbyte_raw.traffic_incidents` |

---

## Project Structure

```
smart-city-iw/
├── ingestion/
│   ├── config.py        # Cities list + API keys + DB config
│   └── ingest.py        # Main ingestion script
├── dbt/smart_city/
│   └── models/
│       ├── staging/     # 5 staging models (done)
│       ├── intermediate/ # (in progress)
│       └── marts/       # (in progress)
├── airflow/
│   └── dags/
│       └── dbt_smart_city.py  # Airflow DAG
├── .env                 # API keys + DB credentials (gitignored)
└── SETUP_NOTES.md       # Detailed setup documentation
```

---

## Quick Start

**1. Start Docker Desktop**

**2. Start Airflow**
```powershell
cd D:\IWConnect\airflow
docker compose up -d
```
UI: http://localhost:8080 (airflow / airflow)

**3. Manual ingestion test (optional)**
```powershell
& "D:\IWConnect\smart-city-iw\venv\Scripts\Activate.ps1"
cd D:\IWConnect\smart-city-iw\ingestion
python ingest.py
```

---

## Adding a New City

In `ingestion/config.py`, add a line to `CITIES`:
```python
{"name": "Paris", "lat": 48.8566, "lon": 2.3522, "bbox": "2.22,48.81,2.47,48.90"},
```
Get bbox coordinates at: https://bboxfinder.com

---

## Environment Variables

Copy `.env.example` to `.env` and fill in:
```
OPENWEATHER_API_KEY=...
TOMTOM_API_KEY=...
POSTGRES_HOST=localhost
POSTGRES_PORT=5434
POSTGRES_DB=smart_city
POSTGRES_USER=postgres
POSTGRES_PASSWORD=...
```

---

## Setup Details

See [SETUP_NOTES.md](SETUP_NOTES.md) for full setup documentation including troubleshooting.
