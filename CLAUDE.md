# Smart City Analytics Pipeline — Project Guide

## Project Purpose

End-to-end ELT data engineering platform that automatically ingests weather, air pollution,
and transportation data from public APIs and transforms it into analytical models with dbt.
Simulates a real-world smart city analytics solution.

The live pipeline runs entirely on PostgreSQL:
Airbyte → `airbyte_raw` → dbt `staging` (views) → dbt `intermediate` (incremental hourly
facts + forecast history), orchestrated hourly by Airflow, with a separate `@daily` maintenance
DAG pruning old raw rows.

> **Marts layer:** ✅ **built** (2026-07-01) — star schema (dims + facts) + derived OBT
> + analytics marts, all green (`dbt build --select marts`, relationships/unique/
> accepted_values tests pass) and orchestrated as the `dbt_marts` step in the hourly DAG.
> `dim_city` is **derived from data — no seed**. Design/rationale live in
> `docs/marts_implementation_plan.md`; the build walkthrough in `docs/marts_build_guide.md`.

---

## What Remains To Be Done

### Medium Priority (the marts now exist — these are unblocked)
| Task | Notes |
|---|---|
| BI dashboard | Power BI / Metabase — build on `mart_city_daily` + the analytics marts (spec deliverable) |
| Noise / energy APIs | Additional smart city data sources |

### Bonus (not in original scope)
| Task | Notes |
|---|---|
| AI-generated city summaries | Claude API reads `mart_city_daily` → daily narrative summaries (marts now available) |

### Recently Completed
- ✅ **Marts layer (star schema + OBT + analytics)** — 12 models in `models/marts/`: dims (`dim_city` *derived, no seed*; `dim_hour`; `dim_date`), daily facts (`fct_weather_daily`, `fct_pollution_daily`, `fct_traffic_daily`), `fct_traffic_hourly`, `fct_forecast_accuracy`, the derived OBT `mart_city_daily`, and analytics marts (`mart_forecast_latest`, `mart_temperature_trends`, `mart_weather_alerts`). `dbt build --select marts` green (57 nodes incl. relationships/unique/accepted_values tests); wired as the `dbt_marts` DAG step.
- ✅ **One Airbyte connection per API** — connectors are partition-routed (`ListPartitionRouter`) over a `locations` list, so a single connection (`openweather_all`, `tomtom_all`) ingests every city instead of one connection per city. Scales to many cities; Airflow + dbt unchanged.
- ✅ Added **Amsterdam + Prilep** to OpenWeather (5 weather cities; Macedonia has no TomTom traffic)
- ✅ **Forecast** intermediate layer — incremental issue history (`int_city_weather_forecast`); the forward-looking *latest* (`mart_forecast_latest`) + prediction-vs-actual *accuracy* (`fct_forecast_accuracy`) models now live in the marts layer
- ✅ Incremental **hourly** intermediate layer (`int_city_hourly_*`) — preserves time-of-day + history; daily models roll up from it
- ✅ TomTom incidents `fields` fix — full incident detail now ingests (id, delay, magnitudeOfDelay, …)
- ✅ Split raw cleanup into a separate `@daily` `smart_city_maintenance` DAG
- ✅ Airflow XCom wait-task fix, on_failure_callback, per-task execution timeouts

---

## Current Status (as of 2026-06-10)

### Infrastructure
| Component | Status | Notes |
|---|---|---|
| PostgreSQL 18 | ✅ Running | localhost:5432, DB: smart_city — ingestion/landing DB |
| Airbyte (abctl) | ✅ Running | localhost:8000, Kind/Kubernetes |
| Airbyte destination | ✅ Configured | smart_city_postgres → airbyte_raw schema |
| Airflow | ✅ Running | localhost:8080, DAG smart_city_pipeline deployed |

### Data Ingestion (APIs)
| API / Stream | Status | Cities | Notes |
|---|---|---|---|
| OpenWeather current weather | ✅ Working | Skopje, Berlin, London, Amsterdam, Prilep | hourly sync |
| OpenWeather air pollution | ✅ Working | Skopje, Berlin, London, Amsterdam, Prilep | hourly sync |
| OpenWeather 5-day forecast | ✅ Working | Skopje, Berlin, London, Amsterdam, Prilep | hourly sync |
| TomTom traffic flow | ✅ Working | London, Berlin, Amsterdam | hourly sync |
| TomTom traffic incidents | ✅ Working | London, Berlin, Amsterdam | hourly sync; full detail via `fields` param |

> Amsterdam + Prilep weather were added 2026-06-10; their first hourly sync backfills them.
> Macedonia (Skopje, Prilep) has no TomTom coverage → weather/pollution only.

### dbt Transformation
| Layer | DB | Model | Status |
|---|---|---|---|
| Staging | PostgreSQL | `stg_current_weather` | ✅ Built |
| Staging | PostgreSQL | `stg_air_pollution` | ✅ Built |
| Staging | PostgreSQL | `stg_weather_forecast` | ✅ Built |
| Staging | PostgreSQL | `stg_traffic_flow` | ✅ Built |
| Staging | PostgreSQL | `stg_traffic_incidents` | ✅ Built |
| Intermediate (hourly facts) | PostgreSQL | `int_city_hourly_weather` | ✅ Built (incremental) |
| Intermediate (hourly facts) | PostgreSQL | `int_city_hourly_pollution` | ✅ Built (incremental) |
| Intermediate (hourly facts) | PostgreSQL | `int_city_hourly_traffic_flow` | ✅ Built (incremental) |
| Intermediate (hourly facts) | PostgreSQL | `int_city_hourly_traffic_incidents` | ✅ Built (incremental) |
| Intermediate (forecast) | PostgreSQL | `int_city_weather_forecast` | ✅ Built (incremental issue history) |
| Marts (dims) | PostgreSQL | `dim_city` (derived), `dim_hour`, `dim_date` | ✅ Built |
| Marts (daily facts) | PostgreSQL | `fct_weather_daily`, `fct_pollution_daily`, `fct_traffic_daily` | ✅ Built |
| Marts (extra facts) | PostgreSQL | `fct_traffic_hourly`, `fct_forecast_accuracy` | ✅ Built |
| Marts (OBT + analytics) | PostgreSQL | `mart_city_daily`, `mart_forecast_latest`, `mart_temperature_trends`, `mart_weather_alerts` | ✅ Built |

### Orchestration
| Component | Status | Notes |
|---|---|---|
| Airflow DAG `smart_city_pipeline` | ✅ Deployed | Triggers all syncs → dbt staging → dbt intermediate → **dbt marts** (all build+test). |
| Airflow DAG `smart_city_maintenance` | ✅ Deployed | `@daily` — prunes old `airbyte_raw` rows per retention policy |
| Hourly schedule | ✅ Configured | `@hourly` via Airflow scheduler |
| Airbyte OAuth auth | ✅ Working | client_id/client_secret via Applications API |

---

## Architecture

```
                        ┌──────────────────────────────────────┐
                        │          Apache Airflow               │
                        │   smart_city_pipeline DAG (@hourly)  │
                        └──────┬───────────────┬───────────────┘
                               │ triggers sync  │ triggers dbt
                               ▼               ▼
┌──────────────────┐    ┌───────────┐    ┌────────────────────────┐
│ OpenWeather API  │    │           │    │  PostgreSQL 18         │
│ TomTom API       │───►│  Airbyte  │───►│  airbyte_raw           │
└──────────────────┘    │           │    │  staging       ◄── dbt │
                        └───────────┘    │  intermediate  ◄── dbt │
                             :8000       │  marts         ◄── dbt │
                                         └────────────────────────┘
```

**Single-database ELT (current):** everything lives in one PostgreSQL database across four schemas.
- **`airbyte_raw`** — Airbyte writes raw, append-only API snapshots here (short 14-day buffer).
- **`staging`** — dbt **views**: typed/cleaned, 1:1 with raw (no dedup, no aggregation).
- **`intermediate`** — durable dbt building blocks:
  - **Hourly facts** (`int_city_hourly_*`) — **incremental**, deduped to one row per observation
    `(city, observed_at)`. Append-only, so they accumulate clean hourly history forever,
    independent of raw pruning. The durable archive.
  - **Forecast issue history** (`int_city_weather_forecast`) — incremental, every prediction as
    issued; the building block the forecast marts consume.
- **`marts`** — ✅ built. Dimensions (`dim_city` *derived, no seed* / `dim_date` / `dim_hour`),
  daily facts (`fct_*_daily`), `fct_traffic_hourly`, `fct_forecast_accuracy`, the derived OBT
  `mart_city_daily`, and analytics marts (`mart_forecast_latest`, `mart_temperature_trends`,
  `mart_weather_alerts`). Star keys with `relationships` tests enforcing FK→dimension integrity.

| Layer | Tool | Location | Purpose |
|---|---|---|---|
| Ingestion | Airbyte (abctl) | localhost:8000 | API connectors, raw data load |
| Landing DB | PostgreSQL 18 | localhost:5432 | airbyte_raw + staging + intermediate + marts schemas |
| Transformation | dbt (Python venv313) | — | staging views + intermediate (hourly facts + forecast history) + marts (star + OBT), tests |
| Orchestration | Airflow (Docker) | localhost:8080 | DAG scheduling, automated pipeline + daily maintenance |

---

## Python Environment

**Always use `venv313` (Python 3.13) — NOT the old `venv` (Python 3.8).**
The old venv has incompatible dbt pins and will error on startup.

```bash
# Activate from project root
source venv313/Scripts/activate

# Or with full path from anywhere
source /c/Users/Andrej/Desktop/IWCONNECT-PRAKSA/smart-city-iw/venv313/Scripts/activate
```

---

## Running dbt (manually)

Always run from `dbt/smart_city/`. One target: `staging` → PostgreSQL (holds all schemas).

```bash
cd dbt/smart_city

# Build staging views
dbt run --select staging --target staging

# Build + test intermediate tables (hourly facts + forecast history)
dbt build --select intermediate --target staging

# Everything (staging → intermediate, in dependency order)
dbt build --select staging intermediate --target staging
```

`dbt build` runs models **and** their tests; `dbt run` builds without testing. (Once you
build the marts per `docs/marts_build_guide.md`, add `dbt build --select marts` to the
sequence. No `dbt seed` step — `dim_city` is derived from data, not a CSV.)

> Host runs dbt 1.11 and reads `~/.dbt/profiles.yml` (localhost). Because a `profiles.yml`
> also lives in the project dir (for Airflow/Docker, needs `SMART_CITY_PG_*` env vars), pass
> `--profiles-dir C:/Users/Andrej/.dbt` when running on the host so it doesn't pick up the
> container profile.

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
| `staging` | stg_current_weather, stg_air_pollution, stg_weather_forecast, stg_traffic_flow, stg_traffic_incidents | dbt (views) |
| `intermediate` (hourly facts) | int_city_hourly_weather, int_city_hourly_pollution, int_city_hourly_traffic_flow, int_city_hourly_traffic_incidents | dbt (incremental tables) |
| `intermediate` (forecast) | int_city_weather_forecast | dbt (incremental issue history) |
| `marts` | dim_city, dim_hour, dim_date, fct_weather_daily, fct_pollution_daily, fct_traffic_daily, fct_traffic_hourly, fct_forecast_accuracy, mart_city_daily, mart_forecast_latest, mart_temperature_trends, mart_weather_alerts | dbt (tables) |

**Hourly facts grain & keys:** one row per clock hour. Each model dedupes its staging source on the
stream's business key — `(city, date_trunc('hour', observed_at))` for weather/pollution/flow (key
`city_hour_key = md5(city|hour)`), keeping the **freshest reading in the hour** (`order by observed_at
desc, extracted_at desc`); `(city, incident_id, observed_at)` for incidents (key `city_incident_key =
md5(city|incident_id|observed_at)`, with `where incident_id is not null`). Hour-truncating both the
partition and the key means two syncs in one clock hour collapse to a single row (idempotent across
runs). `materialized='incremental'`, `delete+insert`, 6h lookback; carries `date_utc` + `hour_utc`
for time-of-day analysis. `unique`/`not_null` tests on the surrogate key.

**Marts grain & keys:** daily facts + OBT one row per `(city, date_utc)`, surrogate
`city_date_key = md5(city|date_utc)`; star keys `city_key = md5(city)`,
`date_key = YYYYMMDD::int`; `relationships` tests enforce FK→dimension integrity.
`dim_city` is **derived** from data (weather facts + traffic presence), not a seed.
`mart_city_daily` LEFT-joins weather+pollution+traffic so weather-only cities (Skopje, Prilep)
appear with NULL traffic. Full spec + reference SQL in `docs/marts_build_guide.md`.

dbt project root: `dbt/smart_city/`
Profiles: `~/.dbt/profiles.yml` (host) + `dbt/smart_city/profiles.yml` (Docker/Airflow)
Targets: `staging` → PostgreSQL (only)
Plan/design doc for the marts: `docs/marts_implementation_plan.md`

---

## Airbyte Setup

### Deployment
- Installed via `abctl` (Kubernetes/Kind), not docker-compose
- UI: `localhost:8000`
- Kubeconfig: `~/.airbyte/abctl/abctl.kubeconfig`

### Config-Driven Setup

```bash
# Set AIRBYTE_CLIENT_ID and AIRBYTE_CLIENT_SECRET in .env first
python ingestion/scripts/setup_airbyte.py
```

Outputs `ingestion/config/connection_ids.yml` with connection UUIDs for Airflow.

**One source + connection per API**, not per city: `openweather_all` and `tomtom_all`.
Each connector is partition-routed (`ListPartitionRouter`) over the `locations` array in
`sources.yml` — one API request per city per stream, all inside one sync. The request params
and the injected `city` column read the current partition (`stream_partition` / `stream_slice`)
instead of flat single-city config. **Add a city** = add a `locations` entry in `sources.yml`
and re-run the setup script (it updates the source config); no new connection, no DAG re-parse.

Config files: `ingestion/config/sources.yml`, `ingestion/config/connections.yml`
Connector YAMLs: `ingestion/connections/open_weather_free_2_5.yaml`, `ingestion/connections/tomtom_traffic.yaml`

### Auth
Airbyte API uses OAuth application tokens (not basic auth).
Get `client_id` / `client_secret` from Airbyte UI → User → Applications.
Set `AIRBYTE_CLIENT_ID` and `AIRBYTE_CLIENT_SECRET` in `.env`.

### Known quirks
- Destination host must be LAN IP (`AIRBYTE_PG_HOST`) — not localhost (sync pods run in Kind)
- Schema refresh may 403 on connector version change — delete and recreate the connection instead
- `city` column injected via `AddFields` — old rows synced before connector update have NULL city (filter with `WHERE city IS NOT NULL` in any downstream model that aggregates by city)
- TomTom incidentDetails v5 returns only `iconCategory` + geometry **unless** the `fields` query param lists the attributes — the `traffic_incidents` requester now sends it (fix). Editing the repo YAML alone has no effect: the connector must be **republished in the Airbyte Builder UI** to take effect.

---

## Airflow

### Starting Airflow
```bash
cd airflow
docker compose up -d     # start all services
docker compose down -v   # full teardown (wipes DB)

# First time setup (after teardown):
docker compose run --rm airflow-init
docker compose up -d
```

UI: `localhost:8080` — login: `admin / admin`

### DAG: `smart_city_pipeline`
- Schedule: `@hourly`
- Triggers all Airbyte syncs in parallel (one task per connection in `connection_ids.yml` — now 2: `openweather_all`, `tomtom_all`)
- Waits for all syncs to complete
- Runs `dbt run --select staging --target staging`
- Runs `dbt build --select intermediate --target staging` (hourly facts + forecast history)
- Runs `dbt build --select marts --target staging` (star schema + OBT + analytics, build+test)

### DAG: `smart_city_maintenance`
- Schedule: `@daily`
- Cleans up old `airbyte_raw` rows per retention policy (`RETENTION_DAYS`)
- Decoupled from the ELT pipeline so pruning runs regardless of any individual
  ELT run. Safe because deduped history is preserved downstream in the
  incremental `int_city_hourly_*` tables (raw is a short 14-day buffer).

### Airflow env vars (from `airflow/.env` and docker-compose)
| Var | Purpose |
|---|---|
| `SMART_CITY_PG_HOST` | `host.docker.internal` — PostgreSQL from inside Docker |
| `SMART_CITY_PG_PASSWORD` | PostgreSQL password |
| `AIRBYTE_URL` | `http://host.docker.internal:8000` |
| `AIRBYTE_CLIENT_ID` | Airbyte OAuth client ID |
| `AIRBYTE_CLIENT_SECRET` | Airbyte OAuth client secret |

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
AIRBYTE_URL=http://localhost:8000
AIRBYTE_USERNAME=<your email>
AIRBYTE_PASSWORD=<your password>
AIRBYTE_CLIENT_ID=<from Airbyte UI → User → Applications>
AIRBYTE_CLIENT_SECRET=<from Airbyte UI → User → Applications>
AIRBYTE_WORKSPACE_ID=<from Airbyte UI URL>
```

---

## Key Constraints

- Always use `venv313` (Python 3.13) — old `venv` (Python 3.8) has incompatible dbt pins
- PostgreSQL runs locally (not Docker) on port 5432
- `AIRBYTE_PG_HOST` must be LAN IP — Airbyte pods can't reach host `localhost`
- Airflow runs in Docker (not natively on Windows)
- dbt runs in `venv313` on the host machine (manual) OR inside Airflow container (automated)
- All timestamps stored as UTC
- Never manually edit `airbyte_raw` tables — Airbyte owns that schema
- `city` column injected by Airbyte `AddFields` — rows before this change have NULL city (filtered out)
- `airflow/.env` must exist with POSTGRES_PASSWORD, AIRBYTE_CLIENT_ID, AIRBYTE_CLIENT_SECRET

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
│   ├── Dockerfile               ← extends apache/airflow:2.9.3 with dbt
│   ├── docker-compose.yml
│   ├── .env                     ← POSTGRES_PASSWORD, AIRBYTE_* (not committed)
│   └── dags/
│       ├── airbyte_utils.py     ← OAuth trigger/wait helpers
│       ├── dag_smart_city_pipeline.py      ← hourly ELT
│       └── dag_smart_city_maintenance.py   ← daily raw cleanup
├── dbt/
│   └── smart_city/              ← dbt project root (run dbt here)
│       ├── dbt_project.yml
│       ├── profiles.yml         ← Docker/Airflow profiles (container paths)
│       ├── macros/
│       └── models/
│           ├── staging/         ← 5 models → PostgreSQL views
│           ├── intermediate/    ← hourly facts (4) + forecast history (1) → tables
│           └── marts/           ← 12 models: dims + facts + OBT + analytics → tables
├── docs/
│   ├── marts_build_guide.md          ← step-by-step DIY build + reference solutions
│   └── marts_implementation_plan.md  ← marts star-schema design / rationale
├── venv313/                     ← Python 3.13 venv (use this one)
├── venv/                        ← Python 3.8 venv (legacy, do not use)
├── requirements.txt
├── .env
└── .env.example
```
