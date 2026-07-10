# Smart City Analytics Pipeline — Project Guide

## Project Purpose

End-to-end ELT data engineering platform that automatically ingests weather, air pollution,
and transportation data from public APIs and transforms it into analytical models with dbt.
Simulates a real-world smart city analytics solution.

The live pipeline runs entirely on PostgreSQL:
Airbyte → `staging` (raw JSON, Airbyte-written) → dbt `intermediate` (incremental hourly
facts + forecast history) → dbt `marts`, orchestrated hourly by Airflow, with a separate
`@daily` maintenance DAG pruning old raw rows. The `stg_*` JSON-parsing models are **ephemeral**
(compile inline into their consumers as CTEs — no DB object), so `staging` holds only raw JSON.

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
| BI dashboard | Power BI — **in progress (blocked)**. Model (10 marts tables, clean star, 11 measures, dark theme) + 7 KPI cards built in `smart_city_dashboard.pbip`; **data refresh currently fails with a cyclic-reference error**. Full build log + open issue in `docs/powerbi_dashboard.md`. |
| Noise / energy APIs | Additional smart city data sources |

### Bonus (not in original scope)
| Task | Notes |
|---|---|
| AI-generated city summaries | Claude API reads `mart_city_daily` → daily narrative summaries (marts now available) |

### Recently Completed
- ✅ **Marts layer (star schema + OBT + analytics)** — 12 models in `models/marts/`: dims (`dim_city` *derived, no seed*; `dim_hour`; `dim_date`), daily facts (`fct_weather_daily`, `fct_pollution_daily`, `fct_traffic_daily`), `fct_traffic_hourly`, `fct_forecast_accuracy`, the derived OBT `mart_city_daily`, and analytics marts (`mart_forecast_latest`, `mart_temperature_trends`, `mart_weather_alerts`). `dbt build --select marts` green (57 nodes incl. relationships/unique/accepted_values tests); wired as the `dbt_marts` DAG step.
- ✅ **One Airbyte connection per API** — connectors are partition-routed (`ListPartitionRouter`) over a `locations` list, so a single connection (`openweather_all`, `tomtom_all`) ingests every city instead of one connection per city. Scales to many cities; Airflow + dbt unchanged.
- ✅ Expanded city coverage to **10 weather cities** (added Amsterdam, Belgrade, Brussels, Barcelona, Prilep, Bitola, Ohrid) and **6 traffic cities** (added Belgrade, Brussels, Barcelona); the 4 Macedonian cities are weather-only (no TomTom coverage)
- ✅ **Forecast** intermediate layer — incremental issue history (`int_city_weather_forecast`); the forward-looking *latest* (`mart_forecast_latest`) + prediction-vs-actual *accuracy* (`fct_forecast_accuracy`) models now live in the marts layer
- ✅ Incremental **hourly** intermediate layer (`int_city_hourly_*`) — preserves time-of-day + history; daily models roll up from it
- ✅ TomTom incidents `fields` fix — full incident detail now ingests (id, delay, magnitudeOfDelay, …)
- ✅ Split raw cleanup into a separate `@daily` `smart_city_maintenance` DAG
- ✅ Airflow XCom wait-task fix, on_failure_callback, per-task execution timeouts
- ✅ **Email alerts** — both DAGs email `ALERT_EMAIL` on failure (which task + error) and success
  (whole-pipeline / daily-cleanup done) via Gmail SMTP (`AIRFLOW__SMTP__*` env, App Password)

---

## Power BI Dashboard (started 2026-07-09 — BLOCKED)

Live work on `C:\Users\Andrej\Documents\smart_city_dashboard.pbip` (Power BI **project**/PBIP,
connected to PostgreSQL `marts`). **Full build log + the open blocker: `docs/powerbi_dashboard.md`.**

- **Done:** converted the project to TMDL (model) + PBIR (report); loaded 10 of 12 marts tables
  (missing `fct_traffic_hourly`, `fct_forecast_accuracy`); cleaned Power BI's auto-detected
  relationships into a proper star (deleted 4 fact-to-fact `city_date_key` links, activated the 6
  `fct_* → dim` links); added **11 measures** on `mart_city_daily`; applied the dark theme
  (`smart_city_theme.json`); authored the **7 KPI-card row** on Page 1.
- **⚠️ BLOCKED:** Home → Refresh fails with *"A cyclic reference was encountered during
  evaluation."* Removing the one custom calc column (`AQI Category (daily)`, self-qualified ref —
  a known trap) did **not** fix it; it now cites `mart_city_daily` + `mart_temperature_trends`.
  Because refresh fails, model data is stale → the Wind Speed / Rain Probability cards show blank
  (their source data exists in Postgres). **First thing to try:** disable Auto Date/Time (Options →
  Data Load) + delete the auto `LocalDateTable_*` tables. More diagnostics in
  `docs/powerbi_dashboard.md` §5.
- **Not done yet:** Azure Map, AQI gauge, pollutant cards, chance-of-rain bars, Sankeys, extra pages.

## Current Status (as of 2026-07-09)

### Infrastructure
| Component | Status | Notes |
|---|---|---|
| PostgreSQL 18 | ✅ Running | localhost:5432, DB: smart_city — ingestion/landing DB |
| Airbyte (abctl) | ✅ Running | localhost:8000, Kind/Kubernetes |
| Airbyte destination | ✅ Configured | smart_city_postgres → staging schema (raw JSON) |
| Airflow | ✅ Running | localhost:8080, DAG smart_city_pipeline deployed |

### Data Ingestion (APIs)
| API / Stream | Status | Cities | Notes |
|---|---|---|---|
| OpenWeather current weather | ✅ Working | Skopje, Berlin, London, Amsterdam, Belgrade, Brussels, Barcelona, Prilep, Bitola, Ohrid (10) | hourly sync |
| OpenWeather air pollution | ✅ Working | Skopje, Berlin, London, Amsterdam, Belgrade, Brussels, Barcelona, Prilep, Bitola, Ohrid (10) | hourly sync |
| OpenWeather 5-day forecast | ✅ Working | Skopje, Berlin, London, Amsterdam, Belgrade, Brussels, Barcelona, Prilep, Bitola, Ohrid (10) | hourly sync |
| TomTom traffic flow | ✅ Working | London, Berlin, Amsterdam, Belgrade, Brussels, Barcelona (6) | hourly sync |
| TomTom traffic incidents | ✅ Working | London, Berlin, Amsterdam, Belgrade, Brussels, Barcelona (6) | hourly sync; full detail via `fields` param |

> **10 weather cities, 6 traffic cities.** Traffic covers London, Berlin, Amsterdam, Belgrade,
> Brussels, Barcelona; the 4 Macedonian cities (Skopje, Prilep, Bitola, Ohrid) are weather/pollution
> only — TomTom has no segment/incident coverage there. Add a city in `ingestion/config/sources.yml`
> and re-run `setup_airbyte.py`.

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
| Airflow DAG `smart_city_maintenance` | ✅ Deployed | `@daily` — prunes old `staging` (raw JSON) rows per retention policy |
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
│ TomTom API       │───►│  Airbyte  │───►│  staging (raw) ◄── dbt* │
└──────────────────┘    │           │    │  intermediate  ◄── dbt │
                        └───────────┘    │  marts         ◄── dbt │
                             :8000       │  (*stg_* ephemeral)    │
                                         └────────────────────────┘
```

**Single-database ELT (current):** everything lives in one PostgreSQL database across three schemas.
- **`staging`** — Airbyte writes raw, append-only API-snapshot JSON here (short buffer). The
  `stg_*` dbt models parse this JSON but are **ephemeral** — they compile inline into `int_*`/
  `dim_city` as CTEs and create no DB object, so `staging` contains only the raw Airbyte tables.
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
| Landing DB | PostgreSQL 18 | localhost:5432 | staging (raw JSON) + intermediate + marts schemas |
| Transformation | dbt (Python venv313) | — | staging ephemeral parsing (stg_*) + intermediate (hourly facts + forecast history) + marts (star + OBT), tests |
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

# Compile staging (stg_* are ephemeral — no DB object; builds nothing physical, just validates)
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
| `staging` | current_weather, air_pollution, weather_forecast, traffic_flow, traffic_incidents (raw JSON) | Airbyte |
| _(ephemeral, no DB object)_ | stg_current_weather, stg_air_pollution, stg_weather_forecast, stg_traffic_flow, stg_traffic_incidents | dbt (ephemeral CTEs — compile inline) |
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
`dim_date` is an **independent** calendar spine (fixed 2026-01-01 anchor → `current_date + 365d`,
not bounded by the facts) so the dims resolve first; the fixed anchor still guarantees every
fact `date_key` exists in the dimension. `dim_hour` carries `hour_label` (`'06:00'`) + `day_part`.
`mart_city_daily` LEFT-joins weather+pollution+traffic so weather-only cities (Skopje, Prilep,
Bitola, Ohrid) appear with NULL traffic. Full spec + reference SQL in `docs/marts_build_guide.md`.

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

> **Short-lived tokens — poll loop re-auths.** Application access tokens expire in minutes.
> `airbyte_utils.py` caches the token in a module global, so a long sync (> token TTL)
> outlives the token cached at the start of `wait_for_sync`, and mid-poll the `jobs/get`
> call 401s. Because `HTTPError` subclasses `RequestException`, the poll loop's transient
> handler catches the 401 too — it now detects 401/403, clears the cached token so the next
> `_headers()` re-authenticates, and retries (instead of spinning on the dead token until the
> task's `execution_timeout` kills it — a 401 that looked like a slow sync). `wait_for_sync`'s
> default `timeout` is `2100`s (35 min), just under the wait task's 40-min `execution_timeout`,
> so its own `TimeoutError` (which names the `job_id`) surfaces before Airflow's generic kill.

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
- `max_active_runs=1` — **runs are serialized.** Worst-case duration (wait_syncs 40m +
  the three dbt steps 15m each) can exceed the hourly interval; without this the scheduler
  would start the next run while the current one is still writing, so two
  `dbt_intermediate`/`dbt_marts` tasks would `DELETE+INSERT` the same incremental Postgres
  tables concurrently (deadlocks / lost rows). `=1` queues the next run; `catchup=False`
  means a long run skips ahead rather than piling up.
- Triggers all Airbyte syncs in parallel (one task per connection in `connection_ids.yml` — now 2: `openweather_all`, `tomtom_all`)
- Waits for all syncs to complete
- Runs `dbt run --select staging --target staging`
- Runs `dbt build --select intermediate --target staging` (hourly facts + forecast history)
- Runs `dbt build --select marts --target staging` (star schema + OBT + analytics, build+test)
- **Email alerts:** `on_failure_callback` on every task (fires after retries — emails which
  step failed + the error); `on_success_callback` on the final `dbt_marts` task (one
  whole-pipeline SUCCESS email). Both guarded by `ALERT_EMAIL`; no-op if unset.

### DAG: `smart_city_maintenance`
- Schedule: `@daily`
- `max_active_runs=1` — serialized, so a slow prune can't overlap the next day's (both
  `DELETE` from the same `staging` tables and would race the pipeline's reads).
- Cleans up old `staging` (raw JSON) rows per retention policy (`RETENTION_DAYS`)
- Decoupled from the ELT pipeline so pruning runs regardless of any individual
  ELT run. Safe because deduped history is preserved downstream in the
  incremental `int_city_hourly_*` tables (raw is a short 1-day buffer).
- **Email alerts:** same pattern — failure email on the cleanup task, success email confirming
  the daily prune ran clean.

### Email alerts (both DAGs)
Failure/success notifications go to `ALERT_EMAIL` via `airflow.utils.email.send_email`. SMTP is
configured entirely through `AIRFLOW__SMTP__*` env vars (no `airflow.cfg` edit) — Gmail SMTP with a
16-char **App Password** (Google Account → Security → 2-Step Verification → App passwords), *not*
the account login. Callbacks are guarded by `if ALERT_EMAIL:`, so leaving it unset disables email
without breaking the DAGs. Each email carries a `Completed`/`Failed at` timestamp rendered in
local time (`ALERT_TZ`, default `Europe/Skopje`) — clearer than the `run_id`, which is UTC + the
data-interval start. On an eventual Airflow 3 upgrade, move the SMTP creds into an `smtp_default`
connection (env-var creds are deprecated there).

### Airflow env vars (from `airflow/.env` and docker-compose)
| Var | Purpose |
|---|---|
| `SMART_CITY_PG_HOST` | `host.docker.internal` — PostgreSQL from inside Docker |
| `SMART_CITY_PG_PASSWORD` | PostgreSQL password |
| `AIRBYTE_URL` | `http://host.docker.internal:8000` |
| `AIRBYTE_CLIENT_ID` | Airbyte OAuth client ID |
| `AIRBYTE_CLIENT_SECRET` | Airbyte OAuth client secret |
| `ALERT_EMAIL` | Recipient(s) for pipeline failure/success emails — comma-separate for several (unset = email disabled) |
| `ALERT_TZ` | Optional — tz for the email "Completed"/"Failed at" stamp (default `Europe/Skopje`, UTC fallback) |
| `AIRFLOW__SMTP__SMTP_HOST` … `_MAIL_FROM` | SMTP config (Gmail + App Password); see Environment Variables |

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

# Email alerts (Airflow reads AIRFLOW__SMTP__* straight from env)
ALERT_EMAIL=<inbox for pipeline alerts>   # one address, or several comma-separated
ALERT_TZ=Europe/Skopje   # optional — tz for the "Completed" stamp (UTC fallback)
AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com
AIRFLOW__SMTP__SMTP_PORT=587
AIRFLOW__SMTP__SMTP_STARTTLS=True
AIRFLOW__SMTP__SMTP_SSL=False
AIRFLOW__SMTP__SMTP_USER=<your gmail>
AIRFLOW__SMTP__SMTP_PASSWORD=<16-char Gmail App Password>
AIRFLOW__SMTP__SMTP_MAIL_FROM=<your gmail>
```

---

## Key Constraints

- Always use `venv313` (Python 3.13) — old `venv` (Python 3.8) has incompatible dbt pins
- PostgreSQL runs locally (not Docker) on port 5432
- `AIRBYTE_PG_HOST` must be LAN IP — Airbyte pods can't reach host `localhost`
- Airflow runs in Docker (not natively on Windows)
- dbt runs in `venv313` on the host machine (manual) OR inside Airflow container (automated)
- All timestamps stored as UTC
- Never manually edit the raw tables in `staging` (current_weather, air_pollution, …) — Airbyte owns them
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
│           ├── staging/         ← 5 stg_* JSON-parsing models → ephemeral (inline CTEs, no DB object)
│           ├── intermediate/    ← hourly facts (4) + forecast history (1) → tables
│           └── marts/           ← 12 models: dims + facts + OBT + analytics → tables
├── docs/
│   ├── staging_as_raw_landing.md     ← airbyte_raw→staging collapse: ephemeral parsing, JSON→typed, migration
│   ├── marts_build_guide.md          ← marts build walkthrough + reference SQL
│   └── marts_implementation_plan.md  ← marts star-schema design / rationale
├── venv313/                     ← Python 3.13 venv (use this one)
├── venv/                        ← Python 3.8 venv (legacy, do not use)
├── requirements.txt
├── .env
└── .env.example
```
