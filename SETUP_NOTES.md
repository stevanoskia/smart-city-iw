# Smart City Analytics Pipeline - Setup Notes
## Last Updated: 2026-06-25

---

## MACHINE INFO
- OS: Windows 11
- WSL: Ubuntu (moved to D:\WSL\Ubuntu)
- Python: 3.12.x (via venv) — 3.14 NOT COMPATIBLE with dbt
- Project path: D:\IWConnect\smart-city-iw

---

## 1. AIRBYTE — INSTALLATION & ISSUES

### Problem
- Airbyte is installed with `abctl` (Kubernetes/kind via Docker)
- After every restart, PostgreSQL pod (`airbyte-db-0`) fails with:
  `mkdir: can't create directory '/var/lib/postgresql/data/pgdata': Permission denied`

### Fix (run after every restart)
In WSL terminal:
```bash
export KUBECONFIG=/home/irina/.airbyte/abctl/abctl.kubeconfig
docker exec airbyte-abctl-control-plane chmod 777 /var/local-path-provisioner/airbyte-volume-db
docker exec airbyte-abctl-control-plane chmod 777 /var/local-path-provisioner/airbyte-local-pv
```
Wait 2-3 minutes for all pods to start.

### Airbyte Login
- URL: http://localhost:8000
- Nginx basic auth: password only (no username)
- Password: see local `.env` file

### If it still doesn't work
```bash
export KUBECONFIG=/home/irina/.airbyte/abctl/abctl.kubeconfig
kubectl get pods -n airbyte-abctl          # check status
kubectl delete pod airbyte-db-0 -n airbyte-abctl   # restart DB if needed
abctl local install                         # full reinstall if needed
```

---

## 2. POSTGRESQL — LOCAL DATABASE

### Installation
- Installed directly on Windows: `D:\postgre\`
- Data directory: `D:\postgre\data\`
- Port: **5434** (5432 was taken)
- User: `postgres`
- Password: see local `.env` file
- Service name: `postgresql-x64-18`

### Databases
- `smart_city` — for ingestion destination + dbt
- `airflow` — for Airflow metadata (old WSL version)

### pg_hba.conf
Added lines in `D:\postgre\data\pg_hba.conf`:
```
host    all    all    10.2.0.0/16    scram-sha-256
host    all    all    172.26.0.0/16  scram-sha-256
```
First allows connections from Kubernetes/Airbyte, second from WSL.

### Restart service
```powershell
Restart-Service postgresql-x64-18
```

### DBeaver connection
- Host: localhost
- Port: 5434
- Database: smart_city
- Username: postgres
- Password: see `.env`

---

## 3. AIRBYTE CONNECTIONS — CONFIGURED (backup, replaced by ingest.py)

### Destination
- Name: `smart_city_postgres`
- Host: `<WIFI_IP>` (may change after restart!)
- Port: 5434
- Database: smart_city
- Schema: airbyte_raw
- Username: postgres

### Sources (Custom Connectors)
1. **OpenWeather Free 2.5** — custom YAML connector
   - Streams: `current_weather`, `air_pollution`
   - Config: API key, lat, lon

2. **TomTom Traffic** — custom YAML connector
   - Streams: traffic data

### Original Connections (replaced by ingest.py)
| Source | City | Destination | Sync |
|--------|------|-------------|------|
| OpenWeather Free 2.5 | Berlin | smart_city_postgres | 1h |
| OpenWeather Free 2.5 | London | smart_city_postgres | 1h |
| OpenWeather Free 2.5 | Amsterdam | smart_city_postgres | 1h |
| TomTom Traffic | Berlin | smart_city_postgres | 1h |
| TomTom Traffic | London | smart_city_postgres | 1h |
| TomTom Traffic | Amsterdam | smart_city_postgres | 1h |

### Tables in airbyte_raw schema
- `air_pollution`
- `current_weather`
- `traffic_flow`
- `traffic_incidents`
- `weather_forecast`

---

## 4. DBT — CONFIGURED AND WORKING

### Virtual Environment (Windows)
```powershell
# IMPORTANT: use Python 3.12 — Python 3.14 is NOT compatible with dbt
# One-time setup:
py -3.12 -m venv C:\Users\Iwi\dbt-env
C:\Users\Iwi\dbt-env\Scripts\activate
pip install dbt-postgres

# Activate every session:
C:\Users\Iwi\dbt-env\Scripts\activate
cd "D:\IWConnect\smart-city-iw\dbt\smart_city"
```

### Virtual Environment (WSL — for running dbt manually in WSL)
```bash
source ~/airflow-venv/bin/activate
cd /mnt/d/IWConnect/smart-city-iw/dbt/smart_city
dbt debug
```

### profiles.yml
Location: `C:\Users\Iwi\.dbt\profiles.yml`
```yaml
smart_city:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      port: 5434
      user: postgres
      password: <POSTGRES_PASSWORD>
      dbname: smart_city
      schema: public
      threads: 4
```

### dbt commands
```powershell
dbt debug   # test connection
dbt run     # run models
dbt test    # run tests
```

### Staging models (CREATED - all 5 PASS)
- `staging.stg_air_pollution`
- `staging.stg_current_weather`
- `staging.stg_traffic_flow`
- `staging.stg_traffic_incidents`
- `staging.stg_weather_forecast`

### Intermediate models (CREATED - all 5 PASS as incremental)
- `intermediate.int_current_weather_hourly`
- `intermediate.int_air_quality_hourly`
- `intermediate.int_traffic_flow_hourly`
- `intermediate.int_traffic_incidents_hourly`
- `intermediate.int_weather_forecast_daily`

**Incremental strategy:** `delete+insert` on `(city, country, observed_hour)`
**Lookback:** 24h with COALESCE fallback (safe on empty table / first run)

### Seeds (CREATED)
- `marts.dim_city` — 4 cities with lat/lon/country/has_traffic_data/city_timezone
- File: `seeds/dim_city.csv`
- Run with: `dbt seed`

### Marts (NOT YET BUILT — next priority)
See WHAT REMAINS section.

---

## 5. AIRFLOW — DOCKER (ACTIVE VERSION)

### Location
- docker-compose.yaml: `D:\IWConnect\airflow\docker-compose.yaml`
- DAG file: `D:\IWConnect\airflow\dags\dbt_smart_city.py`
- dbt profiles for Docker: `D:\IWConnect\airflow\config\profiles.yml`

### Startup (from D:\IWConnect\airflow)
```powershell
cd D:\IWConnect\airflow
docker compose up -d
```
UI: http://localhost:8080 (airflow/airflow)

### Restart (after changes to docker-compose or DAG)
```powershell
cd D:\IWConnect\airflow
docker compose down
docker compose up -d
```

### How it works
- Docker mounts `D:/IWConnect/smart-city-iw` to `/opt/smart-city` in the container
- dbt profiles.yml is in `D:\IWConnect\airflow\config\profiles.yml` (mounted to `/opt/airflow/config`)
- dbt connects to PostgreSQL via `host.docker.internal:5434`
- dbt + ingestion packages installed at startup via `_PIP_ADDITIONAL_REQUIREMENTS: dbt-core==1.8.2 dbt-postgres==1.8.2 requests psycopg2-binary python-dotenv`
- IMPORTANT: must pin `dbt-core==1.8.2` — dbt-core 2.0+ (Fusion) does not support the postgres adapter

### dbt profiles.yml (Docker)
```yaml
smart_city:
  target: dev
  outputs:
    dev:
      type: postgres
      host: host.docker.internal
      port: 5434
      user: postgres
      password: <POSTGRES_PASSWORD>   # see C:\Users\Iwi\.dbt\profiles.yml — NOT "postgres"!
      dbname: smart_city
      schema: public
      threads: 4
```

### DAG
### Main pipeline DAG — dbt_smart_city
- File: `D:\IWConnect\airflow\dags\dbt_smart_city.py`
- Schedule: `@hourly`
- Tasks: `ingest_run → dbt_staging → dbt_test → dbt_intermediate`
- retries=2, execution_timeout=45min
- **max_active_runs=1** — prevents concurrent runs from corrupting data
- ingest_run runs: `python /opt/smart-city/ingestion/ingest.py` (no pip install — packages in docker-compose)

### Maintenance DAG — cleanup_smart_city
- File: `D:\IWConnect\airflow\dags\cleanup_smart_city.py`
- Schedule: `@daily` (runs at midnight)
- Tasks: `cleanup_old_data` — deletes airbyte_raw rows older than 14 days
- retries=1, execution_timeout=15min
- Deletes by `_airbyte_extracted_at` column (confirmed exists + has index)
- **Tested manually 2026-06-22** — confirmed working

### IMPORTANT: LOAD_EXAMPLES = false
docker-compose has `AIRFLOW__CORE__LOAD_EXAMPLES: 'false'` — only your DAG is visible.
Your DAG `dbt_smart_city` will be visible ~30 seconds after startup.

### Fixed issues (for reference)
- `schedule_interval` → `schedule` (Airflow 3.x breaking change)
- `dbt-core==1.8.2` pinned — dbt 2.0 (Fusion) does not support postgres
- Docker profiles.yml password must match `C:\Users\Iwi\.dbt\profiles.yml`

---

## 5b. AIRFLOW (OLD WSL VERSION — NOT IN USE)

### Startup
```bash
~/start-airflow.sh
```
UI: http://localhost:8080 (admin/admin)

---

## 6. GITHUB COLLABORATION

### Repo
- URL: https://github.com/stevanoskia/smart-city-iw
- Local: D:\IWConnect\smart-city-iw
- Branch: main

### Workflow
```bash
git pull                              # always pull before starting
git checkout -b feat/irina-xxx        # new branch
# ... work ...
git add .
git commit -m "description"
git push origin feat/irina-xxx
# Then create a Pull Request on GitHub
```

---

## 7. SCRIPTS

### fix_airbyte.py
- Location: `airflow/scripts/fix_airbyte.py`
- Purpose: runs chmod commands for Airbyte after restart + waits 3 min
- How to run (in WSL):
```bash
python3 /mnt/d/IWConnect/smart-city-iw/airflow/scripts/fix_airbyte.py
```

---

## 8. INGESTION SCRIPT — Python (replaces Airbyte)

### Location
- `D:\IWConnect\smart-city-iw\ingestion\config.py` — cities, API keys, DB config
- `D:\IWConnect\smart-city-iw\ingestion\ingest.py`  — main ingestion script

### Why script instead of Airbyte
- Airbyte requires manually adding each city in the UI
- The script reads a city list from `config.py` — easy to add new cities

### How to add a new city
In `config.py` add a line to `CITIES`:
```python
{"name": "Paris", "country": "FR", "lat": 48.8566, "lon": 2.3522, "bbox": "2.22,48.81,2.47,48.90", "timezone": "Europe/Paris", "has_traffic_data": True},
```
Get bbox coordinates at: **bboxfinder.com** → draw rectangle → copy

### Current cities (config.py)
| City | Country | Lat | Lon | Timezone | Has Traffic |
|------|---------|-----|-----|----------|-------------|
| Berlin | DE | 52.52 | 13.405 | Europe/Berlin | true |
| Madrid | ES | 40.4168 | -3.7038 | Europe/Madrid | true |
| London | GB | 51.5074 | -0.1278 | Europe/London | true |
| Amsterdam | NL | 52.3676 | 4.9041 | Europe/Amsterdam | true |

**Note:** `city_timezone` and `has_traffic_data` are now written to all raw tables on every ingest run.
This allows `dim_city` to be a SQL model (reads from staging) instead of a static seed.

### What the script collects
| Source | Data | Table |
|--------|------|-------|
| OpenWeather | Current weather | `airbyte_raw.current_weather` |
| OpenWeather | Air pollution / AQI | `airbyte_raw.air_pollution` |
| OpenWeather | 5-day forecast | `airbyte_raw.weather_forecast` |
| TomTom | Traffic flow | `airbyte_raw.traffic_flow` |
| TomTom | Traffic incidents | `airbyte_raw.traffic_incidents` |

### Required packages (once)
```powershell
& "D:\IWConnect\smart-city-iw\venv\Scripts\Activate.ps1"
pip install requests psycopg2-binary python-dotenv
```

### Manual run (for testing)
```powershell
& "D:\IWConnect\smart-city-iw\venv\Scripts\Activate.ps1"
cd D:\IWConnect\smart-city-iw\ingestion
python ingest.py
```

### Important details
- `_airbyte_meta` and `_airbyte_generation_id` are added automatically (required by Airbyte tables)
- `@version` from TomTom is skipped (invalid SQL column name)
- `traffic_flow` — only these fields are stored: frc, currentSpeed, freeFlowSpeed, currentTravelTime, freeFlowTravelTime, confidence, roadClosure
- `air_pollution` — API returns `{"list": [...]}`, script unwraps `list[0]`
- `traffic_incidents` — uses `fields` parameter to request full detail from TomTom v5 API
- City name is forced from `config.py` (prevents API returning district names like "Mitte" or "Sol")

---

## DAILY STARTUP SEQUENCE

```powershell
# 1. Open Docker Desktop (from Start menu), wait ~30 seconds

# 2. Start Airflow
cd D:\IWConnect\airflow
docker compose up -d
# UI: http://localhost:8080 (airflow/airflow)
# Wait ~3 min for pip install on startup
```

```powershell
# 3. Manual ingestion test (optional)
& "D:\IWConnect\smart-city-iw\venv\Scripts\Activate.ps1"
cd D:\IWConnect\smart-city-iw\ingestion
python ingest.py
```

```powershell
# 4. Manual dbt run (optional)
& "D:\IWConnect\smart-city-iw\venv\Scripts\Activate.ps1"
cd D:\IWConnect\smart-city-iw\dbt\smart_city
dbt run
```

---

## NOTE — Duplicate Records Fix (2026-06-09)
- Airbyte connections **disabled** (toggle OFF) — `ingest.py` is the only active ingestion source
- Root cause: both Airbyte and `ingest.py` were writing to the same `airbyte_raw` tables simultaneously
- Full details: `docs/duplicate_records_fix.md`

---

## WHAT IS DONE (summary)

| Component | Status |
|-----------|--------|
| Airbyte — ingestion from OpenWeather + TomTom | ✅ WORKS (backup, not actively used) |
| PostgreSQL — smart_city + airflow databases | ✅ WORKS |
| dbt — 5 staging models | ✅ WORKS |
| Python ingestion script (replaces Airbyte) | ✅ WORKS |
| Airflow — dbt_smart_city: ingest_run → dbt_staging → dbt_test → dbt_intermediate (@hourly) | ✅ WORKS |
| Airflow — cleanup_smart_city: cleanup_old_data (@daily, deletes >14 days) | ✅ WORKS (tested 2026-06-22) |
| GitHub — feat/irina-airflow-setup PR | ✅ PUSHED |
| Duplicate records fix — Airbyte disabled | ✅ FIXED (2026-06-09) |
| city/country added to ingest.py | ✅ DONE (2026-06-09) |
| ensure_columns() in ingest.py | ✅ DONE (2026-06-09) |
| country added to config.py for all 4 cities | ✅ DONE (2026-06-09) |
| city/country added to all 5 staging models | ✅ DONE (2026-06-09) |
| dbt intermediate models — 5 incremental, all PASS | ✅ DONE (2026-06-09) |
| Airbyte YAML connectors updated | ✅ DONE (2026-06-09) |
| Docker WSL memory increased to 12GB | ✅ DONE (2026-06-09) |
| dbt_project.yml — intermediate: materialized=incremental (fix) | ✅ DONE (2026-06-22) |
| All 5 intermediate models — 24h lookback + COALESCE empty-table fix | ✅ DONE (2026-06-22) |
| seeds/dim_city.csv — 4 cities with timezone + has_traffic_data | ✅ DONE (2026-06-22) — REMOVED 2026-06-25 |
| dim_city seed removed — replaced by SQL model (reads from staging) | ✅ DONE (2026-06-25) |
| config.py — added timezone + has_traffic_data to all cities | ✅ DONE (2026-06-25) |
| ingest.py — writes city_timezone + has_traffic_data to all raw tables | ✅ DONE (2026-06-25) |
| dim_date + dim_hour — created in marts.dimensions | ✅ DONE (2026-06-25) |
| docker-compose volume fixed: /mnt/d/ → D:/ (Windows path) | ✅ DONE (2026-06-25) |
| dim_city.csv seed deleted — dim_city.sql to be created | ✅ DONE (2026-06-25) |
| DAG — max_active_runs=1 (prevents concurrent run corruption) | ✅ DONE (2026-06-22) |
| DAG — removed pip install from ingest_run task | ✅ DONE (2026-06-22) |
| docker-compose — requests/psycopg2-binary/python-dotenv in _PIP_ADDITIONAL_REQUIREMENTS | ✅ DONE (2026-06-22) |
| dbt venv on Windows with Python 3.12 (C:\Users\Iwi\dbt-env) | ✅ DONE (2026-06-22) |

## INTERMEDIATE MODELS

| Model | Groups by | Key metrics |
|-------|-----------|-------------|
| `int_current_weather_hourly` | city + country + hour | avg/min/max temp, humidity, wind, weather label |
| `int_air_quality_hourly` | city + country + hour | avg AQI, pollutants (CO, NO2, PM2.5...) |
| `int_traffic_flow_hourly` | city + country + hour | avg speed, congestion score, congestion level |
| `int_traffic_incidents_hourly` | city + country + hour | total incidents, by category |
| `int_weather_forecast_daily` | city + country + day | min/max/avg temp, dominant weather, rain/snow |

## WHAT REMAINS

### Priority 1 — Bug fixes ✅ COMPLETED (2026-06-22)
All done. See WHAT IS DONE above.

### Priority 2 — Mart models (IN PROGRESS)
Star schema to build in `smart_city_marts`:

**Dimensions:**
- [ ] `dim_city` — SQL model (reads DISTINCT from stg_current_weather, includes city_timezone + has_traffic_data)
- [x] `dim_date` — generate_series CURRENT_DATE ±365, includes is_weekend, day_of_week_iso
- [x] `dim_hour` — 24 static rows with hour_label + part_of_day

**Facts:**
- [ ] `fct_weather_daily` — city × date from int_current_weather_hourly
- [ ] `fct_pollution_daily` — city × date from int_air_quality_hourly
- [ ] `fct_traffic_daily` — city × date (Berlin/London/Amsterdam only)
- [ ] `fct_traffic_hourly` — city × date × hour (peak-hour analysis)

**Marts (wide tables for dashboard):**
- [ ] `mart_city_daily` — OBT, LEFT JOIN so Madrid gets NULLs for traffic
  - comfort_index: bell curve (peak at 20°C), Madrid renormalized weights
  - local_date column (timezone conversion here only)
- [ ] `mart_forecast_latest` — ROW_NUMBER dedup, only future forecasts
- [ ] `mart_temperature_trends` — rolling 7d/30d avg, anomaly label
- [ ] `mart_weather_alerts` — severe weather from forecast thresholds

**After marts:**
- [ ] Add `dbt_build_marts` task to Airflow DAG
- [ ] dbt tests (schema.yml) for all layers
- [ ] dbt docs generate + exposures.yml
- [ ] dbt source freshness in sources.yml

### Priority 3 — Database cleanup
- [ ] Cleanup of `__dbt_backup` tables in PostgreSQL

### Priority 4 — Dashboard
- [ ] Dashboard (Metabase recommended — free, runs in Docker)

### Future (nice to have)
- [ ] **HERE Maps API** — add as second traffic source for cross-API comparison
  - Free tier: 250,000 req/month (much better than TomTom's 2,500/day)
  - Covers all 4 cities including Madrid → Madrid finally gets real traffic data
  - Would allow: TomTom vs HERE speed/congestion comparison per city
  - Implementation: add HERE fetch in ingest.py, new raw tables, new staging/intermediate models
- [ ] fct_forecast_accuracy — compare forecast vs actual (needs more history first)
- [ ] Push current changes to GitHub
