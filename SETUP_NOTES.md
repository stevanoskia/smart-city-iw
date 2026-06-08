# Smart City Analytics Pipeline - Setup Notes
## Last Updated: 2026-06-08

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
& "D:\IWConnect\smart-city-iw\venv\Scripts\Activate.ps1"
cd "D:\IWConnect\smart-city-iw\dbt\smart_city"
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
- dbt is installed automatically at startup (`_PIP_ADDITIONAL_REQUIREMENTS: dbt-core==1.8.2 dbt-postgres==1.8.2`)
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
- File: `D:\IWConnect\airflow\dags\dbt_smart_city.py`
- Schedule: `@hourly` (parameter `schedule=`, NOT `schedule_interval=` — Airflow 3.x)
- Tasks: `ingest_run → dbt_run`
- dbt project path in container: `/opt/smart-city/dbt/smart_city`

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
{"name": "Paris", "lat": 48.8566, "lon": 2.3522, "bbox": "2.22,48.81,2.47,48.90"},
```
Get bbox coordinates at: **bboxfinder.com** → draw rectangle → copy

### Current cities (config.py)
| City | Lat | Lon | Bbox |
|------|-----|-----|------|
| London | 51.5074 | -0.1278 | -0.25,51.43,-0.01,51.58 |
| Amsterdam | 52.3676 | 4.9041 | 4.78,52.30,5.03,52.43 |
| Berlin | 52.52 | 13.405 | 13.28,52.46,13.54,52.58 |
| Madrid | 40.4168 | -3.7038 | -3.83,40.33,-3.57,40.50 |

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

## WHAT IS DONE (summary)

| Component | Status |
|-----------|--------|
| Airbyte — ingestion from OpenWeather + TomTom | ✅ WORKS (backup, not actively used) |
| PostgreSQL — smart_city + airflow databases | ✅ WORKS |
| dbt — 5 staging models | ✅ WORKS |
| Python ingestion script (replaces Airbyte) | ✅ WORKS |
| Airflow — DAG: ingest_run → dbt_run (@hourly) | ✅ WORKS (confirmed Success) |
| GitHub — feat/irina-airflow-setup PR | ✅ PUSHED |

## WHAT REMAINS

- [ ] Intermediate dbt models
- [ ] Marts dbt models
- [ ] Cleanup of `__dbt_backup` tables in PostgreSQL
- [ ] Dashboard (Power BI / Metabase)
