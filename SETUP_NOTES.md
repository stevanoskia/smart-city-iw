# Smart City Analytics Pipeline - Setup Notes
## Date: 2026-06-03

---

## MACHINE INFO
- OS: Windows 11
- WSL: Ubuntu
- Python: 3.12.x (via venv) — 3.14 NE E KOMPATIBILNO so dbt
- Project path: D:\IWConnect\smart-city-iw

---

## 1. AIRBYTE — INSTALACIJA I PROBLEMI

### Problem
- Airbyte se instaliras so `abctl` (Kubernetes/kind vo Docker)
- Sekojpat po restart na kompjuter, PostgreSQL pod (`airbyte-db-0`) pada so:
  `mkdir: can't create directory '/var/lib/postgresql/data/pgdata': Permission denied`

### Resenie (sekojpat po restart)
Vo WSL terminal:
```bash
export KUBECONFIG=/home/<user>/.airbyte/abctl/abctl.kubeconfig
docker exec airbyte-abctl-control-plane chmod 777 /var/local-path-provisioner/airbyte-volume-db
docker exec airbyte-abctl-control-plane chmod 777 /var/local-path-provisioner/airbyte-local-pv
```
Pa cekaj 2-3 minuti za site podovi da se startuvaat.

### Airbyte Login
- URL: http://localhost:8000
- Nginx basic auth: samo password (bez username)
- Password: `<AIRBYTE_PASSWORD>` — see local `.env` file

### Ako pak ne raboti
```bash
export KUBECONFIG=/home/<user>/.airbyte/abctl/abctl.kubeconfig
kubectl get pods -n airbyte-abctl  # provjeri status
kubectl delete pod airbyte-db-0 -n airbyte-abctl  # restart DB ako treba
abctl local install  # ako treba full reinstall
```

---

## 2. POSTGRESQL — LOKALNA BAZA

### Instalacija
- Instaliran direktno na Windows vo: `D:\postgre\`
- Data directory: `D:\postgre\data\`
- Port: **5434** (5432 bese zafaten)
- User: `postgres`
- Password: `<POSTGRES_PASSWORD>` — see local `.env` file
- Service name: `postgresql-x64-18`

### Bazi
- `smart_city` — za Airbyte destination + dbt
- `airflow` — za Airflow metadata

### pg_hba.conf
Dodadeni linii vo `D:\postgre\data\pg_hba.conf`:
```
host    all    all    10.2.0.0/16    scram-sha-256
host    all    all    172.26.0.0/16  scram-sha-256
```
Prvata dozvoluva konekcii od Kubernetes/Airbyte, vtorata od WSL.

### Restart na servis
```powershell
Restart-Service postgresql-x64-18
```

### DBeaver konekcija
- Host: localhost
- Port: 5434
- Database: smart_city
- Username: postgres
- Password: see `.env`

---

## 3. AIRBYTE CONNECTIONS — POSTAVENI

### Destination
- Name: `smart_city_postgres`
- Host: `<WIFI_IP>` (moze da se promeni po restart!)
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

### Connections (6 vkupno — site HEALTHY)
| Source | Grad | Destination | Sync |
|--------|------|-------------|------|
| OpenWeather Free 2.5 | Berlin (lat:52.52, lon:13.405) | smart_city_postgres | 1h |
| OpenWeather Free 2.5 | London (lat:51.5074, lon:-0.1278) | smart_city_postgres | 1h |
| OpenWeather Free 2.5 | Amsterdam (lat:52.3676, lon:4.9041) | smart_city_postgres | 1h |
| TomTom Traffic | Berlin | smart_city_postgres | 1h |
| TomTom Traffic | London | smart_city_postgres | 1h |
| TomTom Traffic | Amsterdam | smart_city_postgres | 1h |

### Tabeli vo airbyte_raw schema
- `air_pollution`
- `current_weather`
- `traffic_flow`
- `traffic_incidents` (~2445 zapisi)
- `weather_forecast` (~80 zapisi)

---

## 4. DBT — POSTAVENO I RABOTI

### Virtual Environment (Windows)
```powershell
& "D:\IWConnect\smart-city-iw\venv\Scripts\Activate.ps1"
cd "D:\IWConnect\smart-city-iw\dbt\smart_city"
```

### profiles.yml
Lokacija: `C:\Users\<user>\.dbt\profiles.yml`
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

### dbt komandi
```powershell
dbt debug   # test konekcija
dbt run     # pokreni modeli
dbt test    # testovi
```

### Staging modeli (KRERANI - site 5 PASS)
- `staging.stg_air_pollution`
- `staging.stg_current_weather`
- `staging.stg_traffic_flow`
- `staging.stg_traffic_incidents`
- `staging.stg_weather_forecast`

---

## 5. AIRFLOW — DOCKER (AKTIVNA VERZIJA)

### Lokacija
- docker-compose.yaml: `D:\IWConnect\airflow\docker-compose.yaml`
- DAG fajl: `D:\IWConnect\airflow\dags\dbt_smart_city.py`
- dbt profiles za Docker: `D:\IWConnect\airflow\config\profiles.yml`

### Startup (od D:\IWConnect\airflow)
```powershell
cd D:\IWConnect\airflow
docker compose up -d
```
UI: http://localhost:8080 (airflow/airflow)

### Restart (posle promena na docker-compose ili DAG)
```powershell
cd D:\IWConnect\airflow
docker compose down
docker compose up -d
```

### Kako funkcionira
- Docker montura `D:/IWConnect/smart-city-iw` na `/opt/smart-city` vo kontejnerot
- dbt profiles.yml e vo `D:\IWConnect\airflow\config\profiles.yml` (monturan na `/opt/airflow/config`)
- dbt se povrzuva na PostgreSQL preku `host.docker.internal:5434`
- dbt se instalira avtomatski pri startup (`_PIP_ADDITIONAL_REQUIREMENTS: dbt-core==1.8.2 dbt-postgres==1.8.2`)
- VAZNO: mora da se pinuva `dbt-core==1.8.2` — dbt-core 2.0+ (Fusion) ne go podrzuva postgres adapterot

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
      password: <POSTGRES_PASSWORD>   # see C:\Users\<user>\.dbt\profiles.yml — NE e "postgres"!
      dbname: smart_city
      schema: public
      threads: 4
```

### DAG
- Fajl: `D:\IWConnect\airflow\dags\dbt_smart_city.py`
- Schedule: `@hourly` (parameter `schedule=`, NE `schedule_interval=` — Airflow 3.x)
- Task: `dbt run` so site 5 staging modeli
- dbt project path vo kontejner: `/opt/smart-city/dbt/smart_city`

### VAZNO: LOAD_EXAMPLES = false
docker-compose ima `AIRFLOW__CORE__LOAD_EXAMPLES: 'false'` — samo tvojot DAG e vidliv.
Tvojot DAG `dbt_smart_city` ke bide vidliv po ~30 sekundi od startup.

---

## 5b. AIRFLOW (STARA WSL VERZIJA — NE SE KORISTI)

### Instalacija (WSL - Python 3.12 venv)
```bash
python3.12 -m venv ~/airflow-venv
source ~/airflow-venv/bin/activate
pip install apache-airflow==2.9.3 apache-airflow-providers-postgres \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.12.txt"
pip install "dbt-postgres==1.8.2" "dbt-core<2.0"
```

### Startup
```bash
~/start-airflow.sh
```
UI: http://localhost:8080 (admin/admin)

### DAG
- Fajl: `airflow/dags/dbt_smart_city.py` (vo ovoj repo)
- Schedule: `@hourly`
- Task: `dbt run` so site 5 staging modeli

### dbt profiles.yml za WSL
Lokacija: `~/.dbt/profiles.yml`
```yaml
smart_city:
  target: dev
  outputs:
    dev:
      type: postgres
      host: <WINDOWS_IP>   # ip route show default | awk '{print $3}'
      port: 5434
      user: postgres
      password: <POSTGRES_PASSWORD>
      dbname: smart_city
      schema: public
      threads: 4
```

---

## 6. GITHUB KOLABORACIJA

### Repo
- URL: https://github.com/stevanoskia/smart-city-iw
- Local: D:\IWConnect\smart-city-iw
- Branch: main

### Workflow
```bash
git pull                          # sekojpat pred pocnuvanje
git checkout -b feat/irina-xxx    # nova branch
# ... raboti ...
git add .
git commit -m "opis"
git push origin feat/irina-xxx
# Pa napravi Pull Request na GitHub
```

---

## 7. SKRIPTI

### fix_airbyte.py
- Lokacija: `airflow/scripts/fix_airbyte.py`
- Sto pravi: gi izvrsуva chmod komandite za Airbyte po restart + ceka 3 min
- Kako se pokreнуva (vo WSL):
```bash
python3 /mnt/d/IWConnect/smart-city-iw/airflow/scripts/fix_airbyte.py
```

### start-airflow.sh
- Lokacija: `~/start-airflow.sh` (vo WSL home, NE vo repo)
- Sto pravi: aktivira venv, setira env variables, startуva webserver + scheduler
- Kako se pokreнуva (vo WSL):
```bash
~/start-airflow.sh
```

---

## 8. INGESTION SKRIPTA — Python (zamena za Airbyte)

### Lokacija
- `D:\IWConnect\smart-city-iw\ingestion\config.py` — gradovi, API klucevi, DB config
- `D:\IWConnect\smart-city-iw\ingestion\ingest.py`  — glavna skripta

### Zosto skripta namesto Airbyte
- Airbyte bara racno dodavanje na sekoj grad vo UI
- Skriptata zema lista na gradovi od config.py — lесно se dodava nov grad

### Kako da dodades nov grad
Vo `config.py` dodaj red vo `CITIES`:
```python
{"name": "Paris", "lat": 48.8566, "lon": 2.3522, "bbox": "2.22,48.81,2.47,48.90"}
```
bbox vrednosti: **bboxfinder.com** → nacrtaj pravoagolnik → kopiraj

### Sto zema skriptata
| Izvor | Podatok | Tabela |
|-------|---------|--------|
| OpenWeather | Momentalna sostojba | `airbyte_raw.current_weather` |
| OpenWeather | Zagaduvanje na vozduh | `airbyte_raw.air_pollution` |
| OpenWeather | Prognoza 5 dena | `airbyte_raw.weather_forecast` |
| TomTom | Soobrakаen protok | `airbyte_raw.traffic_flow` |
| TomTom | Incidenti | `airbyte_raw.traffic_incidents` |

### Potrebni paketi (ednas)
```powershell
& "D:\IWConnect\smart-city-iw\venv\Scripts\Activate.ps1"
pip install requests psycopg2-binary python-dotenv
```

### Racno pustanje (za test)
```powershell
& "D:\IWConnect\smart-city-iw\venv\Scripts\Activate.ps1"
cd D:\IWConnect\smart-city-iw\ingestion
python ingest.py
```

### Vazni detalji
- `_airbyte_meta` i `_airbyte_generation_id` se dodavaat avtomatski (Airbyte gi bara)
- `@version` od TomTom se preskokuva (nevalidna SQL kolona)
- `traffic_flow` — se zemaat samo: frc, currentSpeed, freeFlowSpeed, currentTravelTime, freeFlowTravelTime, confidence, roadClosure
- `air_pollution` — API vraka `{"list": [...]}`, skriptata go unwrap-nuva `list[0]`

---

## CESTA KOMANDA SEKVENCA (start na den)

```powershell
# 1. Startuvaj Airflow (od D:\IWConnect\airflow)
cd D:\IWConnect\airflow
docker compose up -d
# UI: http://localhost:8080 (airflow/airflow)
# Cekaj ~3 min za pip install pri startup
```

```powershell
# 2. Racno test na ingestion (opciono)
& "D:\IWConnect\smart-city-iw\venv\Scripts\Activate.ps1"
cd D:\IWConnect\smart-city-iw\ingestion
python ingest.py
```

```powershell
# 3. Racno dbt run (opciono)
& "D:\IWConnect\smart-city-iw\venv\Scripts\Activate.ps1"
cd D:\IWConnect\smart-city-iw\dbt\smart_city
dbt run
```

---

## STO E NAPRAVENO (summary)

| Komponenta | Status |
|------------|--------|
| Airbyte — ingestion od OpenWeather + TomTom | ✅ RABOTI (backup, ne se koristi aktivno) |
| PostgreSQL — smart_city + airflow bazi | ✅ RABOTI |
| dbt — 5 staging modeli | ✅ RABOTI |
| Python ingestion skripta (zamena za Airbyte) | ✅ RABOTI |
| Airflow — DAG: ingest_run → dbt_run (@hourly) | ✅ RABOTI (potvrden Success) |
| GitHub — feat/irina-airflow-setup PR | ✅ PUSHANO |
| fix_airbyte.py skripta | ✅ NAPRAVENA |

## STO OSTANA

- [ ] Intermediate dbt modeli
- [ ] Marts dbt modeli
- [ ] Cleanup na `__dbt_backup` tabeli vo PostgreSQL
