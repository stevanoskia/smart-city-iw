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

## 5. AIRFLOW — POSTAVENO I RABOTI

### Instalacija (WSL - Python 3.12 venv)
```bash
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt install python3.12 python3.12-venv -y
python3.12 -m venv ~/airflow-venv
source ~/airflow-venv/bin/activate
pip install apache-airflow==2.9.3 apache-airflow-providers-postgres \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.12.txt"
pip install "dbt-postgres==1.8.2" "dbt-core<2.0"
```

### Konfiguracija
```bash
export AIRFLOW_HOME=~/airflow
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN='postgresql+psycopg2://postgres:<POSTGRES_PASSWORD>@<WINDOWS_IP>:5434/airflow'
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__LOAD_EXAMPLES=False
```
Windows IP od WSL: `ip route show default | awk '{print $3}'`

### Startup
```bash
~/start-airflow.sh   # startira webserver + scheduler
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

## CESTA KOMANDA SEKVENCA (start na den)

```bash
# 1. WSL - fix Airbyte permissions
export KUBECONFIG=/home/<user>/.airbyte/abctl/abctl.kubeconfig
docker exec airbyte-abctl-control-plane chmod 777 /var/local-path-provisioner/airbyte-volume-db
docker exec airbyte-abctl-control-plane chmod 777 /var/local-path-provisioner/airbyte-local-pv

# 2. Cekaj 2-3 min, pa otvori http://localhost:8000

# 3. WSL - startuvaj Airflow
~/start-airflow.sh
# UI: http://localhost:8080
```

```powershell
# 4. PowerShell - aktiviraj venv za dbt (ako treba racno)
& "D:\IWConnect\smart-city-iw\venv\Scripts\Activate.ps1"
cd "D:\IWConnect\smart-city-iw\dbt\smart_city"
dbt run
```
