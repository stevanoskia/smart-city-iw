# Ingestion Layer — Airbyte Setup

Data ingestion is handled entirely by Airbyte. No custom scripts needed.
Airbyte connects to the APIs and loads raw data directly into PostgreSQL.

## Airbyte UI: http://localhost:8000

---

## Step 1 — Add PostgreSQL Destination

1. Go to **Destinations** → New destination → search **PostgreSQL**
2. Fill in:
   - Host: `host.docker.internal`  ← NOT localhost (Airbyte is inside Docker)
   - Port: `5432`
   - Database: `smart_city`
   - Username: `postgres`
   - Password: (your password from .env)
   - Default Schema: `airbyte_raw`
   - SSL: disabled (local)
3. Click **Test and save**

---

## Step 2 — Add OpenWeather Source (Weather + Air Quality)

1. Go to **Sources** → New source → search **OpenWeather**
2. Fill in:
   - API Key: (your OPENWEATHER_API_KEY from .env)
   - Cities: add each city you want to monitor
3. Click **Test and save**

---

## Step 3 — Create Connection

1. Go to **Connections** → New connection
2. Source: OpenWeather | Destination: PostgreSQL
3. Streams to sync:
   - `current_weather` — full refresh, every 1 hour
   - `air_pollution` — full refresh, every 1 hour
4. Enable **Basic Normalization** (Airbyte will create flat tables automatically)
5. Click **Set up connection**

---

## Step 4 — Run First Sync

Click **Sync now** on the connection page.
After it completes, verify in PostgreSQL:

```sql
-- Connect: psql -U postgres -d smart_city
SELECT * FROM airbyte_raw.weather LIMIT 5;
SELECT * FROM airbyte_raw.air_pollution LIMIT 5;
```

---

## Connection Config Exports

The `connections/` folder holds exported JSON snapshots of each Airbyte connection.
To export: Airbyte UI → Connection → Settings → Export.
These are for version control / documentation — Airbyte is the live source of truth.
