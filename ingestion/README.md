# Ingestion Layer — Airbyte (config-driven)

Ingestion is handled by **Airbyte** using two **custom declarative connectors** (built in the
Airbyte Connector Builder), loaded raw into PostgreSQL (`airbyte_raw` schema). Setup is
**config-driven**: edit YAML, run one script — no manual UI clicking to add cities.

Airbyte UI: http://localhost:8000 (deployed via `abctl`, runs in Kind/Kubernetes)

---

## How it fits together

```
sources.yml + connections.yml  ──►  setup_airbyte.py  ──►  Airbyte (sources, destination, connections)
   (cities, streams, dest)          (creates via API)              │
                                                                   ▼
                                                          connection_ids.yml ──► Airflow DAG
```

- **`config/sources.yml`** — per provider: connector name, streams, and the **list of cities**
  (coordinates; TomTom cities also have a bounding box).
- **`config/connections.yml`** — the PostgreSQL destination + sync settings (`full_refresh_append`).
- **`scripts/setup_airbyte.py`** — reads both, creates any missing sources / destination /
  connections via the Airbyte API (idempotent — safe to re-run), and writes
  **`config/connection_ids.yml`** (the connection UUIDs Airflow triggers).
- **`connections/*.yaml`** — the custom connector definitions:
  `open_weather_free_2_5.yaml`, `tomtom_traffic.yaml`.

One Airbyte **source + connection per city, per provider** (e.g. `openweather_berlin`,
`tomtom_london`). All write to the same `airbyte_raw` tables, distinguished by an injected
`city` column.

---

## Setup

### 1. Prerequisites
- Airbyte running (`abctl local install`; UI at `localhost:8000`).
- The two custom connectors published in the Connector Builder ("OpenWeather Free 2.5",
  "TomTom Traffic").
- `.env` populated: `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `AIRBYTE_WORKSPACE_ID`
  (from Airbyte UI → User → Applications), `AIRBYTE_PG_HOST` (**LAN IP, not localhost** — sync
  pods run in Kind and can't reach the host via localhost), `OPENWEATHER_API_KEY`,
  `TOMTOM_API_KEY`, and the `POSTGRES_*` creds.

### 2. Run the setup script
```bash
# from project root, in venv313
python ingestion/scripts/setup_airbyte.py
```
Creates the destination, one source + connection per city, and writes
`ingestion/config/connection_ids.yml`.

### 3. Trigger syncs
Airflow's `smart_city_pipeline` DAG triggers all connections hourly, or use **Sync now** in the
Airbyte UI. Verify in PostgreSQL:
```sql
-- psql -U postgres -d smart_city
SELECT city, COUNT(*) FROM airbyte_raw.current_weather GROUP BY city;
SELECT city, COUNT(*) FROM airbyte_raw.traffic_incidents GROUP BY city;
```

---

## Adding a new city

1. Add a city block under `openweather` and/or `tomtom` in `config/sources.yml`
   (include the bounding box for TomTom).
2. Re-run `python ingestion/scripts/setup_airbyte.py` (creates the new source + connection,
   updates `connection_ids.yml`).
3. Re-parse the Airflow DAG so it picks up the new connection
   (`docker compose restart airflow-scheduler`, or wait a parse cycle).

No connector code or dbt changes needed — `city` is source config, injected via `AddFields`, and
dbt models aggregate by `city` automatically.

---

## Notes / known quirks

- **Sync mode is `full_refresh_append`** by design — each sync appends a fresh full snapshot;
  deduplication happens downstream in dbt (the `int_city_hourly_*` models).
- **Connector edits take effect only after republishing** in the Airbyte Builder UI — editing the
  repo `connections/*.yaml` alone does nothing to the running connector.
- **TomTom incidents `fields` param** — incidentDetails v5 returns only `iconCategory` + geometry
  unless the `fields` query param lists the attributes; `tomtom_traffic.yaml` now sends it so full
  incident detail (id, delay, magnitudeOfDelay, from/to, …) ingests.
- **`city` may be NULL** on rows synced before the `AddFields` injection was added — downstream
  models filter `WHERE city IS NOT NULL`.
- **Schema refresh may 403** on a connector version change — delete and recreate the connection
  (re-run the setup script) instead.
