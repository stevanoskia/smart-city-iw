# Ingestion Layer — Airbyte (config-driven)

Ingestion is handled by **Airbyte** using two **custom declarative connectors** (built in the
Airbyte Connector Builder), loaded raw into PostgreSQL (`staging` schema — raw JSON). Setup is
**config-driven**: sources / streams / cities live in the **`config` schema in Postgres**
(`config.sources`, `config.streams`, `config.source_locations` — see `config/README.md`), and
one script applies them to Airbyte. Adding a city is an `INSERT`, not UI clicking.

Airbyte UI: http://localhost:8000 (deployed via `abctl`, runs in Kind/Kubernetes)

---

## How it fits together

```
config schema (Postgres)  ──►  setup_airbyte.py  ──►  Airbyte (sources, destination, connections)
   sources/streams/            (main: host, or               │
   source_locations            reconcile: container)          ▼
                                                     connection_ids.yml ──► Airflow DAG
```

- **`config` schema** — `config.sources` (connector name, `api_key_env`/`api_key_field`),
  `config.streams`, `config.locations` + `config.source_locations` (one row per city; TomTom rows
  carry the bounding box). DDL/seed in `config/`. This is the source of truth.
- **destination** — the single PostgreSQL destination is a constant in `setup_airbyte.py`
  (`smart_city_postgres` → `staging`); sync mode `full_refresh_append`.
- **`scripts/setup_airbyte.py`** — reads `config.*`, creates/updates sources / destination /
  connections via the Airbyte API, and writes **`config/connection_ids.yml`** (the connection
  UUIDs Airflow triggers). Idempotent. Two entrypoints: **`main()`** (host — manages the
  destination + this machine's LAN IP; run after a network switch) and **`reconcile()`**
  (container-safe — skips the destination; the DAG's `reconcile_airbyte` task calls it each run).
- **`connections/*.yaml`** — the custom connector definitions:
  `open_weather_free_2_5.yaml`, `tomtom_traffic.yaml`.
- **`config/sources.yml` + `config/connections.yml`** — *retired*; kept only as the one-time input
  for `config/seed_config.py`. After seeding, edit `config.*` with SQL, not these files.

**One Airbyte source + connection per provider** — `openweather_all`, `tomtom_all`. Each connector
is **partition-routed** (`ListPartitionRouter`) over its `locations` list, so a single connection
makes one API request per city per stream within one sync. The request params and the injected
`city` column read the current partition (`stream_partition` / `stream_slice`). All rows write to
the same `staging` (raw JSON) tables, tagged with the `city` column.

---

## Setup

### 1. Prerequisites
- Airbyte running (`abctl local install`; UI at `localhost:8000`).
- The two custom connectors published in the Connector Builder ("OpenWeather Free 2.5",
  "TomTom Traffic").
- `.env` populated: `AIRBYTE_CLIENT_ID`, `AIRBYTE_CLIENT_SECRET`, `AIRBYTE_WORKSPACE_ID`
  (from Airbyte UI → User → Applications), `AIRBYTE_PG_HOST` (leave at **`auto`** — it detects this
  machine's LAN IP; **never localhost**, since sync pods run in Kind and can't reach the host that
  way. Pin an explicit IP only if detection picks the wrong interface), `OPENWEATHER_API_KEY`,
  `TOMTOM_API_KEY`, and the `POSTGRES_*` creds.

### 2. Run the setup script
```bash
# from project root, in venv313
python ingestion/scripts/setup_airbyte.py
```
Creates the destination, one source + connection per provider (`openweather_all`, `tomtom_all`),
each partition-routed over all cities, and writes `ingestion/config/connection_ids.yml`.

### 3. Trigger syncs
Airflow's `smart_city_pipeline` DAG triggers all connections hourly, or use **Sync now** in the
Airbyte UI. Verify in PostgreSQL:
```sql
-- psql -U postgres -d smart_city
SELECT city, COUNT(*) FROM staging.current_weather GROUP BY city;
SELECT city, COUNT(*) FROM staging.traffic_incidents GROUP BY city;
```

---

## Adding a new city

1. Call the helper — `select config.add_city('Zagreb', 45.8150, 15.9819);` for a weather-only city,
   or pass a bounding box `select config.add_city('Zagreb', 45.8150, 15.9819, 45.75,15.85,45.88,16.05);`
   to also enable TomTom traffic. (It does the `config.locations` + `config.source_locations` inserts
   for you; raw SQL alternative in `config/README.md`.)
2. It applies automatically on the next hourly run (the DAG's `reconcile_airbyte` task pushes the
   updated source config to Airbyte), or run `python ingestion/scripts/setup_airbyte.py` on the
   host to apply it immediately.

No new connection, **no Airflow DAG re-parse** (the connection count is unchanged), and no connector
or dbt changes — the connector partition-routes over the `locations` list, injects `city` via
`AddFields`, and dbt models aggregate by `city` automatically.

> Only republish the connector in the Builder UI if you change the connector *definition* itself
> (streams, request shape, spec) — not for adding cities to the list.

---

## Notes / known quirks

- **Switching networks breaks syncs until you re-run the setup script.** The destination holds this
  machine's LAN IP (pods run in Kind — `localhost` is the pod itself), and the router reassigns that
  IP per network. Airbyte stores it *literally*, so every sync fails from a new network until the
  destination is re-pointed. Fix: `python ingestion/scripts/setup_airbyte.py` once connected to the
  new network. Postgres itself is already network-agnostic (`pg_hba.conf` uses `samenet`).
  A failed sync now names this cause in the alert email (`[destination/config_error]` + a
  connection timeout).
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
