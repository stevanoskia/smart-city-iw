# dbt — Smart City Analytics

dbt project for the Smart City Analytics Pipeline. Parses raw Airbyte JSON into typed columns
(ephemeral `stg_*` models), dedupes it into incremental **intermediate** hourly facts + forecast
issue history, and models it into a **marts** star schema (dims + facts + OBT + analytics). All in
one PostgreSQL database, one target (`staging`).

Pipeline: **Airbyte → `staging` (raw JSON) → intermediate → marts.** See
[docs/staging_as_raw_landing.md](../../docs/staging_as_raw_landing.md) for how the raw-JSON
landing + ephemeral parsing works.

## Target

| Target | Database | Schemas | Models |
|---|---|---|---|
| `staging` | PostgreSQL (localhost:5432, db `smart_city`) | `staging` (raw JSON, Airbyte-owned), `intermediate`, `marts` | 5 ephemeral parsers + 5 intermediate tables + 12 marts tables |

> The `staging` **schema** holds Airbyte's raw JSON tables. The dbt `staging` **models** (`stg_*`)
> are ephemeral — they parse that JSON inline and create no DB object.

## Running dbt

Always activate `venv313` first and run from this directory. On the host, pass
`--profiles-dir C:/Users/Andrej/.dbt` so dbt uses the localhost profile (not the container one):

```bash
# From project root
source venv313/Scripts/activate
cd dbt/smart_city

# Staging is ephemeral — this builds nothing physical, just validates the parse compiles
dbt run   --select staging --target staging

# Build + test intermediate tables (hourly facts + forecast issue history)
dbt build --select intermediate --target staging

# Build + test marts (star schema + OBT + analytics)
dbt build --select marts --target staging

# Everything in dependency order
dbt build --select staging intermediate marts --target staging

# Docs
dbt docs generate --target staging
dbt docs serve
```

`dbt build` = run models **and** their tests; `dbt run` builds without testing.

## Model layers

### Staging → ephemeral parsers (no DB object)
Parse raw Airbyte JSON into typed columns; one model per source table, 1:1 with raw (no
dedup/aggregation). `materialized: ephemeral`, so each compiles inline as a CTE into its consumers.
They read from `{{ source('staging', '<table>') }}` — the raw JSON tables Airbyte writes.
- `stg_current_weather` — typed weather fields from OpenWeather
- `stg_air_pollution` — typed AQI + pollutants from OpenWeather
- `stg_weather_forecast` — 5-day / 3-hour forecast records from OpenWeather
- `stg_traffic_flow` — road segment speeds from TomTom
- `stg_traffic_incidents` — active incidents from TomTom

### Intermediate → `intermediate` schema (incremental tables — the durable archive)
Deduped on each stream's business key (keeping the freshest reading) to **one row per clock hour**.
`materialized='incremental'`, `delete+insert`, 6h lookback — so they accumulate clean hourly history
forever, independent of `staging` raw pruning (dedup is required because Airbyte runs
`full_refresh_append`). Carry `date_utc` + `hour_utc` for time-of-day analysis.
- `int_city_hourly_weather` — hourly temp/wind/humidity/precip/condition
- `int_city_hourly_pollution` — hourly AQI + pollutant concentrations
- `int_city_hourly_traffic_flow` — per-hour congestion/speed snapshots
- `int_city_hourly_traffic_incidents` — per-hour incident detail (keyed on `(city, incident_id, observed_at)`, `where incident_id is not null`)

Keys: `city_hour_key = md5(city|date_trunc('hour', observed_at))` (weather/pollution/flow);
`city_incident_key = md5(city|incident_id|observed_at)` (incidents). `unique`/`not_null` tested.

Plus the forecast building block:
- `int_city_weather_forecast` — **incremental, append-only issue history**: one row per prediction
  issuance `(city, forecast_at, issued_at)`, keyed `md5(city|forecast_at|issued_at)`. A forecast row
  has two timestamps — `forecast_at` (the future time predicted) and `issued_at` (when it was
  predicted); `lead_time = forecast_at − issued_at`. Persists predictions as issued so they survive
  raw pruning and can be scored for accuracy later.

### Marts → `marts` schema (tables — star schema + OBT + analytics)
Built from the intermediate facts. 12 models:
- **Dimensions:** `dim_city` (**derived from data — no seed**), `dim_date` (independent calendar
  spine), `dim_hour` (`hour_label` + `day_part`).
- **Daily facts:** `fct_weather_daily`, `fct_pollution_daily`, `fct_traffic_daily` — one row per
  `(city, date_utc)`, `city_date_key = md5(city|date_utc)`.
- **Extra facts:** `fct_traffic_hourly`, `fct_forecast_accuracy` (past predictions scored against
  observed `int_city_hourly_weather`).
- **OBT + analytics:** `mart_city_daily` (LEFT-joins weather+pollution+traffic; weather-only cities
  appear with NULL traffic), `mart_forecast_latest` (current forward-looking forecast),
  `mart_temperature_trends`, `mart_weather_alerts`.

Star keys `city_key = md5(city)`, `date_key = YYYYMMDD::int`; `relationships` tests enforce
FK→dimension integrity, plus `unique` / `not_null` / `accepted_values`.

See [docs/marts_build_guide.md](../../docs/marts_build_guide.md) for the marts build walkthrough and
reference SQL, and [docs/marts_implementation_plan.md](../../docs/marts_implementation_plan.md) for
the design rationale.

## Profiles (`~/.dbt/profiles.yml`)

See README.md at the project root for the full profiles.yml configuration.
