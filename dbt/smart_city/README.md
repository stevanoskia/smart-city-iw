# dbt — Smart City Analytics

dbt project for the Smart City Analytics Pipeline. Parses raw Airbyte JSON into typed columns
(ephemeral `stg_*` models), dedupes it into incremental **intermediate** hourly facts + forecast
issue history, and models it into a **marts** star schema (dims + facts + OBT + analytics). All in
one PostgreSQL database, one target (`staging`).

Pipeline: **Airbyte → `staging` (raw JSON) → intermediate → marts.** Airbyte lands raw API
snapshots as JSON directly in the `staging` schema and owns those tables; the `stg_*` models parse
that JSON but are **ephemeral**, so they compile inline as CTEs into their consumers and create no
DB object. That's why `staging` contains only Airbyte's raw tables and no `stg_*` views.

## Target

| Target | Database | Schemas | Models |
|---|---|---|---|
| `staging` | PostgreSQL (localhost:5432, db `smart_city`) | `staging` (raw JSON, Airbyte-owned), `intermediate`, `marts` | 5 ephemeral parsers + 5 intermediate tables + 15 marts tables |

> The `staging` **schema** holds Airbyte's raw JSON tables. The dbt `staging` **models** (`stg_*`)
> are ephemeral — they parse that JSON inline and create no DB object.

## Running dbt

Always activate `venv313` first and run from this directory. On the host, pass
`--profiles-dir C:/Users/Andrej/.dbt` so dbt uses the localhost profile (not the container one):

```bash
# From project root
source venv313/Scripts/activate
cd dbt/smart_city

# Install pinned packages (dbt_utils 1.4.1, from package-lock.yml). Required once, and
# after any packages.yml change — every model's surrogate keys depend on it.
dbt deps

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

Keys are built with **`dbt_utils.generate_surrogate_key`** (NULL-safe, `-` separator):
`city_hour_key = generate_surrogate_key(['city', hour-truncated observed_at])`
(weather/pollution/flow); `city_incident_key = generate_surrogate_key(['city', 'incident_id',
'observed_at'])` (incidents). `unique`/`not_null` tested.

Plus the forecast building block:
- `int_city_weather_forecast` — **incremental, append-only issue history**: one row per prediction
  issuance `(city, forecast_at, issued_at)`, keyed
  `generate_surrogate_key(['city', 'forecast_at', 'issued_at'])`. A forecast row
  has two timestamps — `forecast_at` (the future time predicted) and `issued_at` (when it was
  predicted); `lead_time = forecast_at − issued_at`. Persists predictions as issued so they survive
  raw pruning and can be scored for accuracy later.

### Marts → `marts` schema (incremental facts + tables — star schema + OBT + analytics)
Built from the intermediate facts. 15 models (materialization is mixed — see below):
- **Dimensions (3):** `dim_city` (**derived from data — no seed**), `dim_date` (independent calendar
  spine), `dim_hour` (`hour_label` + `day_part`).
- **Daily facts (3):** `fct_weather_daily`, `fct_pollution_daily`, `fct_traffic_daily` — one row per
  `(city, date_utc)`, `city_date_key = generate_surrogate_key(['city', 'date_utc'])`.
- **Hourly facts (3):** `fct_weather_hourly`, `fct_pollution_hourly`, `fct_traffic_hourly` — one row
  per `(city, hour)`. ⚠️ **Not diurnal curves**: Airflow only runs while the dev machine is on, so
  coverage is ~07:00–15:00 UTC with no evening/overnight data — peak-hour / time-of-day analysis is
  not viable on them. Their honest use is point-in-time "latest reading" semantics.
- **Forecast fact (1):** `fct_forecast_accuracy` — past predictions scored against observed
  `int_city_hourly_weather`.
- **OBT + analytics (5):** `mart_city_daily` (LEFT-joins weather+pollution+traffic; weather-only
  cities appear with NULL traffic), `mart_forecast_latest` (current forward-looking forecast),
  `mart_temperature_trends`, `mart_weather_alerts` (forward-looking, from the forecast), and
  `mart_pollution_alerts` (AQI/PM2.5/PM10/NO2 threshold breaches — **measured**, not forecast).

Star keys `city_key = generate_surrogate_key(['city'])`, `date_key = YYYYMMDD::int`;
`relationships` tests enforce FK→dimension integrity, plus `unique` / `not_null` /
`accepted_values`.

**Materialization (mixed).** The **8 append-only facts** are `materialized='incremental'`,
`delete+insert` (like the intermediate layer): the 3 hourly facts (`city_hour_key`, 12h lookback),
the 3 daily facts (`city_date_key`, 2-day lookback — only today's row is still mutable),
`fct_forecast_accuracy` (`forecast_key`), and `mart_pollution_alerts` (`alert_key`, measured
history). The other **7 stay full-rebuild `table`s** on purpose: the 3 dims (tiny/static), the two
rolling-window marts (`mart_city_daily`, `mart_temperature_trends` — a window needs the prior days
as *input* rows, so an incremental batch would truncate it), and the two forward-looking snapshots
(`mart_forecast_latest`, `mart_weather_alerts` — passed slots must drop out, which `delete+insert`
can't express). Output is byte-identical to a full rebuild, so
`dbt build --select marts --full-refresh` reproduces it exactly.

## Profiles (`~/.dbt/profiles.yml`)

See README.md at the project root for the full profiles.yml configuration.
