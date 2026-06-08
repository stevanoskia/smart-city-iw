# dbt — Smart City Analytics

dbt project for the Smart City Analytics Pipeline. Cleans raw Airbyte data into PostgreSQL
**staging** views, then dedupes + aggregates it into **intermediate** tables. All in one
PostgreSQL database, one target (`staging`).

## Target

| Target | Database | Schemas | Models |
|---|---|---|---|
| `staging` | PostgreSQL (localhost:5432, db `smart_city`) | `staging`, `intermediate` | 5 views + 3 tables |

## Running dbt

Always activate `venv313` first and run from this directory:

```bash
# From project root
source venv313/Scripts/activate
cd dbt/smart_city

# Build staging views
dbt run   --select staging --target staging

# Build + test intermediate tables (deduped daily aggregates)
dbt build --select intermediate --target staging

# Everything in dependency order
dbt build --select staging intermediate --target staging

# Docs
dbt docs generate --target staging
dbt docs serve
```

`dbt build` = run models **and** their tests; `dbt run` builds without testing.

## Model layers

### Staging → `staging` schema (views)
Light cleanup of raw Airbyte data. One view per source table, 1:1 with raw (no dedup/aggregation).
- `stg_current_weather` — typed weather fields from OpenWeather
- `stg_air_pollution` — typed AQI + pollutants from OpenWeather
- `stg_weather_forecast` — 5-day forecast records from OpenWeather
- `stg_traffic_flow` — road segment speeds from TomTom
- `stg_traffic_incidents` — active incidents from TomTom

### Intermediate → `intermediate` schema (tables)
Deduped on each stream's business key (keeping latest `extracted_at`), then aggregated to one
row per `(city, date_utc)`. Keyed on `city_date_key = md5(city|date_utc)` with `unique` +
`not_null` tests. Dedup is required because Airbyte runs `full_refresh_append` (appends a full
snapshot every hour).
- `int_city_daily_weather` — daily temp/wind/precip + dominant condition
- `int_city_daily_pollution` — daily AQI + pollutant averages, `hours_poor_air`
- `int_city_daily_traffic` — daily congestion/speed + incident counts

## Profiles (`~/.dbt/profiles.yml`)

See README.md at the project root for the full profiles.yml configuration.
