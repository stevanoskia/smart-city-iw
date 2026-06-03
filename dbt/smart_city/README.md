# dbt — Smart City Analytics

dbt project for the Smart City Analytics Pipeline. Transforms raw API data from
PostgreSQL into an analytical DuckDB warehouse.

## Two-target setup

| Target | Database | Models | Command |
|---|---|---|---|
| `staging` | PostgreSQL (localhost:5432) | 5 staging views | `dbt run --select staging --target staging` |
| `warehouse` | DuckDB (warehouse/smart_city.duckdb) | 4 intermediate views + 5 mart tables | `dbt run --select intermediate marts --target warehouse` |

## Running dbt

Always activate `venv313` first and run from this directory:

```bash
# From project root
source venv313/Scripts/activate
cd dbt/smart_city

# Full pipeline
dbt run --select staging --target staging && dbt run --select intermediate marts --target warehouse

# Tests
dbt test --select staging --target staging
dbt test --select intermediate marts --target warehouse

# Docs
dbt docs generate --target warehouse
dbt docs serve
```

## Model layers

### Staging → PostgreSQL (`--target staging`)
Light cleanup of raw Airbyte data. One view per source table.
- `stg_current_weather` — typed weather fields from OpenWeather
- `stg_air_pollution` — typed AQI + pollutants from OpenWeather
- `stg_weather_forecast` — 5-day forecast records from OpenWeather
- `stg_traffic_flow` — road segment speeds from TomTom
- `stg_traffic_incidents` — active incidents from TomTom

### Intermediate → DuckDB (`--target warehouse`)
Daily aggregations per city. Read from PostgreSQL staging via postgres ATTACH.
- `int_city_daily_weather` — avg/min/max temp, rain, dominant condition
- `int_city_daily_pollution` — avg/max AQI, pollutant averages, hours of poor air
- `int_city_daily_traffic` — congestion score, speed, incidents
- `int_composite_city_score` — joins all three + computes comfort index (0–1)

### Marts → DuckDB (`--target warehouse`)
Final analytics tables. Power BI reads from here.
- `mart_temperature_trends` — rolling 7d/30d averages, anomaly flags
- `mart_aqi_monitoring` — AQI labels, alert flags, 7d trend
- `mart_traffic_density` — congestion labels, speed ratio, 7d rolling avg
- `mart_city_comparison` — all cities ranked per day by comfort/AQI/temp/congestion
- `mart_smart_city_kpis` — headline KPI cards: comfort index, livability score, alerts

## Profiles (`~/.dbt/profiles.yml`)

See README.md at the project root for the full profiles.yml configuration.
