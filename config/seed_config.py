"""
One-time loader for the metadata-driven config schema (config.*).

Reads the legacy YAML (ingestion/config/sources.yml + connections.yml) for the
sources / streams / locations, and carries the ~88 field mappings + starter
validation rules transcribed from the five dbt staging models
(dbt/smart_city/models/staging/stg_*.sql). Populates:

    config.sources · config.streams · config.locations · config.source_locations
    config.field_mappings · config.validation_rules

Idempotent (ON CONFLICT upserts). Re-running refreshes the *definition* columns
(source_expr, data_type, ordinal, descriptions, thresholds) but PRESERVES the
operational flags (is_active / is_required) so a live toggle survives a re-seed.

    Run AFTER config/schema.sql, with venv313 (has psycopg2 via dbt-postgres):
        python config/seed_config.py

Requires POSTGRES_HOST/PORT/DB/USER/PASSWORD in .env (same as setup_airbyte.py).

NOTE: after this initial load the DB is the source of truth — make further
changes with SQL against config.*, not by editing YAML. The YAML files are
retained only as the seed input and are otherwise retired.
"""

import os
import sys
from pathlib import Path

import yaml
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

# Windows consoles default to cp1252; force UTF-8 so status glyphs don't crash.
sys.stdout.reconfigure(encoding="utf-8")

ROOT = Path(__file__).resolve().parents[1]
CONFIG_DIR = ROOT / "ingestion" / "config"
SOURCES_FILE = CONFIG_DIR / "sources.yml"
CONNECTIONS_FILE = CONFIG_DIR / "connections.yml"

load_dotenv(ROOT / ".env")

# Env var that holds each source's API key value, and the connector config key it
# maps to in the Airbyte source configuration (mirrors setup_airbyte.py).
API_KEY_ENV = {"openweather": "OPENWEATHER_API_KEY", "tomtom": "TOMTOM_API_KEY"}
API_KEY_FIELD = {"openweather": "appid", "tomtom": "api_key"}

# ── Field mappings — transcribed verbatim from the stg_*.sql SELECT lists ─────
# Each tuple: (target_column, source_expr, data_type, is_required, description)
# source_expr is a SQL expression over the raw Airbyte row; data_type is an
# optional cast (None = source_expr already yields the final type). The staging
# engine emits:  source_expr [::data_type] as target_column
# raw_id (_airbyte_raw_id) and extracted_at (_airbyte_extracted_at) are emitted
# by the engine as a fixed header, so they are NOT listed here.

FIELD_MAPPINGS = {
    "current_weather": [
        ("city",                      "city",                                                          None,      True,  "City name (injected by Airbyte AddFields from source config)"),
        ("api_locality",              "name",                                                          None,      False, "API locality/district name, kept for reference"),
        ("latitude",                  "(coord->>'lat')",                                               "numeric", False, "Latitude of the observation point"),
        ("longitude",                 "(coord->>'lon')",                                               "numeric", False, "Longitude of the observation point"),
        ("country",                   "(sys->>'country')",                                             "text",    False, "ISO country code"),
        ("temp_celsius",              "(main->>'temp')",                                               "numeric", True,  "Air temperature (deg C, units=metric)"),
        ("feels_like_celsius",        "(main->>'feels_like')",                                         "numeric", False, "Feels-like temperature (deg C)"),
        ("temp_min_celsius",          "(main->>'temp_min')",                                           "numeric", False, "Minimum temperature at the moment (deg C)"),
        ("temp_max_celsius",          "(main->>'temp_max')",                                           "numeric", False, "Maximum temperature at the moment (deg C)"),
        ("humidity_pct",              "(main->>'humidity')",                                           "integer", False, "Relative humidity (percent)"),
        ("pressure_hpa",              "(main->>'pressure')",                                           "integer", False, "Atmospheric pressure (hPa)"),
        ("sea_level_pressure_hpa",    "(main->>'sea_level')",                                          "integer", False, "Sea-level pressure (hPa)"),
        ("ground_level_pressure_hpa", "(main->>'grnd_level')",                                         "integer", False, "Ground-level pressure (hPa)"),
        ("wind_speed_ms",             "(wind->>'speed')",                                              "numeric", False, "Wind speed (m/s)"),
        ("wind_direction_deg",        "(wind->>'deg')",                                                "integer", False, "Wind direction (degrees)"),
        ("wind_gust_ms",              "(wind->>'gust')",                                               "numeric", False, "Wind gust (m/s)"),
        ("weather_main",              "(weather->0->>'main')",                                         "text",    False, "Weather condition group (Rain, Clear, ...)"),
        ("weather_description",       "(weather->0->>'description')",                                  "text",    False, "Weather condition description"),
        ("cloudiness_pct",            "(clouds->>'all')",                                              "integer", False, "Cloudiness (percent)"),
        ("visibility_m",              "visibility",                                                    None,      False, "Visibility (metres; OpenWeather caps at 10000)"),
        ("rain_1h_mm",                "(rain->>'1h')",                                                 "numeric", False, "Rain volume, last 1h (mm)"),
        ("snow_1h_mm",                "(snow->>'1h')",                                                 "numeric", False, "Snow volume, last 1h (mm)"),
        ("observed_at",               "to_timestamp(dt) at time zone 'UTC'",                           None,      True,  "Observation time (UTC)"),
        ("sunrise_at",                "to_timestamp((sys->>'sunrise')::integer) at time zone 'UTC'",   None,      False, "Sunrise time (UTC)"),
        ("sunset_at",                 "to_timestamp((sys->>'sunset')::integer) at time zone 'UTC'",    None,      False, "Sunset time (UTC)"),
        ("timezone_offset_sec",       "timezone",                                                      None,      False, "Shift in seconds from UTC"),
    ],
    "air_pollution": [
        ("city",        "city",                None,      True,  "City name (injected by Airbyte AddFields)"),
        ("aqi",         "(main->>'aqi')",      "integer", True,  "Air Quality Index (OpenWeather 1=Good..5=Very Poor)"),
        ("aqi_label",   "case (main->>'aqi')::integer when 1 then 'Good' when 2 then 'Fair' when 3 then 'Moderate' when 4 then 'Poor' when 5 then 'Very Poor' end", None, False, "Human-readable AQI band"),
        ("co_ug_m3",    "(components->>'co')",    "numeric", False, "Carbon monoxide (ug/m3)"),
        ("no_ug_m3",    "(components->>'no')",    "numeric", False, "Nitrogen monoxide (ug/m3)"),
        ("no2_ug_m3",   "(components->>'no2')",   "numeric", False, "Nitrogen dioxide (ug/m3)"),
        ("o3_ug_m3",    "(components->>'o3')",    "numeric", False, "Ozone (ug/m3)"),
        ("so2_ug_m3",   "(components->>'so2')",   "numeric", False, "Sulphur dioxide (ug/m3)"),
        ("pm2_5_ug_m3", "(components->>'pm2_5')", "numeric", True,  "Fine particulates PM2.5 (ug/m3)"),
        ("pm10_ug_m3",  "(components->>'pm10')",  "numeric", False, "Coarse particulates PM10 (ug/m3)"),
        ("nh3_ug_m3",   "(components->>'nh3')",   "numeric", False, "Ammonia (ug/m3)"),
        ("observed_at", "to_timestamp(dt) at time zone 'UTC'", None, True, "Observation time (UTC)"),
    ],
    "weather_forecast": [
        ("city",                          "city",                                     None,      True,  "City name (injected by Airbyte AddFields)"),
        ("forecast_at",                   "to_timestamp(dt) at time zone 'UTC'",      None,      True,  "Forecast valid time (UTC)"),
        ("forecast_dt_txt",               "dt_txt",                                   None,      False, "Forecast time as API text"),
        ("precipitation_probability",     "pop",                                      None,      False, "Probability of precipitation (0.0-1.0)"),
        ("precipitation_probability_pct", "round((pop * 100)::numeric, 0)::integer",  None,      False, "Probability of precipitation (percent)"),
        ("temp_celsius",                  "(main->>'temp')",                          "numeric", True,  "Forecast temperature (deg C)"),
        ("feels_like_celsius",            "(main->>'feels_like')",                    "numeric", False, "Feels-like temperature (deg C)"),
        ("temp_min_celsius",              "(main->>'temp_min')",                      "numeric", False, "Minimum temperature (deg C)"),
        ("temp_max_celsius",              "(main->>'temp_max')",                      "numeric", False, "Maximum temperature (deg C)"),
        ("humidity_pct",                  "(main->>'humidity')",                      "integer", False, "Relative humidity (percent)"),
        ("pressure_hpa",                  "(main->>'pressure')",                      "integer", False, "Atmospheric pressure (hPa)"),
        ("sea_level_pressure_hpa",        "(main->>'sea_level')",                     "integer", False, "Sea-level pressure (hPa)"),
        ("ground_level_pressure_hpa",     "(main->>'grnd_level')",                    "integer", False, "Ground-level pressure (hPa)"),
        ("wind_speed_ms",                 "(wind->>'speed')",                         "numeric", False, "Wind speed (m/s)"),
        ("wind_direction_deg",            "(wind->>'deg')",                           "integer", False, "Wind direction (degrees)"),
        ("wind_gust_ms",                  "(wind->>'gust')",                          "numeric", False, "Wind gust (m/s)"),
        ("weather_main",                  "(weather->0->>'main')",                    "text",    False, "Weather condition group"),
        ("weather_description",           "(weather->0->>'description')",             "text",    False, "Weather condition description"),
        ("cloudiness_pct",                "(clouds->>'all')",                         "integer", False, "Cloudiness (percent)"),
        ("visibility_m",                  "visibility",                               None,      False, "Visibility (metres)"),
        ("rain_3h_mm",                    "(rain->>'3h')",                            "numeric", False, "Rain volume, 3h window (mm)"),
        ("snow_3h_mm",                    "(snow->>'3h')",                            "numeric", False, "Snow volume, 3h window (mm)"),
        ("day_or_night",                  "(sys->>'pod')",                            "text",    False, "Part of day: 'd' or 'n'"),
    ],
    "traffic_flow": [
        ("city",                      "city",                  None, True,  "City name (injected by Airbyte AddFields)"),
        ("road_class",                "frc",                   None, False, "Functional road class (FRC0=motorway..FRC7=local)"),
        ("road_closure",              '"roadClosure"',         None, False, "Whether the road segment is closed"),
        ("current_speed_kmh",         '"currentSpeed"',        None, True,  "Current speed (km/h)"),
        ("free_flow_speed_kmh",       '"freeFlowSpeed"',       None, True,  "Free-flow speed (km/h)"),
        ("current_travel_time_sec",   '"currentTravelTime"',   None, False, "Current travel time (s)"),
        ("free_flow_travel_time_sec", '"freeFlowTravelTime"',  None, False, "Free-flow travel time (s)"),
        ("congestion_score",          'round(1.0 - ("currentSpeed"::numeric / nullif("freeFlowSpeed", 0)::numeric), 2)', None, False, "Congestion: 0=free flow, 1=fully congested"),
        ("confidence",                "confidence",            None, False, "TomTom data-quality confidence (0-1)"),
        ("observed_at",               "(_airbyte_extracted_at at time zone 'UTC')", None, True, "Sync time used as observation time (naive UTC)"),
    ],
    # traffic_incidents: NO min_row_count rule below — zero incidents is a valid,
    # healthy state (a city with no active incidents), not a data failure.
    "traffic_incidents": [
        ("city",               "city",                                None,                       True,  "City name (injected by Airbyte AddFields)"),
        ("incident_id",        "(properties->>'id')",                 "text",                     True,  "TomTom incident id"),
        ("feature_type",       "type",                                None,                       False, "GeoJSON feature type"),
        ("road_from",          "(properties->>'from')",               "text",                     False, "Segment start description"),
        ("road_to",            "(properties->>'to')",                 "text",                     False, "Segment end description"),
        ("started_at",         "(properties->>'startTime')",          "timestamp with time zone", False, "Incident start time"),
        ("ends_at",            "(properties->>'endTime')",            "timestamp with time zone", False, "Incident end time"),
        ("time_validity",      "(properties->>'timeValidity')",       "text",                     False, "Time validity flag"),
        ("magnitude_of_delay", "(properties->>'magnitudeOfDelay')",   "integer",                  False, "0=Unknown,1=Minor,2=Moderate,3=Major,4=Undefined"),
        ("delay_severity",     "case (properties->>'magnitudeOfDelay')::integer when 0 then 'Unknown' when 1 then 'Minor' when 2 then 'Moderate' when 3 then 'Major' when 4 then 'Undefined' end", None, False, "Human-readable delay severity"),
        ("delay_sec",          "(properties->>'delay')",              "integer",                  False, "Delay caused by the incident (s)"),
        ("length_m",           "(properties->>'length')",             "numeric",                  False, "Affected length (m)"),
        ("category_id",        "(properties->>'iconCategory')",       "integer",                  False, "TomTom icon category id"),
        ("number_of_reports",  "(properties->>'numberOfReports')",    "integer",                  False, "Number of reports"),
        ("probability",        "(properties->>'probabilityOfOccurrence')", "text",                False, "Probability of occurrence"),
        ("geometry",           "geometry",                            None,                       False, "GeoJSON geometry (coordinates) as JSONB"),
        ("observed_at",        "(_airbyte_extracted_at at time zone 'UTC')", None,                True,  "Sync time used as observation time (naive UTC)"),
    ],
}

# ── Validation rules — starter set (STEP 03 "Define Rules"), tune in the DB ────
# Each tuple: (target_column | None, rule_type, rule_value, severity, description)
# target_column None = stream-level rule (min_row_count, freshness_minutes).
VALIDATION_RULES = {
    "current_weather": [
        (None,           "min_row_count",     "1",   "error", "At least one weather row per sync"),
        ("temp_celsius", "min",               "-60", "warn",  "Plausible air-temperature lower bound (deg C)"),
        ("temp_celsius", "max",               "60",  "warn",  "Plausible air-temperature upper bound (deg C)"),
        ("humidity_pct", "max",               "100", "warn",  "Humidity cannot exceed 100 percent"),
        (None,           "freshness_minutes", "120", "warn",  "Latest observation should be within 2h of the run"),
    ],
    "air_pollution": [
        (None,  "min_row_count",   "1",           "error", "At least one pollution row per sync"),
        ("aqi", "accepted_values", "[1,2,3,4,5]", "error", "AQI must be on the OpenWeather 1-5 scale"),
    ],
    "weather_forecast": [
        (None,           "min_row_count", "1",   "error", "At least one forecast row per sync"),
        ("temp_celsius", "min",           "-60", "warn",  "Plausible forecast-temperature lower bound (deg C)"),
        ("temp_celsius", "max",           "60",  "warn",  "Plausible forecast-temperature upper bound (deg C)"),
    ],
    "traffic_flow": [
        (None,                "min_row_count", "1", "error", "At least one flow row per sync"),
        ("current_speed_kmh", "min",           "0", "warn",  "Speed cannot be negative"),
    ],
    "traffic_incidents": [
        ("magnitude_of_delay", "accepted_values", "[0,1,2,3,4]", "warn", "TomTom magnitudeOfDelay domain"),
    ],
}


def get_conn():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "smart_city"),
        user=os.getenv("POSTGRES_USER", "postgres"),
        password=os.getenv("POSTGRES_PASSWORD"),
    )


def seed_sources(cur, sources_cfg, sync_cfg) -> dict:
    """Upsert config.sources; return {source_name: source_id}. Preserves is_active."""
    ids = {}
    for source_name, cfg in sources_cfg.items():
        cur.execute(
            """
            insert into config.sources
                (source_name, connector_name, api_key_env, api_key_field, schedule_cron, is_active)
            values (%s, %s, %s, %s, %s, true)
            on conflict (source_name) do update set
                connector_name = excluded.connector_name,
                api_key_env    = excluded.api_key_env,
                api_key_field  = excluded.api_key_field,
                schedule_cron  = excluded.schedule_cron
            returning source_id
            """,
            (source_name, cfg["connector_name"], API_KEY_ENV.get(source_name),
             API_KEY_FIELD.get(source_name), sync_cfg.get("schedule")),
        )
        ids[source_name] = cur.fetchone()[0]
    return ids


def seed_streams(cur, sources_cfg, sync_cfg, source_ids) -> dict:
    """Upsert config.streams; return {(source_name, stream_name): stream_id}."""
    ids = {}
    sync_mode = sync_cfg.get("sync_mode", "full_refresh_append")
    for source_name, cfg in sources_cfg.items():
        for stream_name in cfg["streams"]:
            cur.execute(
                """
                insert into config.streams
                    (source_id, stream_name, target_schema, target_table, sync_mode, is_active)
                values (%s, %s, 'staging', %s, %s, true)
                on conflict (source_id, stream_name) do update set
                    target_schema = excluded.target_schema,
                    target_table  = excluded.target_table,
                    sync_mode     = excluded.sync_mode
                returning stream_id
                """,
                (source_ids[source_name], stream_name, stream_name, sync_mode),
            )
            ids[(source_name, stream_name)] = cur.fetchone()[0]
    return ids


def seed_locations(cur, sources_cfg) -> dict:
    """Upsert config.locations (union of all cities); return {city: location_id}."""
    cities = {}  # city -> (lat, lon); first occurrence wins (weather & tomtom agree)
    for cfg in sources_cfg.values():
        for loc in cfg["locations"]:
            cities.setdefault(loc["city"], (loc["lat"], loc["lon"]))
    ids = {}
    for city, (lat, lon) in cities.items():
        cur.execute(
            """
            insert into config.locations (city, latitude, longitude, is_active)
            values (%s, %s, %s, true)
            on conflict (city) do update set
                latitude  = excluded.latitude,
                longitude = excluded.longitude
            returning location_id
            """,
            (city, lat, lon),
        )
        ids[city] = cur.fetchone()[0]
    return ids


def seed_source_locations(cur, sources_cfg, source_ids, location_ids) -> int:
    """Upsert config.source_locations (bbox only where the YAML provides it)."""
    n = 0
    for source_name, cfg in sources_cfg.items():
        for loc in cfg["locations"]:
            cur.execute(
                """
                insert into config.source_locations
                    (source_id, location_id, min_lat, min_lon, max_lat, max_lon, is_active)
                values (%s, %s, %s, %s, %s, %s, true)
                on conflict (source_id, location_id) do update set
                    min_lat = excluded.min_lat,
                    min_lon = excluded.min_lon,
                    max_lat = excluded.max_lat,
                    max_lon = excluded.max_lon
                """,
                (source_ids[source_name], location_ids[loc["city"]],
                 loc.get("min_lat"), loc.get("min_lon"),
                 loc.get("max_lat"), loc.get("max_lon")),
            )
            n += 1
    return n


def seed_field_mappings(cur, stream_ids) -> int:
    """Upsert config.field_mappings. Refreshes definitions; preserves is_active/is_required."""
    n = 0
    for (source_name, stream_name), stream_id in stream_ids.items():
        mappings = FIELD_MAPPINGS.get(stream_name, [])
        rows = [
            (stream_id, target, expr, dtype, required, ordinal, desc)
            for ordinal, (target, expr, dtype, required, desc) in enumerate(mappings, start=1)
        ]
        execute_values(
            cur,
            """
            insert into config.field_mappings
                (stream_id, target_column, source_expr, data_type, is_required, ordinal, description)
            values %s
            on conflict (stream_id, target_column) do update set
                source_expr = excluded.source_expr,
                data_type   = excluded.data_type,
                ordinal     = excluded.ordinal,
                description = excluded.description
            """,
            rows,
        )
        n += len(rows)
    return n


def seed_validation_rules(cur, stream_ids) -> int:
    """Upsert config.validation_rules. Refreshes value/severity; preserves is_active."""
    n = 0
    for (source_name, stream_name), stream_id in stream_ids.items():
        rules = VALIDATION_RULES.get(stream_name, [])
        rows = [
            (stream_id, target, rtype, rvalue, severity, desc)
            for (target, rtype, rvalue, severity, desc) in rules
        ]
        if not rows:
            continue
        execute_values(
            cur,
            """
            insert into config.validation_rules
                (stream_id, target_column, rule_type, rule_value, severity, description)
            values %s
            on conflict (stream_id, target_column, rule_type) do update set
                rule_value  = excluded.rule_value,
                severity    = excluded.severity,
                description = excluded.description
            """,
            rows,
        )
        n += len(rows)
    return n


def main():
    sources_cfg = yaml.safe_load(SOURCES_FILE.read_text())
    conn_cfg = yaml.safe_load(CONNECTIONS_FILE.read_text())
    sync_cfg = conn_cfg.get("sync", {})

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            source_ids = seed_sources(cur, sources_cfg, sync_cfg)
            stream_ids = seed_streams(cur, sources_cfg, sync_cfg, source_ids)
            location_ids = seed_locations(cur, sources_cfg)
            n_srcloc = seed_source_locations(cur, sources_cfg, source_ids, location_ids)
            n_fields = seed_field_mappings(cur, stream_ids)
            n_rules = seed_validation_rules(cur, stream_ids)
        conn.commit()
    finally:
        conn.close()

    print("Seeded config schema:")
    print(f"  sources          : {len(source_ids)}")
    print(f"  streams          : {len(stream_ids)}")
    print(f"  locations        : {len(location_ids)}")
    print(f"  source_locations : {n_srcloc}")
    print(f"  field_mappings   : {n_fields}")
    print(f"  validation_rules : {n_rules}")
    print("Done. The DB is now the source of truth — edit config.* with SQL from here on.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\nFailed: {e}", file=sys.stderr)
        sys.exit(1)
