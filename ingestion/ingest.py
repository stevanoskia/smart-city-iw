# =============================================================
# ingest.py — Ingestion script for the Smart City project
#
# Execution flow:
#   for each city in config.CITIES:
#       1. OpenWeather → current_weather
#       2. OpenWeather → air_pollution
#       3. OpenWeather → weather_forecast (5 days)
#       4. TomTom     → traffic_flow
#       5. TomTom     → traffic_incidents (by bbox)
#   Everything is saved to the airbyte_raw schema (PostgreSQL)
#
# Usage:
#   python ingest.py
# =============================================================

import uuid
import json
import requests
import psycopg2
from datetime import datetime, timezone
from config import CITIES, OPENWEATHER_API_KEY, TOMTOM_API_KEY, DB_CONFIG


# -------------------------------------------------------------
# HELPER FUNCTION — Add missing columns to a raw table automatically
# -------------------------------------------------------------
def ensure_columns(cursor, table: str, data: dict):
    """
    Checks each key in data and adds the column to the table if it does not exist.
    This allows ingest.py to add new columns (e.g. city, country) without manual ALTER TABLE.
    """
    for col, val in data.items():
        if not col.isidentifier():
            continue
        if isinstance(val, bool):
            col_type = "BOOLEAN"
        elif isinstance(val, int):
            col_type = "BIGINT"
        elif isinstance(val, float):
            col_type = "NUMERIC"
        else:
            col_type = "TEXT"
        cursor.execute(
            f'ALTER TABLE airbyte_raw.{table} ADD COLUMN IF NOT EXISTS "{col}" {col_type}'
        )


# -------------------------------------------------------------
# HELPER FUNCTION — INSERT into airbyte_raw table
# -------------------------------------------------------------
def insert_raw(cursor, table: str, data: dict):
    """
    Saves one record into airbyte_raw.<table>.

    Each row gets:
      _airbyte_raw_id       — unique UUID per record
      _airbyte_extracted_at — timestamp when data was fetched
      remaining fields      — directly from the API response (flat columns)

    dict/list values are converted to JSON string
    because psycopg2 cannot directly insert a Python dict into PostgreSQL.
    """
    now = datetime.now(timezone.utc)

    # Convert dict/list values to JSON string
    # Skip keys with special characters (e.g. @version from TomTom) — invalid SQL column names
    serialized = {}
    for key, value in data.items():
        if not key.isidentifier():          # skip @version, @type and similar
            continue
        if isinstance(value, (dict, list)):
            serialized[key] = json.dumps(value)
        else:
            serialized[key] = value

    # Automatically add any missing columns to the table before inserting
    ensure_columns(cursor, table, serialized)

    # Required Airbyte system columns
    columns = ["_airbyte_raw_id", "_airbyte_extracted_at", "_airbyte_meta", "_airbyte_generation_id"] + list(serialized.keys())
    values  = [str(uuid.uuid4()), now, json.dumps({}), 0] + list(serialized.values())

    # Double quotes to preserve case (currentSpeed ≠ currentspeed)
    col_str = ", ".join(f'"{c}"' for c in columns)
    val_str = ", ".join(["%s"] * len(values))

    cursor.execute(
        f'INSERT INTO airbyte_raw.{table} ({col_str}) VALUES ({val_str})',
        values
    )


# -------------------------------------------------------------
# OPENWEATHER — CURRENT WEATHER
# Docs: https://openweathermap.org/current
# -------------------------------------------------------------
def fetch_current_weather(city: dict) -> dict:
    """
    Returns the current weather conditions for the city:
    temperature, humidity, pressure, wind, cloudiness...
    """
    url = "https://api.openweathermap.org/data/2.5/weather"

    params = {
        "lat":   city["lat"],
        "lon":   city["lon"],
        "appid": OPENWEATHER_API_KEY,
        "units": "metric",    # Celsius, km/h
    }

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()

    data = response.json()
    # Force our city name from config.py instead of API district name (Mitte, Sol...)
    data["name"] = city["name"]
    return data


# -------------------------------------------------------------
# OPENWEATHER — AIR POLLUTION
# Docs: https://openweathermap.org/api/air-pollution
# -------------------------------------------------------------
def fetch_air_pollution(city: dict) -> dict:
    """
    Returns AQI (1=good → 5=very poor) and
    concentrations of: CO, NO, NO2, O3, SO2, PM2.5, PM10, NH3
    """
    url = "https://api.openweathermap.org/data/2.5/air_pollution"

    params = {
        "lat":   city["lat"],
        "lon":   city["lon"],
        "appid": OPENWEATHER_API_KEY,
    }

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()

    # API returns {"coord": ..., "list": [{...}]}
    # Table expects fields from list[0]: main, components, dt
    data = response.json()["list"][0]
    return data


# -------------------------------------------------------------
# OPENWEATHER — 5-DAY FORECAST
# Docs: https://openweathermap.org/forecast5
# -------------------------------------------------------------
def fetch_weather_forecast(city: dict) -> list:
    """
    Returns a list of 40 forecasts (every 3 hours, 5 days).
    Each forecast is a separate row in the database.
    """
    url = "https://api.openweathermap.org/data/2.5/forecast"

    params = {
        "lat":   city["lat"],
        "lon":   city["lon"],
        "appid": OPENWEATHER_API_KEY,
        "units": "metric",
    }

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()

    forecasts = response.json().get("list", [])

    return forecasts


# -------------------------------------------------------------
# TOMTOM — TRAFFIC FLOW
# Docs: https://developer.tomtom.com/traffic-api/documentation/traffic-flow/flow-segment-data
# -------------------------------------------------------------
def fetch_traffic_flow(city: dict) -> dict:
    """
    Returns current speed vs free-flow speed for the segment
    closest to the city coordinates.
    congestion_score = 1 - (currentSpeed / freeFlowSpeed)
    """
    url = "https://api.tomtom.com/traffic/services/4/flowSegmentData/absolute/10/json"

    params = {
        "point": f"{city['lat']},{city['lon']}",
        "key":   TOMTOM_API_KEY,
        "unit":  "kmph",
    }

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()

    # API returns {"flowSegmentData": {...}}
    # Table expects only: frc, currentSpeed, freeFlowSpeed, currentTravelTime, freeFlowTravelTime, confidence, roadClosure
    # "coordinates" is not in the table — we remove it
    ALLOWED_KEYS = {"frc", "currentSpeed", "freeFlowSpeed", "currentTravelTime",
                    "freeFlowTravelTime", "confidence", "roadClosure"}
    raw  = response.json()["flowSegmentData"]
    data = {k: v for k, v in raw.items() if k in ALLOWED_KEYS}
    return data


# -------------------------------------------------------------
# TOMTOM — TRAFFIC INCIDENTS
# Docs: https://developer.tomtom.com/traffic-api/documentation/traffic-incidents/incident-details
# -------------------------------------------------------------
def fetch_traffic_incidents(city: dict) -> list:
    """
    Returns a list of active incidents (accidents, closures, delays)
    within the city's bounding box.
    Each incident is a separate row in the database.
    """
    url = "https://api.tomtom.com/traffic/services/5/incidentDetails"

    params = {
        "bbox":             city["bbox"],
        "key":              TOMTOM_API_KEY,
        "language":         "en-GB",
        "categoryFilter":   "0,1,2,3,4,5,6,7,8,9,10,11",
        "timeValidityFilter": "present",
        # Without fields, TomTom returns only type+geometry+iconCategory
        # With fields we get full details: id, from, to, delay, length...
        "fields": "{incidents{type,geometry{type,coordinates},properties{id,iconCategory,magnitudeOfDelay,startTime,endTime,from,to,length,delay,timeValidity,probabilityOfOccurrence,numberOfReports}}}",
    }

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()

    incidents = response.json().get("incidents", [])
    return incidents


# -------------------------------------------------------------
# MAIN FUNCTION
# -------------------------------------------------------------
def run_ingestion():
    print(f"\n[{datetime.now()}] ▶ Starting ingestion for {len(CITIES)} cities...")

    conn   = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    for city in CITIES:
        print(f"\n  🌍 {city['name']}")

        # 1. Current weather
        try:
            cursor.execute("SAVEPOINT sp")
            data = fetch_current_weather(city)
            data["city_timezone"]    = city["timezone"]
            data["has_traffic_data"] = city["has_traffic_data"]
            insert_raw(cursor, "current_weather", data)
            print(f"     ✓ current_weather")
        except Exception as e:
            cursor.execute("ROLLBACK TO SAVEPOINT sp")  # roll back only this insert
            print(f"     ✗ current_weather: {e}")

        # 2. Air pollution
        try:
            cursor.execute("SAVEPOINT sp")
            data = fetch_air_pollution(city)
            # Add city metadata — API response does not include city name
            data["city"]             = city["name"]
            data["country"]          = city.get("country")
            data["city_timezone"]    = city["timezone"]
            data["has_traffic_data"] = city["has_traffic_data"]
            insert_raw(cursor, "air_pollution", data)
            print(f"     ✓ air_pollution")
        except Exception as e:
            cursor.execute("ROLLBACK TO SAVEPOINT sp")
            print(f"     ✗ air_pollution: {e}")

        # 3. Forecast (40 rows per city)
        try:
            cursor.execute("SAVEPOINT sp")
            forecasts = fetch_weather_forecast(city)
            for forecast in forecasts:
                # Add city metadata — API response does not include city name
                forecast["city"]             = city["name"]
                forecast["country"]          = city.get("country")
                forecast["city_timezone"]    = city["timezone"]
                forecast["has_traffic_data"] = city["has_traffic_data"]
                insert_raw(cursor, "weather_forecast", forecast)
            print(f"     ✓ weather_forecast ({len(forecasts)} forecasts)")
        except Exception as e:
            cursor.execute("ROLLBACK TO SAVEPOINT sp")
            print(f"     ✗ weather_forecast: {e}")

        # 4. Traffic flow
        try:
            cursor.execute("SAVEPOINT sp")
            data = fetch_traffic_flow(city)
            # Add city metadata — API response does not include city name
            data["city"]             = city["name"]
            data["country"]          = city.get("country")
            data["city_timezone"]    = city["timezone"]
            data["has_traffic_data"] = city["has_traffic_data"]
            insert_raw(cursor, "traffic_flow", data)
            print(f"     ✓ traffic_flow")
        except Exception as e:
            cursor.execute("ROLLBACK TO SAVEPOINT sp")
            print(f"     ✗ traffic_flow: {e}")

        # 5. Traffic incidents
        try:
            cursor.execute("SAVEPOINT sp")
            incidents = fetch_traffic_incidents(city)
            for incident in incidents:
                # Add city metadata so incidents can be analyzed by city later
                incident["city"]             = city["name"]
                incident["country"]          = city.get("country")
                incident["city_timezone"]    = city["timezone"]
                incident["has_traffic_data"] = city["has_traffic_data"]
                insert_raw(cursor, "traffic_incidents", incident)
            print(f"     ✓ traffic_incidents ({len(incidents)} incidents)")
        except Exception as e:
            cursor.execute("ROLLBACK TO SAVEPOINT sp")
            print(f"     ✗ traffic_incidents: {e}")

    conn.commit()
    cursor.close()
    conn.close()

    print(f"\n[{datetime.now()}] ✅ Done!\n")


# -------------------------------------------------------------
# ENTRY POINT
# -------------------------------------------------------------
if __name__ == "__main__":
    run_ingestion()

