# =============================================================
# ingest.py — Ingestion скрипта за Smart City проект
#
# Тек на извршување:
#   за секој град во config.CITIES:
#       1. OpenWeather → current_weather
#       2. OpenWeather → air_pollution
#       3. OpenWeather → weather_forecast (5 дена)
#       4. TomTom     → traffic_flow
#       5. TomTom     → traffic_incidents (по bbox)
#   Сè се зачувува во airbyte_raw schema (PostgreSQL)
#
# Употреба:
#   python ingest.py
# =============================================================

import uuid
import json
import requests
import psycopg2
from datetime import datetime, timezone
from config import CITIES, OPENWEATHER_API_KEY, TOMTOM_API_KEY, DB_CONFIG


# -------------------------------------------------------------
# ПОМОШНА ФУНКЦИЈА — INSERT во airbyte_raw табела
# -------------------------------------------------------------
def insert_raw(cursor, table: str, data: dict):
    """
    Зачувува еден запис во airbyte_raw.<table>.

    Секој ред добива:
      _airbyte_raw_id       — уникатен UUID за секој запис
      _airbyte_extracted_at — момент кога е земено
      останатите полиња     — директно од API одговорот (рамни колони)

    dict/list вредности се конвертираат во JSON string
    бидејќи psycopg2 не може директно да вметне Python dict во PostgreSQL.
    """
    now = datetime.now(timezone.utc)

    # Конвертирај dict/list вредности во JSON string
    # Прескокни клучеви со специјални знаци (пр. @version од TomTom) — невалидни SQL колони
    serialized = {}
    for key, value in data.items():
        if not key.isidentifier():          # прескокни @version, @type и слични
            continue
        if isinstance(value, (dict, list)):
            serialized[key] = json.dumps(value)
        else:
            serialized[key] = value

    # Задолжителни Airbyte системски колони
    columns = ["_airbyte_raw_id", "_airbyte_extracted_at", "_airbyte_meta", "_airbyte_generation_id"] + list(serialized.keys())
    values  = [str(uuid.uuid4()), now, json.dumps({}), 0] + list(serialized.values())

    # Двојни кавички за да се зачува case (currentSpeed ≠ currentspeed)
    col_str = ", ".join(f'"{c}"' for c in columns)
    val_str = ", ".join(["%s"] * len(values))

    cursor.execute(
        f'INSERT INTO airbyte_raw.{table} ({col_str}) VALUES ({val_str})',
        values
    )


# -------------------------------------------------------------
# OPENWEATHER — МОМЕНТАЛНА ВРЕМЕНСКА СОСТОЈБА
# Документација: https://openweathermap.org/current
# -------------------------------------------------------------
def fetch_current_weather(city: dict) -> dict:
    """
    Враќа моменталната временска состојба за градот:
    температура, влажност, притисок, ветер, облачност...
    """
    url = "https://api.openweathermap.org/data/2.5/weather"

    params = {
        "lat":   city["lat"],
        "lon":   city["lon"],
        "appid": OPENWEATHER_API_KEY,
        "units": "metric",    # Целзиус, km/h
    }

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()

    data = response.json()
    # city_name не се додава — табелата нема таа колона
    # името на градот е веќе во полето "name" од API одговорот
    return data


# -------------------------------------------------------------
# OPENWEATHER — ЗАГАДУВАЊЕ НА ВОЗДУХОТ
# Документација: https://openweathermap.org/api/air-pollution
# -------------------------------------------------------------
def fetch_air_pollution(city: dict) -> dict:
    """
    Враќа AQI (1=добар → 5=многу лош) и
    концентрации на: CO, NO, NO2, O3, SO2, PM2.5, PM10, NH3
    """
    url = "https://api.openweathermap.org/data/2.5/air_pollution"

    params = {
        "lat":   city["lat"],
        "lon":   city["lon"],
        "appid": OPENWEATHER_API_KEY,
    }

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()

    # API враќа {"coord": ..., "list": [{...}]}
    # Табелата очекува полиња од list[0]: main, components, dt
    data = response.json()["list"][0]
    return data


# -------------------------------------------------------------
# OPENWEATHER — ПРОГНОЗА ЗА 5 ДЕНА
# Документација: https://openweathermap.org/forecast5
# -------------------------------------------------------------
def fetch_weather_forecast(city: dict) -> list:
    """
    Враќа листа од 40 прогнози (на секои 3 часа, 5 дена).
    Секоја прогноза е посебен ред во базата.
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
# TOMTOM — СООБРАЌАЕН ПРОТОК
# Документација: https://developer.tomtom.com/traffic-api/documentation/traffic-flow/flow-segment-data
# -------------------------------------------------------------
def fetch_traffic_flow(city: dict) -> dict:
    """
    Враќа тековна брзина vs слободна брзина за сегментот
    најблизу до координатите на градот.
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

    # API враќа {"flowSegmentData": {...}}
    # Табелата очекува само: frc, currentSpeed, freeFlowSpeed, currentTravelTime, freeFlowTravelTime, confidence, roadClosure
    # "coordinates" го нема во табелата — го отстрануваме
    ALLOWED_KEYS = {"frc", "currentSpeed", "freeFlowSpeed", "currentTravelTime",
                    "freeFlowTravelTime", "confidence", "roadClosure"}
    raw  = response.json()["flowSegmentData"]
    data = {k: v for k, v in raw.items() if k in ALLOWED_KEYS}
    return data


# -------------------------------------------------------------
# TOMTOM — СООБРАЌАЈНИ ИНЦИДЕНТИ
# Документација: https://developer.tomtom.com/traffic-api/documentation/traffic-incidents/incident-details
# -------------------------------------------------------------
def fetch_traffic_incidents(city: dict) -> list:
    """
    Враќа листа на активни инциденти (несреќи, затворања, задоцнувања)
    во bounding box-от на градот.
    Секој инцидент е посебен ред во базата.
    """
    url = "https://api.tomtom.com/traffic/services/5/incidentDetails"

    params = {
        "bbox":     city["bbox"],   # minLon,minLat,maxLon,maxLat
        "key":      TOMTOM_API_KEY,
        "language": "en-GB",
        "categoryFilter": "0,1,2,3,4,5,6,7,8,9,10,11",  # сите категории
    }

    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()

    incidents = response.json().get("incidents", [])
    return incidents


# -------------------------------------------------------------
# ГЛАВНА ФУНКЦИЈА
# -------------------------------------------------------------
def run_ingestion():
    print(f"\n[{datetime.now()}] ▶ Почнуваме ingestion за {len(CITIES)} градови...")

    conn   = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()

    for city in CITIES:
        print(f"\n  🌍 {city['name']}")

        # 1. Моментална временска состојба
        try:
            cursor.execute("SAVEPOINT sp")
            data = fetch_current_weather(city)
            insert_raw(cursor, "current_weather", data)
            print(f"     ✓ current_weather")
        except Exception as e:
            cursor.execute("ROLLBACK TO SAVEPOINT sp")  # откажи само овој insert
            print(f"     ✗ current_weather: {e}")

        # 2. Загадување на воздухот
        try:
            cursor.execute("SAVEPOINT sp")
            data = fetch_air_pollution(city)
            insert_raw(cursor, "air_pollution", data)
            print(f"     ✓ air_pollution")
        except Exception as e:
            cursor.execute("ROLLBACK TO SAVEPOINT sp")
            print(f"     ✗ air_pollution: {e}")

        # 3. Прогноза (40 редови по град)
        try:
            cursor.execute("SAVEPOINT sp")
            forecasts = fetch_weather_forecast(city)
            for forecast in forecasts:
                insert_raw(cursor, "weather_forecast", forecast)
            print(f"     ✓ weather_forecast ({len(forecasts)} прогнози)")
        except Exception as e:
            cursor.execute("ROLLBACK TO SAVEPOINT sp")
            print(f"     ✗ weather_forecast: {e}")

        # 4. Сообраќаен проток
        try:
            cursor.execute("SAVEPOINT sp")
            data = fetch_traffic_flow(city)
            insert_raw(cursor, "traffic_flow", data)
            print(f"     ✓ traffic_flow")
        except Exception as e:
            cursor.execute("ROLLBACK TO SAVEPOINT sp")
            print(f"     ✗ traffic_flow: {e}")

        # 5. Сообраќајни инциденти
        try:
            cursor.execute("SAVEPOINT sp")
            incidents = fetch_traffic_incidents(city)
            for incident in incidents:
                insert_raw(cursor, "traffic_incidents", incident)
            print(f"     ✓ traffic_incidents ({len(incidents)} инциденти)")
        except Exception as e:
            cursor.execute("ROLLBACK TO SAVEPOINT sp")
            print(f"     ✗ traffic_incidents: {e}")

    conn.commit()
    cursor.close()
    conn.close()

    print(f"\n[{datetime.now()}] ✅ Завршено!\n")


# -------------------------------------------------------------
# ТОЧКА НА ВЛЕЗ
# -------------------------------------------------------------
if __name__ == "__main__":
    run_ingestion()

