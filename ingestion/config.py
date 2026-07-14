# config.py — cities, API keys, and database configuration
# Single place to add new cities

import os
from dotenv import load_dotenv

# Load secrets from .env file

load_dotenv()

# CITY LIST — same 10 cities as ingestion/config/sources.yml (Airbyte setup)
# To add a new city: copy one line and change the values
# lat/lon coordinates available at: https://www.latlong.net/
# bbox format: "min_lon,min_lat,max_lon,max_lat" (get at bboxfinder.com)
# Macedonian cities are weather-only — TomTom has no traffic coverage there.

CITIES = [
    {"name": "Skopje",    "country": "MK", "lat": 41.9981, "lon": 21.4321, "bbox": None,                      "timezone": "Europe/Skopje",    "has_traffic_data": False},
    {"name": "Berlin",    "country": "DE", "lat": 52.5200, "lon": 13.4050, "bbox": "13.20,52.40,13.65,52.65", "timezone": "Europe/Berlin",    "has_traffic_data": True},
    {"name": "London",    "country": "GB", "lat": 51.5074, "lon": -0.1278, "bbox": "-0.25,51.40,0.00,51.60",  "timezone": "Europe/London",    "has_traffic_data": True},
    {"name": "Amsterdam", "country": "NL", "lat": 52.3676, "lon": 4.9041,  "bbox": "4.75,52.28,5.05,52.45",   "timezone": "Europe/Amsterdam", "has_traffic_data": True},
    {"name": "Belgrade",  "country": "RS", "lat": 44.7866, "lon": 20.4489, "bbox": "20.35,44.70,20.55,44.87", "timezone": "Europe/Belgrade",  "has_traffic_data": True},
    {"name": "Brussels",  "country": "BE", "lat": 50.8503, "lon": 4.3517,  "bbox": "4.25,50.78,4.45,50.92",   "timezone": "Europe/Brussels",  "has_traffic_data": True},
    {"name": "Barcelona", "country": "ES", "lat": 41.3874, "lon": 2.1686,  "bbox": "2.08,41.32,2.25,41.46",   "timezone": "Europe/Madrid",    "has_traffic_data": True},
    {"name": "Prilep",    "country": "MK", "lat": 41.3442, "lon": 21.5544, "bbox": None,                      "timezone": "Europe/Skopje",    "has_traffic_data": False},
    {"name": "Bitola",    "country": "MK", "lat": 41.0314, "lon": 21.3347, "bbox": None,                      "timezone": "Europe/Skopje",    "has_traffic_data": False},
    {"name": "Ohrid",     "country": "MK", "lat": 41.1231, "lon": 20.8016, "bbox": None,                      "timezone": "Europe/Skopje",    "has_traffic_data": False},
]

# API KEYS ARE READ FROM .ENV

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY")

# POSTGRESQL DATABASE CONNECTION

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5434)),
    "dbname": os.getenv("POSTGRES_DB", "smart_city"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}
