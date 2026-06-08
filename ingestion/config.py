# config.py — cities, API keys, and database configuration
# Single place to add new cities

import os
from dotenv import load_dotenv

# Load secrets from .env file

load_dotenv()

# CITY LIST
# To add a new city: copy one line and change the values
# lat/lon coordinates available at: https://www.latlong.net/

CITIES = [
    {"name": "Berlin", "lat": 52.52, "lon": 13.405, "bbox": "13.28,52.46,13.54,52.58"},
    {"name": "Madrid", "lat": 40.4168, "lon": -3.7038, "bbox": "-3.83,40.33,-3.57,40.50"},
    {"name": "London", "lat": 51.5074, "lon": -0.1278, "bbox":"-0.246,51.38,0.146,51.68"},
    {"name": "Amsterdam", "lat":52.3676, "lon":4.9041, "bbox":"4.78,52.28,5.08,52.48"}

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