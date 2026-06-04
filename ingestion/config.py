#config.py postavuvanje gradovi api klucevi i baza
#edinstveno mesto kaj sto se dodavaat novi gradovi

import os
from dotenv import load_dotenv

#vcituvanje na secret od .env fajlot

load_dotenv()

#LISTA NA GRADOVI
#za da dodademe nov grad: se kopira edeen red i smenuvame vrednosti
#lat/lon se koordinati i gi ima na : https://www.latlong.net/

CITIES = [
    {"name": "Berlin", "lat": 52.52, "lon":13.405, "bbox":"13.28,52.46,13.54,52.58"},
    {"name": "London", "lat": 51.5074, "lon": -0.1278, "bbox":"-0.246,51.38,0.146,51.68"},
    {"name": "Amsterdam", "lat":52.3676, "lon":4.9041, "bbox":"4.78,52.28,5.08,52.48"}

]

#API KLUCEVI SE CITAAT OD .ENV

OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")
TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY")

#konekcija so POSTGRESQL BAZA

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "localhost"),
    "port": int(os.getenv("POSTGRES_PORT", 5434)),
    "dbname": os.getenv("POSTGRES_DB", "smart_city"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD"),

}