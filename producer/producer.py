# -*- coding: utf-8 -*-
import requests
import json
import time
from kafka import KafkaProducer
from datetime import datetime
import os
import math

API_KEY = os.environ.get("API_KEY", "")

# Paraglajding lokacije u Srbiji
LOCATIONS = [
    {"name": "Vrsac",     "lat": 45.1167, "lon": 21.3000},
    {"name": "Fruska_Gora", "lat": 45.1333, "lon": 19.7000},
    {"name": "Rtanj",     "lat": 43.7667, "lon": 21.9000},
    {"name": "Zlatibor",  "lat": 43.7333, "lon": 19.7167},
    {"name": "Kopaonik",  "lat": 43.2833, "lon": 20.8000},
]

def calc_dewpoint(temp_c, humidity):
    a, b = 17.27, 237.7
    alpha = ((a * temp_c) / (b + temp_c)) + math.log(humidity / 100.0)
    return round((b * alpha) / (a - alpha), 2)

def get_weather(lat, lon):
    url = f"https://api.openweathermap.org/data/2.5/weather"
    params = {
        "lat": lat,
        "lon": lon,
        "appid": API_KEY,
        "units": "metric"
    }
    response = requests.get(url, params=params)
    return response.json()

def parse_weather(raw, location_name):
    temp = raw["main"]["temp"]
    humidity = raw["main"]["humidity"]
    dewpoint = calc_dewpoint(temp, humidity)
    
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "location": location_name,
        "lat": raw["coord"]["lat"],
        "lon": raw["coord"]["lon"],
        "temp_c": temp,
        "dewpoint_c": round(dewpoint, 2),
        "pressure": raw["main"]["pressure"],
        "humidity": humidity,
        "wind_speed": raw["wind"]["speed"],
        "wind_dir": raw["wind"].get("deg", 0),
        "wind_gust": raw["wind"].get("gust", 0),
        "cloud_cover": raw["clouds"]["all"] / 100.0,  # 0-1 kao u ERA5
        "visibility": raw.get("visibility", 0),
        "weather_desc": raw["weather"][0]["description"],
        "cloud_base_m": ((temp - dewpoint) / 8) * 1000
    }

producer = KafkaProducer(
    bootstrap_servers=["kafka:29092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    key_serializer=lambda k: k.encode("utf-8")
)

print("Producer pokrenut, saljem podatke svakih 60 sekundi...", flush=True)
print("Lokacije:", [l["name"] for l in LOCATIONS], flush=True)

while True:
    for loc in LOCATIONS:
        try:
            raw = get_weather(loc["lat"], loc["lon"])
            data = parse_weather(raw, loc["name"])

            # Racunamo cloud base
            data["cloud_base_m"] = (
                (data["temp_c"] - data["dewpoint_c"]) / 8 * 1000
            )

            producer.send(
                topic="weather-stream",
                key=loc["name"],
                value=data
            )
            print(f"[{data['timestamp']}] {loc['name']}: "
                  f"wind={data['wind_speed']}m/s, "
                  f"temp={data['temp_c']}C, "
                  f"humidity={data['humidity']}%, "
                  f"dewpoint={data['dewpoint_c']}C, "
                  f"cloud_base={data['cloud_base_m']:.0f}m", flush=True)

        except Exception as e:
            print(f"Greska za {loc['name']}: {e}", flush=True)

    producer.flush()
    print(f"--- Batch poslat, cekam 60s ---", flush=True)
    time.sleep(60)