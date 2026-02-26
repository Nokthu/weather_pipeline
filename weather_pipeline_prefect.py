"""
Pipeline didáctico:
Open-Meteo (API clima) -> pandas (staging) -> transform -> MongoDB

Open-Meteo:
- Forecast docs: https://open-meteo.com/en/docs
- Geocoding docs: https://open-meteo.com/en/docs/geocoding-api
"""

import os
from datetime import datetime, timezone
import requests
import pandas as pd
from pymongo import MongoClient, ASCENDING
from dotenv import load_dotenv
from prefect import flow

# -----------------------------------
# CONFIG
# -----------------------------------
load_dotenv()
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME", "weather_demo")
COLLECTION_NAME = os.getenv("COLLECTION_NAME", "hourly_weather")

GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"

# -----------------------------------
# 1) EXTRACT - Geocoding
# -----------------------------------
def get_lat_lon(city: str, country_code: str = "CR") -> tuple[float, float, str]:
    """
    Convierte un nombre de ciudad a (lat, lon) usando el Geocoding API de Open-Meteo.
    Devuelve: lat, lon, nombre_resuelto
    """
    params = {
        "name": city,
        "count": 1,
        "language": "es",
        "format": "json",
        "country_code": country_code,
    }
    
    r = requests.get(GEOCODING_URL, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    
    if "results" not in data or len(data["results"]) == 0:
        raise ValueError(f"No se encontraron resultados para city={city}, country_code={country_code}")

    top = data["results"][0]
    return float(top["latitude"]), float(top["longitude"]), top["name"]

# -----------------------------------
# 2) EXTRACT - Weather Forecast
# -----------------------------------
def fetch_hourly_weather(lat: float, lon: float, timezone_name: str = "America/Costa_Rica") -> dict:
    """
    Extrae datos horarios (ej: temp, precipitación, viento) desde Open-Meteo.
    """
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,precipitation,windspeed_10m",
        "timezone": timezone_name,
    }
    
    r = requests.get(FORECAST_URL, params=params, timeout=30)
    r.raise_for_status()
    return r.json()

# -----------------------------------
# 3) STAGE - dict/json -> DataFrame temporal
# -----------------------------------
def to_staging_df(weather_json: dict, location_name: str) -> pd.DataFrame:
    """
    Convierte la respuesta JSON a un DataFrame 'staging' (temporal).
    No aplica reglas de negocio todavía; solo estructura.
    """
    hourly = weather_json.get("hourly", {})
    df = pd.DataFrame({
        "time": hourly.get("time", []),
        "temperature_2m": hourly.get("temperature_2m", []),
        "precipitation": hourly.get("precipitation", []),
        "windspeed_10m": hourly.get("windspeed_10m", []),
    })

    # Metadatos de contexto (útiles en Mongo)
    df["location_name"] = location_name
    df["latitude"] = weather_json.get("latitude")
    df["longitude"] = weather_json.get("longitude")

    # Timestamp de ingesta (para auditoría)
    df["ingested_at_utc"] = datetime.now(timezone.utc).isoformat()

    return df

# -----------------------------------
# 4) TRANSFORM - limpieza + tipos + features
# -----------------------------------
def transform(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transformación típica:
    - parsear time a datetime
    - asegurar numéricos
    - crear columnas derivadas
    - eliminar nulos críticos
    """
    out = df.copy()

    # Convertir time a datetime (muy importante para análisis)
    out["time"] = pd.to_datetime(out["time"], errors="coerce")

    # Tipos numéricos
    for col in ["temperature_2m", "precipitation", "windspeed_10m"]:
        out[col] = pd.to_numeric(out[col], errors="coerce")

    # Reglas mínimas de calidad:
    # si no hay tiempo o temperatura, el registro no sirve
    out = out.dropna(subset=["time", "temperature_2m"])

    # Feature derivada didáctica: bandera de "lluvia"
    out["is_rain"] = out["precipitation"].fillna(0) > 0

    # Feature derivada: fecha (para particionar o agrupar)
    out["date"] = out["time"].dt.date.astype(str)

    return out

# -----------------------------------
# 5) LOAD - DataFrame -> MongoDB
# -----------------------------------
def load_to_mongo(df: pd.DataFrame) -> int:
    """
    Inserta en MongoDB como documentos.
    Recomendación didáctica: usar un índice único para evitar duplicados por (location_name, time)
    """
    if df.empty:
        return 0

    client = MongoClient(MONGO_URI)
    col = client[DB_NAME][COLLECTION_NAME]

    # Índice para evitar duplicados
    col.create_index([("location_name", ASCENDING), ("time", ASCENDING)], unique=True)

    # Mongo acepta datetime nativo; convertimos a python datetime
    records = df.to_dict(orient="records")

    inserted = 0
    for doc in records:
        # upsert: si ya existe (misma location+time), lo actualiza; si no, lo crea
        col.update_one(
            {"location_name": doc["location_name"], "time": doc["time"]},
            {"$set": doc},
            upsert=True
        )
        inserted += 1

    client.close()
    return inserted

# -----------------------------------
# RUN PIPELINE
# -----------------------------------
@flow(name="weather_pipeline_prefect")
def run(city: str = "San José", country_code: str = "CR"):
    # 1) geocoding
    lat, lon, resolved_name = get_lat_lon(city=city, country_code=country_code)

    # 2) extract weather
    weather_json = fetch_hourly_weather(lat, lon)

    # 3) staging df
    staging_df = to_staging_df(weather_json, location_name=resolved_name)
    
    # Guardar staging en archivo JSON en carpeta temporal con fecha y hora
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    temp_dir = f"temporal/{timestamp}"
    os.makedirs(temp_dir, exist_ok=True)
    staging_df.to_json(f"{temp_dir}/staging_data.json", orient="records")
    
    print("STAGING DF (head):")
    print(staging_df.head())

    # 4) transform
    final_df = transform(staging_df)
    print("\nTRANSFORMED DF (head):")
    print(final_df.head())

    # 5) load
    n = load_to_mongo(final_df)
    print(f"\nListo. Registros upserted (insert/update): {n}")
    print(f"Destino: {DB_NAME}.{COLLECTION_NAME}")

if __name__ == "__main__":
    run()
