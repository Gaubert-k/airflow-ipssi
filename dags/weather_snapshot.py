from datetime import datetime, timedelta
import json
import os

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

from weather_datasets import WEATHER_SNAPSHOTS_DATASET

CITY = "Paris"
OUTPUT_DIR = "/opt/airflow/data/weather_snapshots"
GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"


def fetch_coordinates(city_name: str, **context):
    response = requests.get(
        GEOCODING_URL,
        params={"name": city_name, "count": 1, "language": "fr", "format": "json"},
        timeout=30,
    )
    response.raise_for_status()
    results = response.json().get("results")
    if not results:
        raise ValueError(f"Ville introuvable: {city_name}")
    place = results[0]
    coords = {
        "city": place["name"],
        "country": place.get("country"),
        "latitude": place["latitude"],
        "longitude": place["longitude"],
        "timezone": place.get("timezone", "auto"),
    }
    context["ti"].xcom_push(key="coordinates", value=coords)
    return coords


def fetch_weather(**context):
    coords = context["ti"].xcom_pull(task_ids="get_coordinates", key="coordinates")
    response = requests.get(
        FORECAST_URL,
        params={
            "latitude": coords["latitude"],
            "longitude": coords["longitude"],
            "hourly": "temperature_2m,wind_speed_10m,precipitation",
            "forecast_days": 1,
            "timezone": coords["timezone"],
        },
        timeout=30,
    )
    response.raise_for_status()
    hourly = response.json()["hourly"]
    temps = [t for t in hourly["temperature_2m"] if t is not None]
    winds = [w for w in hourly["wind_speed_10m"] if w is not None]
    precips = [p for p in hourly["precipitation"] if p is not None]
    metrics = {
        "avg_temperature_c": round(sum(temps) / len(temps), 2),
        "max_wind_speed_kmh": round(max(winds), 2),
        "total_precipitation_mm": round(sum(precips), 2),
    }
    context["ti"].xcom_push(key="metrics", value=metrics)
    return metrics


def build_and_save_snapshot(**context):
    coords = context["ti"].xcom_pull(task_ids="get_coordinates", key="coordinates")
    metrics = context["ti"].xcom_pull(task_ids="get_weather", key="metrics")
    snapshot = {
        "city": coords["city"],
        "country": coords.get("country"),
        "latitude": coords["latitude"],
        "longitude": coords["longitude"],
        "run_date": context["ds"],
        "generated_at": context["logical_date"].isoformat(),
        "metrics": metrics,
    }
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    safe_city = coords["city"].lower().replace(" ", "_")
    filepath = os.path.join(OUTPUT_DIR, f"{safe_city}_{context['ds']}.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(snapshot, f, ensure_ascii=False, indent=2)
    return filepath


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="weather_snapshot",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["weather"],
) as dag:
    get_coordinates = PythonOperator(
        task_id="get_coordinates",
        python_callable=fetch_coordinates,
        op_kwargs={"city_name": CITY},
    )
    get_weather = PythonOperator(
        task_id="get_weather",
        python_callable=fetch_weather,
    )
    save_snapshot = PythonOperator(
        task_id="save_snapshot",
        python_callable=build_and_save_snapshot,
        outlets=[WEATHER_SNAPSHOTS_DATASET],
    )
    get_coordinates >> get_weather >> save_snapshot
