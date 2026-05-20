from datetime import datetime, timedelta
import json
import os

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer

CITY = "Paris"
GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_WEATHER_ALERTS_TOPIC", "weather_alerts")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


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
        "latitude": place["latitude"],
        "longitude": place["longitude"],
        "timezone": place.get("timezone", "auto"),
    }
    context["ti"].xcom_push(key="coordinates", value=coords)
    return coords


def fetch_current_weather(**context):
    coords = context["ti"].xcom_pull(task_ids="get_coordinates", key="coordinates")
    response = requests.get(
        FORECAST_URL,
        params={
            "latitude": coords["latitude"],
            "longitude": coords["longitude"],
            "current": "temperature_2m,wind_speed_10m,weather_code",
            "timezone": coords["timezone"],
        },
        timeout=30,
    )
    response.raise_for_status()
    current = response.json()["current"]
    weather = {
        "timestamp": current["time"],
        "temperature": current["temperature_2m"],
        "wind_speed": current["wind_speed_10m"],
        "weather_code": current["weather_code"],
    }
    context["ti"].xcom_push(key="current_weather", value=weather)
    return weather


def _alert_types(temperature: float, wind_speed: float) -> list[str]:
    alerts = []
    if temperature < 5 and wind_speed > 20:
        alerts.append("cold_alert")
    if temperature > 35:
        alerts.append("hot_alert")
    return alerts


def publish_weather_alerts(**context):
    coords = context["ti"].xcom_pull(task_ids="get_coordinates", key="coordinates")
    weather = context["ti"].xcom_pull(task_ids="fetch_current_weather", key="current_weather")
    alerts = _alert_types(weather["temperature"], weather["wind_speed"])
    if not alerts:
        return []

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    )
    published = []
    try:
        for alert_type in alerts:
            message = {
                "city": coords["city"],
                "timestamp": weather["timestamp"],
                "temperature": weather["temperature"],
                "wind_speed": weather["wind_speed"],
                "weather_code": weather["weather_code"],
                "alert_type": alert_type,
            }
            producer.send(KAFKA_TOPIC, value=message)
            published.append(message)
        producer.flush()
    finally:
        producer.close()
    return published


with DAG(
    dag_id="weather_alert_stream",
    default_args=default_args,
    schedule=timedelta(minutes=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["weather", "kafka"],
) as dag:
    get_coordinates = PythonOperator(
        task_id="get_coordinates",
        python_callable=fetch_coordinates,
        op_kwargs={"city_name": CITY},
    )
    fetch_current_weather = PythonOperator(
        task_id="fetch_current_weather",
        python_callable=fetch_current_weather,
    )
    publish_alerts = PythonOperator(
        task_id="publish_weather_alerts",
        python_callable=publish_weather_alerts,
    )
    get_coordinates >> fetch_current_weather >> publish_alertsge
