"""Exercice 3 — alertes météo vers Kafka (TaskFlow)."""

from __future__ import annotations

import json
import os
from datetime import timedelta

import pendulum
from kafka import KafkaProducer

from airflow.sdk import dag, task, task_group

from exos_airflow3 import openmeteo

CITY = "Paris"
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_WEATHER_ALERTS_TOPIC", "weather_alerts")


@dag(
    dag_id="weather_alert_stream",
    schedule=timedelta(minutes=1),
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["weather", "kafka", "taskflow", "exo3"],
    default_args={"owner": "airflow", "retries": 1},
)
def weather_alert_stream():
    @task_group(group_id="exo3")
    def exo3():
        @task(task_id="get_coordinates")
        def get_coordinates(city: str = CITY) -> dict:
            return openmeteo.geocode_city(city)

        @task(task_id="fetch_current_weather")
        def fetch_current_weather(coords: dict) -> dict:
            return openmeteo.fetch_current_weather(coords)

        @task(task_id="publish_weather_alerts")
        def publish_weather_alerts(coords: dict, weather: dict) -> list[dict]:
            alerts = openmeteo.alert_types(weather["temperature"], weather["wind_speed"])
            if not alerts:
                return []
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP.split(","),
                value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            )
            published: list[dict] = []
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

        coords = get_coordinates()
        current = fetch_current_weather(coords)
        publish_weather_alerts(coords, current)

    exo3()


weather_alert_stream()
