"""Exercice 1 — snapshot météo quotidien (TaskFlow / Airflow 3)."""

from __future__ import annotations

import json
import os

import pendulum

from airflow.sdk import dag, get_current_context, task

from exos_airflow3.assets import WEATHER_SNAPSHOTS_ASSET
from exos_airflow3 import openmeteo

CITY = "Paris"
OUTPUT_DIR = "/opt/airflow/data/weather_snapshots"


@dag(
    dag_id="weather_snapshot",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["weather", "taskflow", "exo1"],
    default_args={"owner": "airflow", "retries": 1},
)
def weather_snapshot():
    @task
    def fetch_coordinates(city: str = CITY) -> dict:
        return openmeteo.geocode_city(city)

    @task
    def fetch_weather(coords: dict) -> dict:
        return openmeteo.hourly_snapshot_metrics(coords, forecast_days=1)

    @task(outlets=[WEATHER_SNAPSHOTS_ASSET])
    def save_snapshot(coords: dict, metrics: dict) -> str:
        ctx = get_current_context()
        logical = ctx.get("logical_date") or ctx.get("data_interval_end")
        if logical is None:
            logical = ctx["dag_run"].start_date
        ds = ctx.get("ds") or logical.strftime("%Y-%m-%d")
        snapshot = {
            "city": coords["city"],
            "country": coords.get("country"),
            "latitude": coords["latitude"],
            "longitude": coords["longitude"],
            "run_date": ds,
            "generated_at": logical.isoformat(),
            "metrics": metrics,
        }
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        safe_city = coords["city"].lower().replace(" ", "_")
        filepath = os.path.join(OUTPUT_DIR, f"{safe_city}_{ds}.json")
        with open(filepath, "w", encoding="utf-8") as f:
            json.dump(snapshot, f, ensure_ascii=False, indent=2)
        return filepath

    coords = fetch_coordinates()
    metrics = fetch_weather(coords)
    save_snapshot(coords, metrics)


weather_snapshot()
