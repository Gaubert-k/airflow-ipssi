"""Exercice 2 — rapports par intervalle hourly / daily (TaskFlow)."""

from __future__ import annotations

import json
import os
from collections import Counter

import pendulum

from airflow.sdk import dag, get_current_context, task

from exos_airflow3 import openmeteo

CITY = "Paris"
OUTPUT_DIR = "/opt/airflow/data/weather_intervals"


def _make_interval_dag(dag_id: str, schedule: str, dag_kind: str):
    @dag(
        dag_id=dag_id,
        schedule=schedule,
        start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
        catchup=False,
        tags=["weather", "intervals", "taskflow", "exo2"],
        default_args={"owner": "airflow", "retries": 1},
    )
    def _dag():
        @task
        def fetch_coordinates(city: str = CITY) -> dict:
            return openmeteo.geocode_city(city)

        @task
        def build_report(coords: dict) -> str:
            ctx = get_current_context()
            interval_start = ctx["data_interval_start"]
            interval_end = ctx["data_interval_end"]
            codes = openmeteo.weather_codes_in_interval(coords, interval_start, interval_end)
            distribution = dict(Counter(codes))
            dominant = max(distribution, key=distribution.get) if distribution else None
            report = {
                "dag": dag_kind,
                "city": coords["city"],
                "country": coords.get("country"),
                "interval": {
                    "start": interval_start.isoformat(),
                    "end": interval_end.isoformat(),
                },
                "total_measurements": len(codes),
                "weather_code_distribution": {str(k): v for k, v in sorted(distribution.items())},
                "dominant_weather_code": dominant,
            }
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            safe_city = coords["city"].lower().replace(" ", "_")
            stamp = interval_start.strftime("%Y%m%dT%H%M%S")
            filepath = os.path.join(OUTPUT_DIR, f"{safe_city}_{dag_kind}_{stamp}.json")
            with open(filepath, "w", encoding="utf-8") as f:
                json.dump(report, f, ensure_ascii=False, indent=2)
            return filepath

        build_report(fetch_coordinates())

    return _dag


weather_hourly = _make_interval_dag("weather_hourly", "@hourly", "hourly")()
weather_daily = _make_interval_dag("weather_daily", "@daily", "daily")()
