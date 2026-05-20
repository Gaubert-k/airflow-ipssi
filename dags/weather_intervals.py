from collections import Counter
from datetime import datetime, timedelta
import json
import os
from zoneinfo import ZoneInfo

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

CITY = "Paris"
OUTPUT_DIR = "/opt/airflow/data/weather_intervals"
GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
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
        "country": place.get("country"),
        "latitude": place["latitude"],
        "longitude": place["longitude"],
        "timezone": place.get("timezone", "auto"),
    }
    context["ti"].xcom_push(key="coordinates", value=coords)
    return coords


def _city_tz(timezone_name: str) -> ZoneInfo:
    return ZoneInfo(timezone_name) if timezone_name != "auto" else ZoneInfo("UTC")


def _fetch_hourly_weather_codes(coords, interval_start, interval_end):
    timezone = coords["timezone"]
    tz = _city_tz(timezone)
    local_start = interval_start.astimezone(tz)
    local_end = interval_end.astimezone(tz)
    start_date = local_start.date().isoformat()
    end_date = (local_end - timedelta(seconds=1)).date().isoformat()
    base_params = {
        "latitude": coords["latitude"],
        "longitude": coords["longitude"],
        "hourly": "weather_code",
        "timezone": timezone,
    }

    if interval_end < datetime.now(tz=ZoneInfo("UTC")) - timedelta(days=5):
        response = requests.get(
            ARCHIVE_URL,
            params={**base_params, "start_date": start_date, "end_date": end_date},
            timeout=30,
        )
    else:
        response = requests.get(
            FORECAST_URL,
            params={
                **base_params,
                "start_hour": local_start.strftime("%Y-%m-%dT%H:%M"),
                "end_hour": local_end.strftime("%Y-%m-%dT%H:%M"),
            },
            timeout=30,
        )
    response.raise_for_status()
    return response.json()["hourly"]


def _codes_in_interval(hourly, interval_start, interval_end, timezone_name: str):
    tz = _city_tz(timezone_name)
    start_utc = interval_start.astimezone(ZoneInfo("UTC"))
    end_utc = interval_end.astimezone(ZoneInfo("UTC"))
    codes = []
    for time_str, code in zip(hourly["time"], hourly["weather_code"]):
        if code is None:
            continue
        ts = datetime.fromisoformat(time_str).replace(tzinfo=tz).astimezone(ZoneInfo("UTC"))
        if start_utc <= ts < end_utc:
            codes.append(int(code))
    return codes


def build_interval_report(dag_kind: str, **context):
    coords = context["ti"].xcom_pull(task_ids="get_coordinates", key="coordinates")
    interval_start = context["data_interval_start"]
    interval_end = context["data_interval_end"]

    hourly = _fetch_hourly_weather_codes(coords, interval_start, interval_end)
    codes = _codes_in_interval(hourly, interval_start, interval_end, coords["timezone"])
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


def _make_interval_dag(dag_id: str, schedule: str, dag_kind: str):
    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        schedule=schedule,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=["weather", "intervals"],
    ) as dag:
        get_coordinates = PythonOperator(
            task_id="get_coordinates",
            python_callable=fetch_coordinates,
            op_kwargs={"city_name": CITY},
        )
        build_report = PythonOperator(
            task_id="build_interval_report",
            python_callable=build_interval_report,
            op_kwargs={"dag_kind": dag_kind},
        )
        get_coordinates >> build_report
    return dag


weather_hourly = _make_interval_dag("weather_hourly", "@hourly", "hourly")
weather_daily = _make_interval_dag("weather_daily", "@daily", "daily")
