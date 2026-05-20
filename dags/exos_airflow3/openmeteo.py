"""Appels HTTP Open-Meteo (sans dépendance Airflow)."""

from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import requests

GEOCODING_URL = "https://geocoding-api.open-meteo.com/v1/search"
FORECAST_URL = "https://api.open-meteo.com/v1/forecast"
ARCHIVE_URL = "https://archive-api.open-meteo.com/v1/archive"


def geocode_city(city_name: str) -> dict:
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
    return {
        "city": place["name"],
        "country": place.get("country"),
        "latitude": place["latitude"],
        "longitude": place["longitude"],
        "timezone": place.get("timezone", "auto"),
    }


def _city_tz(timezone_name: str) -> ZoneInfo:
    return ZoneInfo(timezone_name) if timezone_name != "auto" else ZoneInfo("UTC")


def hourly_snapshot_metrics(coords: dict, forecast_days: int = 1) -> dict:
    response = requests.get(
        FORECAST_URL,
        params={
            "latitude": coords["latitude"],
            "longitude": coords["longitude"],
            "hourly": "temperature_2m,wind_speed_10m,precipitation",
            "forecast_days": forecast_days,
            "timezone": coords["timezone"],
        },
        timeout=30,
    )
    response.raise_for_status()
    hourly = response.json()["hourly"]
    temps = [t for t in hourly["temperature_2m"] if t is not None]
    winds = [w for w in hourly["wind_speed_10m"] if w is not None]
    precips = [p for p in hourly["precipitation"] if p is not None]
    return {
        "avg_temperature_c": round(sum(temps) / len(temps), 2),
        "max_wind_speed_kmh": round(max(winds), 2),
        "total_precipitation_mm": round(sum(precips), 2),
    }


def _fetch_hourly_weather_codes(coords: dict, interval_start: datetime, interval_end: datetime) -> dict:
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


def weather_codes_in_interval(
    coords: dict,
    interval_start: datetime,
    interval_end: datetime,
) -> list[int]:
    hourly = _fetch_hourly_weather_codes(coords, interval_start, interval_end)
    tz = _city_tz(coords["timezone"])
    start_utc = interval_start.astimezone(ZoneInfo("UTC"))
    end_utc = interval_end.astimezone(ZoneInfo("UTC"))
    codes: list[int] = []
    for time_str, code in zip(hourly["time"], hourly["weather_code"]):
        if code is None:
            continue
        ts = datetime.fromisoformat(time_str).replace(tzinfo=tz).astimezone(ZoneInfo("UTC"))
        if start_utc <= ts < end_utc:
            codes.append(int(code))
    return codes


def fetch_current_weather(coords: dict) -> dict:
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
    return {
        "timestamp": current["time"],
        "temperature": current["temperature_2m"],
        "wind_speed": current["wind_speed_10m"],
        "weather_code": current["weather_code"],
    }


def alert_types(temperature: float, wind_speed: float) -> list[str]:
    alerts: list[str] = []
    if temperature < 5 and wind_speed > 20:
        alerts.append("cold_alert")
    if temperature > 35:
        alerts.append("hot_alert")
    return alerts
