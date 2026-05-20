from collections import Counter
from datetime import datetime, timedelta
import json
import os

from airflow import DAG
from airflow.operators.python import PythonOperator

from weather_datasets import WEATHER_SNAPSHOTS_DATASET

SNAPSHOTS_DIR = "/opt/airflow/data/weather_snapshots"
REPORTS_DIR = "/opt/airflow/data/weather_reports"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def _risk_level(metrics: dict) -> str:
    temp = metrics["avg_temperature_c"]
    wind = metrics["max_wind_speed_kmh"]
    precip = metrics["total_precipitation_mm"]
    if temp < 0 or temp > 35 or wind > 50 or precip > 30:
        return "high"
    if temp < 5 or temp > 30 or wind > 25 or precip > 10:
        return "medium"
    return "low"


def _load_snapshots() -> list[dict]:
    snapshots = []
    if not os.path.isdir(SNAPSHOTS_DIR):
        return snapshots
    for filename in sorted(os.listdir(SNAPSHOTS_DIR)):
        if not filename.endswith(".json"):
            continue
        filepath = os.path.join(SNAPSHOTS_DIR, filename)
        with open(filepath, encoding="utf-8") as f:
            snapshots.append(json.load(f))
    return snapshots


def aggregate_snapshots(**context):
    snapshots = _load_snapshots()
    if not snapshots:
        raise ValueError(f"Aucun snapshot trouvé dans {SNAPSHOTS_DIR}")

    cities_processed = []
    temperatures = []
    risk_levels = []

    for snapshot in snapshots:
        metrics = snapshot["metrics"]
        temp = metrics["avg_temperature_c"]
        risk = _risk_level(metrics)
        cities_processed.append(
            {
                "city": snapshot["city"],
                "country": snapshot.get("country"),
                "avg_temperature_c": temp,
                "risk_level": risk,
                "run_date": snapshot.get("run_date"),
            }
        )
        temperatures.append((snapshot["city"], temp))
        risk_levels.append(risk)

    hottest_city, hottest_temp = max(temperatures, key=lambda item: item[1])
    coldest_city, coldest_temp = min(temperatures, key=lambda item: item[1])
    global_avg = round(sum(t for _, t in temperatures) / len(temperatures), 2)
    risk_distribution = dict(Counter(risk_levels))

    report = {
        "generated_at": context["logical_date"].isoformat(),
        "triggered_by_dag_run": context["dag_run"].run_id,
        "total_snapshots": len(snapshots),
        "global_avg_temperature_c": global_avg,
        "hottest_city": {"city": hottest_city, "avg_temperature_c": hottest_temp},
        "coldest_city": {"city": coldest_city, "avg_temperature_c": coldest_temp},
        "risk_level_distribution": risk_distribution,
        "cities_processed": cities_processed,
    }

    os.makedirs(REPORTS_DIR, exist_ok=True)
    stamp = context["logical_date"].strftime("%Y%m%dT%H%M%S")
    filepath = os.path.join(REPORTS_DIR, f"aggregation_{stamp}.json")
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(report, f, ensure_ascii=False, indent=2)
    return filepath


with DAG(
    dag_id="weather_snapshot_aggregator",
    default_args=default_args,
    schedule=[WEATHER_SNAPSHOTS_DATASET],
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["weather", "aggregation"],
) as dag:
    aggregate = PythonOperator(
        task_id="aggregate_snapshots",
        python_callable=aggregate_snapshots,
    )
