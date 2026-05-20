from airflow.sdk import Asset

WEATHER_SNAPSHOTS_DATASET = Asset("file:///opt/airflow/data/weather_snapshots")
