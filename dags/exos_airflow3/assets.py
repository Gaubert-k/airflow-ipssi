from airflow.sdk import Asset

WEATHER_SNAPSHOTS_ASSET = Asset("file:///opt/airflow/data/weather_snapshots")
