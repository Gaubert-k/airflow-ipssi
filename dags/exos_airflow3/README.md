# Exercices météo (Airflow 3 — TaskFlow)

Les DAGs du PDF sont dans ce dossier, au format **TaskFlow** (`airflow.sdk` : `dag`, `task`, `Asset`, `get_current_context`).

| Fichier | DAG | Rôle |
|---------|-----|------|
| `weather_snapshot.py` | `weather_snapshot` | Exo 1 — snapshot JSON + outlet `Asset` |
| `weather_intervals.py` | `weather_hourly`, `weather_daily` | Exo 2 — intervalles + `data_interval_*` |
| `weather_alert_stream.py` | `weather_alert_stream` | Exo 3 — Kafka |
| `weather_snapshot_aggregator.py` | `weather_snapshot_aggregator` | Exo 4 — déclenché par l’asset |

Les données passent entre tâches par **valeurs de retour** des `@task` (pas de `ti.xcom_push` / `pull`).

- `openmeteo.py` : appels HTTP Open-Meteo (sans Airflow).
- `assets.py` : `WEATHER_SNAPSHOTS_ASSET` partagé entre producteur et agrégateur.

Sorties : `data/weather_snapshots/`, `data/weather_intervals/`, `data/weather_reports/` (volume Docker `data/`).
