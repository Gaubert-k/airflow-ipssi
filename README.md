# Apache Airflow (Docker Compose)

Stack locale **Airflow 3.2.1** avec **CeleryExecutor**, PostgreSQL et Redis, basée sur la [documentation officielle](https://airflow.apache.org/docs/apache-airflow/3.2.1/howto/docker-compose/index.html).

> Usage pédagogique uniquement — ne pas déployer en production tel quel.

## Prérequis

- Docker Engine + Docker Compose v2.14+
- Au moins **4 Go de RAM** alloués à Docker

## Structure

| Dossier   | Rôle                                      |
|-----------|-------------------------------------------|
| `dags/`   | Fichiers de workflows (DAGs)              |
| `logs/`   | Journaux d'exécution                      |
| `plugins/`| Extensions personnalisées               |
| `config/` | `airflow.cfg` et réglages locaux          |

## Démarrage rapide

```bash
# 1. Variables d'environnement (déjà fait si .env présent)
echo -e "AIRFLOW_UID=$(id -u)" >> .env   # ou éditer .env manuellement

# 2. (Optionnel) Pré-générer airflow.cfg
docker compose run --rm airflow-cli airflow config list

# 3. Initialiser la base et l'utilisateur admin
docker compose up airflow-init

# 4. Lancer le cluster
docker compose up -d

# 5. Interface web
# http://localhost:8080
# identifiants par défaut : airflow / airflow
```

## Services

| Service              | Port  | Description                    |
|----------------------|-------|--------------------------------|
| `airflow-apiserver`  | 8080  | Interface web + API            |
| `postgres`           | —     | Métadonnées Airflow            |
| `redis`              | —     | Broker Celery                  |
| `airflow-scheduler`  | —     | Planification des tâches       |
| `airflow-worker`     | —     | Exécution des tâches           |
| `airflow-dag-processor` | —  | Parsing des DAGs               |
| `airflow-triggerer`  | —     | Tâches différées (deferrable)   |

### Flower (monitoring Celery, optionnel)

```bash
docker compose --profile flower up -d
# http://localhost:5555
```

## Commandes utiles

```bash
# État des conteneurs
docker compose ps

# Logs
docker compose logs -f airflow-scheduler

# CLI Airflow
docker compose run --rm airflow-cli airflow dags list

# Arrêt et nettoyage complet (données incluses)
docker compose down --volumes --remove-orphans
```

## Mise à jour de la stack

Pour passer à une version plus récente d'Airflow :

```bash
curl -LfO 'https://airflow.apache.org/docs/apache-airflow/<VERSION>/docker-compose.yaml'
# Adapter AIRFLOW_IMAGE_NAME dans .env
docker compose down --volumes
docker compose up airflow-init
docker compose up -d
```

## Références

- [Running Airflow in Docker](https://airflow.apache.org/docs/apache-airflow/3.2.1/howto/docker-compose/index.html)
- [docker-compose.yaml officiel](https://airflow.apache.org/docs/apache-airflow/3.2.1/docker-compose.yaml)
