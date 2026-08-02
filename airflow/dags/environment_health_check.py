from __future__ import annotations

from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


def check_environment() -> None:
    """Vérifie que l'environnement Airflow fonctionne correctement."""
    print("Airflow fonctionne correctement.")
    print("Le dossier DAGs est bien monté dans le conteneur.")
    print("Validation de l'environnement Data Lakehouse réussie.")


with DAG(
    dag_id="environment_health_check",
    description="Validation de l'environnement Apache Airflow",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["setup", "health-check", "week-1"],
) as dag:

    verify_airflow_environment = PythonOperator(
        task_id="verify_airflow_environment",
        python_callable=check_environment,
    )
