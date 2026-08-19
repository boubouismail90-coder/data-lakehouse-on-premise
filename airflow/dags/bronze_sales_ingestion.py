import csv
import logging
import os
import tempfile
from datetime import datetime, timedelta

import boto3
import psycopg2
from botocore.config import Config

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator


logger = logging.getLogger(__name__)


def ingest_sales_to_bronze():
    logger.info("Début de l'ingestion des ventes vers Bronze.")

    connection = None
    cursor = None
    temp_file_path = None

    try:
        # Connexion à PostgreSQL source_db
        connection = psycopg2.connect(
            host=os.environ["SOURCE_DB_HOST"],
            port=os.environ["SOURCE_DB_PORT"],
            dbname=os.environ["SOURCE_DB_NAME"],
            user=os.environ["SOURCE_DB_USER"],
            password=os.environ["SOURCE_DB_PASSWORD"],
        )

        cursor = connection.cursor()

        logger.info("Connexion PostgreSQL réussie.")

        # Extraction des données
        cursor.execute(
            """
            SELECT
                id,
                sale_date,
                product_id,
                product_name,
                customer_id,
                region,
                quantity,
                unit_price,
                created_at
            FROM sales
            ORDER BY id;
            """
        )

        rows = cursor.fetchall()

        logger.info(
            "Nombre de lignes extraites : %s",
            len(rows),
        )

        if not rows:
            raise ValueError(
                "La table sales ne contient aucune donnée."
            )

        column_names = [
            description[0]
            for description in cursor.description
        ]

        # Création d'un fichier CSV temporaire
        execution_time = datetime.utcnow()

        object_key = (
            "sales/"
            f"ingestion_date={execution_time:%Y-%m-%d}/"
            f"sales_{execution_time:%Y%m%d_%H%M%S}.csv"
        )

        with tempfile.NamedTemporaryFile(
            mode="w",
            newline="",
            suffix=".csv",
            delete=False,
            encoding="utf-8",
        ) as temp_file:

            writer = csv.writer(temp_file)

            writer.writerow(column_names)
            writer.writerows(rows)

            temp_file_path = temp_file.name

        file_size = os.path.getsize(
            temp_file_path
        )

        logger.info(
            "Fichier CSV généré."
        )

        logger.info(
            "Taille du fichier : %s octets",
            file_size,
        )

        # Connexion RustFS via API S3
        s3_client = boto3.client(
            "s3",
            endpoint_url=os.environ[
                "RUSTFS_ENDPOINT_URL"
            ],
            aws_access_key_id=os.environ[
                "RUSTFS_ACCESS_KEY"
            ],
            aws_secret_access_key=os.environ[
                "RUSTFS_SECRET_KEY"
            ],
            region_name=os.environ.get(
                "RUSTFS_REGION",
                "us-east-1",
            ),
            config=Config(
                signature_version="s3v4",
                s3={
                    "addressing_style": "path"
                },
            ),
        )

        logger.info(
            "Connexion RustFS réussie."
        )

        # Upload dans Bronze
        s3_client.upload_file(
            temp_file_path,
            "bronze",
            object_key,
        )

        logger.info(
            "Upload réussi : "
            "s3://bronze/%s",
            object_key,
        )

        logger.info(
            "Ingestion Bronze terminée avec succès."
        )

    except Exception:
        logger.exception(
            "Erreur pendant l'ingestion Bronze."
        )
        raise

    finally:
        if cursor:
            cursor.close()

        if connection:
            connection.close()

        if (
            temp_file_path
            and os.path.exists(temp_file_path)
        ):
            os.remove(temp_file_path)

            logger.info(
                "Fichier temporaire supprimé."
            )


default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="bronze_sales_ingestion",
    description=(
        "Extraction PostgreSQL vers "
        "RustFS Bronze."
    ),
    start_date=datetime(2026, 8, 1),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=[
        "bronze",
        "postgresql",
        "rustfs",
        "ingestion",
        "week-2",
    ],
) as dag:

    extract_and_load = PythonOperator(
        task_id=(
            "extract_and_load_sales_to_bronze"
        ),
        python_callable=ingest_sales_to_bronze,
    )