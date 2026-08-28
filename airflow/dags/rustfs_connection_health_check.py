from __future__ import annotations

import os
from datetime import datetime

import boto3
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError


REQUIRED_BUCKETS = {"bronze", "silver", "gold"}


def verify_rustfs_connection() -> None:
    """Vérifie la connexion S3 entre Airflow et RustFS."""

    endpoint_url = os.environ.get("RUSTFS_ENDPOINT_URL")
    access_key = os.environ.get("RUSTFS_ACCESS_KEY")
    secret_key = os.environ.get("RUSTFS_SECRET_KEY")
    region = os.environ.get("RUSTFS_REGION", "us-east-1")

    missing_variables = [
        variable_name
        for variable_name, variable_value in {
            "RUSTFS_ENDPOINT_URL": endpoint_url,
            "RUSTFS_ACCESS_KEY": access_key,
            "RUSTFS_SECRET_KEY": secret_key,
        }.items()
        if not variable_value
    ]

    if missing_variables:
        raise RuntimeError(
            "Variables RustFS manquantes : "
            + ", ".join(missing_variables)
        )

    s3_client = boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(
            signature_version="s3v4",
            s3={"addressing_style": "path"},
            connect_timeout=10,
            read_timeout=30,
            retries={
                "max_attempts": 3,
                "mode": "standard",
            },
        ),
    )

    try:
        response = s3_client.list_buckets()
    except (BotoCoreError, ClientError) as error:
        raise RuntimeError(
            f"Connexion à RustFS impossible : {error}"
        ) from error

    bucket_names = {
        bucket["Name"]
        for bucket in response.get("Buckets", [])
    }

    print(f"Compartiments détectés : {sorted(bucket_names)}")

    missing_buckets = REQUIRED_BUCKETS.difference(bucket_names)

    if missing_buckets:
        raise RuntimeError(
            "Compartiments obligatoires manquants : "
            + ", ".join(sorted(missing_buckets))
        )

    bronze_response = s3_client.list_objects_v2(
        Bucket="bronze",
        MaxKeys=20,
    )

    bronze_objects = [
        item["Key"]
        for item in bronze_response.get("Contents", [])
    ]

    print(f"Objets présents dans bronze : {bronze_objects}")
    print("Connexion Airflow vers RustFS validée.")
    print("Les compartiments bronze, silver et gold sont disponibles.")


with DAG(
    dag_id="rustfs_connection_health_check",
    description="Validation de la connexion entre Airflow et RustFS",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["setup", "rustfs", "s3", "week-1"],
) as dag:

    verify_rustfs = PythonOperator(
        task_id="verify_rustfs_connection",
        python_callable=verify_rustfs_connection,
    )
