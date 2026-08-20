from datetime import timedelta

import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


# ============================================================
# CONFIGURATION
# ============================================================

SPARK_MASTER = "spark://spark:7077"

DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.2.0"

HADOOP_AWS_PACKAGE = "org.apache.hadoop:hadoop-aws:3.3.4"

SPARK_PACKAGES = (
    f"{DELTA_PACKAGE},"
    f"{HADOOP_AWS_PACKAGE}"
)

BRONZE_TO_SILVER_JOB = (
    "/opt/spark/jobs/"
    "bronze_to_silver_rustfs_delta.py"
)

SILVER_TO_GOLD_JOB = (
    "/opt/spark/jobs/"
    "silver_to_gold_rustfs_delta.py"
)


# ============================================================
# ARGUMENTS PAR DEFAUT
# ============================================================

default_args = {
    "owner": "data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}


# ============================================================
# DAG AIRFLOW #2
# ============================================================

with DAG(
    dag_id="silver_gold_transformation",
    description=(
        "Transformation automatisée Bronze -> Silver -> Gold "
        "avec PySpark, Delta Lake et RustFS"
    ),
    default_args=default_args,

    # Pour le premier test, on ne planifie pas encore
    # automatiquement le DAG.
    schedule=None,

    start_date=pendulum.datetime(
        2026,
        8,
        1,
        tz="UTC",
    ),

    catchup=False,

    tags=[
        "lakehouse",
        "pyspark",
        "silver",
        "gold",
        "delta",
        "rustfs",
    ],
) as dag:

    # ========================================================
    # TASK 1 : BRONZE -> SILVER
    # ========================================================

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver_delta",
        bash_command=f"""
            set -e

            echo "========================================"
            echo "TASK 1 - BRONZE -> SILVER DELTA"
            echo "========================================"

            mkdir -p /tmp/.ivy2

            spark-submit \
              --master {SPARK_MASTER} \
              --conf spark.jars.ivy=/tmp/.ivy2 \
              --packages {SPARK_PACKAGES} \
              {BRONZE_TO_SILVER_JOB}

            echo "========================================"
            echo "BRONZE -> SILVER TERMINE"
            echo "========================================"
        """,
    )


    # ========================================================
    # TASK 2 : SILVER -> GOLD
    # ========================================================

    silver_to_gold = BashOperator(
        task_id="silver_to_gold_delta",
        bash_command=f"""
            set -e

            echo "========================================"
            echo "TASK 2 - SILVER -> GOLD DELTA"
            echo "========================================"

            mkdir -p /tmp/.ivy2

            spark-submit \
              --master {SPARK_MASTER} \
              --conf spark.jars.ivy=/tmp/.ivy2 \
              --packages {SPARK_PACKAGES} \
              {SILVER_TO_GOLD_JOB}

            echo "========================================"
            echo "SILVER -> GOLD TERMINE"
            echo "========================================"
        """,
    )


    # ========================================================
    # DEPENDANCE
    # ========================================================

    bronze_to_silver >> silver_to_gold