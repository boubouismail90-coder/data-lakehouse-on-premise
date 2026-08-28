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
POSTGRES_PACKAGE = "org.postgresql:postgresql:42.7.3"

SPARK_PACKAGES = ",".join(
    [
        DELTA_PACKAGE,
        HADOOP_AWS_PACKAGE,
        POSTGRES_PACKAGE,
    ]
)


# ============================================================
# JOBS SPARK
# ============================================================

BRONZE_TO_SILVER_JOB = (
    "/opt/spark/jobs/"
    "bronze_to_silver_rustfs_delta.py"
)

SILVER_TO_GOLD_JOB = (
    "/opt/spark/jobs/"
    "silver_to_gold_rustfs_delta.py"
)

GOLD_TO_BI_JOB = (
    "/opt/spark/jobs/"
    "gold_to_bi.py"
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
        "Transformation automatisée Bronze -> Silver -> Gold -> BI "
        "avec PySpark, Delta Lake, RustFS et PostgreSQL"
    ),

    default_args=default_args,

    # Exécution manuelle pour le moment.
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
        "bi",
        "delta",
        "rustfs",
        "postgresql",
    ],
) as dag:

    # ========================================================
    # TASK 1 : BRONZE -> SILVER
    # ========================================================

    bronze_to_silver = BashOperator(
        task_id="bronze_to_silver_delta",

        bash_command=f"""
set -e

echo "======================================================"
echo "TASK 1 - BRONZE -> SILVER DELTA"
echo "======================================================"

echo "Spark Master : {SPARK_MASTER}"
echo "Spark Job    : {BRONZE_TO_SILVER_JOB}"

mkdir -p /tmp/.ivy2

spark-submit \
    --master {SPARK_MASTER} \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --packages {SPARK_PACKAGES} \
    {BRONZE_TO_SILVER_JOB}

echo "======================================================"
echo "BRONZE -> SILVER TERMINE"
echo "======================================================"
""",
    )


    # ========================================================
    # TASK 2 : SILVER -> GOLD
    # ========================================================

    silver_to_gold = BashOperator(
        task_id="silver_to_gold_delta",

        bash_command=f"""
set -e

echo "======================================================"
echo "TASK 2 - SILVER -> GOLD DELTA"
echo "======================================================"

echo "Spark Master : {SPARK_MASTER}"
echo "Spark Job    : {SILVER_TO_GOLD_JOB}"

mkdir -p /tmp/.ivy2

spark-submit \
    --master {SPARK_MASTER} \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --packages {SPARK_PACKAGES} \
    {SILVER_TO_GOLD_JOB}

echo "======================================================"
echo "SILVER -> GOLD TERMINE"
echo "======================================================"
""",
    )


    # ========================================================
    # TASK 3 : GOLD -> BI
    # ========================================================

    gold_to_bi = BashOperator(
        task_id="gold_to_bi",

        bash_command=f"""
set -e

echo "======================================================"
echo "TASK 3 - GOLD -> BI"
echo "======================================================"

echo "Verification de l'environnement..."
echo "PATH : $PATH"

# --------------------------------------------------------
# Vérification de Spark
# --------------------------------------------------------

if ! command -v spark-submit >/dev/null 2>&1; then
    echo "ERREUR : spark-submit est introuvable dans PATH"
    exit 1
fi

echo "spark-submit : $(command -v spark-submit)"

echo "Spark version :"
spark-submit --version 2>&1 | head -20 || true


# --------------------------------------------------------
# Vérification des variables BI
# --------------------------------------------------------

echo "Verification des variables BI..."

if [ -z "${{BI_DB_HOST:-}}" ]; then
    echo "ERREUR : BI_DB_HOST n'est pas defini"
    exit 1
fi

if [ -z "${{BI_DB_PORT:-}}" ]; then
    echo "ERREUR : BI_DB_PORT n'est pas defini"
    exit 1
fi

if [ -z "${{BI_DB_NAME:-}}" ]; then
    echo "ERREUR : BI_DB_NAME n'est pas defini"
    exit 1
fi

if [ -z "${{BI_DB_USER:-}}" ]; then
    echo "ERREUR : BI_DB_USER n'est pas defini"
    exit 1
fi

if [ -z "${{BI_DB_PASSWORD:-}}" ]; then
    echo "ERREUR : BI_DB_PASSWORD n'est pas defini"
    exit 1
fi

echo "BI_DB_HOST : ${{BI_DB_HOST}}"
echo "BI_DB_PORT : ${{BI_DB_PORT}}"
echo "BI_DB_NAME : ${{BI_DB_NAME}}"
echo "BI_DB_USER : ${{BI_DB_USER}}"
echo "BI_DB_PASSWORD : ********"


# --------------------------------------------------------
# Vérification réseau PostgreSQL
# --------------------------------------------------------

echo "Verification de PostgreSQL..."

if command -v getent >/dev/null 2>&1; then
    getent hosts "${{BI_DB_HOST}}" || true
fi


# --------------------------------------------------------
# Lancement du job Gold -> BI
# --------------------------------------------------------

echo "Lancement du job Gold -> BI..."

mkdir -p /tmp/.ivy2

spark-submit \
    --master {SPARK_MASTER} \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension \
    --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog \
    --packages {SPARK_PACKAGES} \
    {GOLD_TO_BI_JOB}

echo "======================================================"
echo "GOLD -> BI TERMINE"
echo "======================================================"
""",

        # ----------------------------------------------------
        # VARIABLES D'ENVIRONNEMENT BI
        # ----------------------------------------------------
        #
        # IMPORTANT :
        # PostgreSQL utilise actuellement Ismail123
        # pour lakehouse_admin.
        #
        # Le test depuis airflow-scheduler a confirmé :
        #
        # Ismail123  -> OK
        # Ismail1123 -> password authentication failed
        #
        # ----------------------------------------------------

        env={
            "BI_DB_HOST": "postgres",
            "BI_DB_PORT": "5432",
            "BI_DB_NAME": "bi_db",
            "BI_DB_USER": "lakehouse_admin",
            "BI_DB_PASSWORD": "Ismail1123",
        },

        # Conserve les autres variables déjà présentes
        # dans l'environnement du conteneur.
        append_env=True,
    )


    # ========================================================
    # DEPENDENCIES
    # ========================================================

    bronze_to_silver >> silver_to_gold >> gold_to_bi