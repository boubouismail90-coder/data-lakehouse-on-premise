import os

from pyspark.sql import SparkSession


# ============================================================
# CONFIGURATION
# ============================================================

GOLD_BASE_PATH = "s3a://gold"

TABLES = [
    "dim_date",
    "dim_product",
    "dim_customer",
    "dim_region",
    "fact_sales",
]


# ============================================================
# VARIABLES BI / POSTGRESQL
# ============================================================

BI_DB_HOST = os.getenv("BI_DB_HOST")
BI_DB_PORT = os.getenv("BI_DB_PORT", "5432")
BI_DB_NAME = os.getenv("BI_DB_NAME")
BI_DB_USER = os.getenv("BI_DB_USER")
BI_DB_PASSWORD = os.getenv("BI_DB_PASSWORD")


# ============================================================
# VALIDATION ENVIRONNEMENT
# ============================================================

def validate_environment():
    """
    Vérifie que toutes les variables PostgreSQL BI
    sont disponibles.
    """

    required = {
        "BI_DB_HOST": BI_DB_HOST,
        "BI_DB_PORT": BI_DB_PORT,
        "BI_DB_NAME": BI_DB_NAME,
        "BI_DB_USER": BI_DB_USER,
        "BI_DB_PASSWORD": BI_DB_PASSWORD,
    }

    missing = [
        name
        for name, value in required.items()
        if not value
    ]

    if missing:
        raise RuntimeError(
            "Variables BI manquantes : "
            + ", ".join(missing)
        )


# ============================================================
# SESSION SPARK
# ============================================================

def create_spark_session():
    """
    Crée la session Spark avec support Delta Lake.
    """

    return (
        SparkSession.builder
        .appName("Gold-To-BI")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config(
            "spark.delta.logStore.class",
            "io.delta.storage.S3SingleDriverLogStore",
        )
        .getOrCreate()
    )


# ============================================================
# CONFIGURATION S3A / RUSTFS
# ============================================================

def configure_s3a(spark):
    """
    Configure Spark/Hadoop pour lire les tables Gold
    depuis RustFS.
    """

    endpoint_url = os.environ.get(
        "RUSTFS_ENDPOINT_URL"
    )

    access_key = os.environ.get(
        "RUSTFS_ACCESS_KEY"
    )

    secret_key = os.environ.get(
        "RUSTFS_SECRET_KEY"
    )

    region = os.environ.get(
        "RUSTFS_REGION",
        "us-east-1",
    )

    missing_variables = []

    if not endpoint_url:
        missing_variables.append(
            "RUSTFS_ENDPOINT_URL"
        )

    if not access_key:
        missing_variables.append(
            "RUSTFS_ACCESS_KEY"
        )

    if not secret_key:
        missing_variables.append(
            "RUSTFS_SECRET_KEY"
        )

    if missing_variables:
        raise RuntimeError(
            "Variables RustFS manquantes : "
            + ", ".join(missing_variables)
        )

    hadoop_conf = (
        spark
        .sparkContext
        ._jsc
        .hadoopConfiguration()
    )

    hadoop_conf.set(
        "fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem",
    )

    hadoop_conf.set(
        "fs.s3a.endpoint",
        endpoint_url,
    )

    hadoop_conf.set(
        "fs.s3a.endpoint.region",
        region,
    )

    hadoop_conf.set(
        "fs.s3a.path.style.access",
        "true",
    )

    hadoop_conf.set(
        "fs.s3a.connection.ssl.enabled",
        "false",
    )

    hadoop_conf.set(
        "fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )

    hadoop_conf.set(
        "fs.s3a.access.key",
        access_key,
    )

    hadoop_conf.set(
        "fs.s3a.secret.key",
        secret_key,
    )

    print("Configuration S3A appliquée.")
    print("Endpoint RustFS :", endpoint_url)
    print("Région :", region)
    print("Path style access : true")


# ============================================================
# JDBC CONFIGURATION
# ============================================================

def get_jdbc_url():
    """
    Construit l'URL JDBC PostgreSQL.
    """

    return (
        f"jdbc:postgresql://"
        f"{BI_DB_HOST}:{BI_DB_PORT}/{BI_DB_NAME}"
    )


# ============================================================
# TEST CONNEXION POSTGRESQL
# ============================================================

def test_postgresql_connection(spark, jdbc_url):
    """
    Teste la connexion PostgreSQL avant de charger les données.
    """

    print()
    print("=" * 80)
    print("TEST CONNEXION POSTGRESQL BI")
    print("=" * 80)

    test_query = """
        (
            SELECT 1 AS connection_test
        ) AS connection_test
    """

    (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", test_query)
        .option("user", BI_DB_USER)
        .option("password", BI_DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
        .show()
    )

    print("PostgreSQL BI : CONNECTION SUCCESS")


# ============================================================
# LOAD GOLD -> BI
# ============================================================

def load_table_to_bi(
    spark,
    table_name,
    jdbc_url,
):
    """
    Lit une table Delta depuis RustFS Gold
    et l'écrit dans PostgreSQL BI.
    """

    gold_path = (
        f"{GOLD_BASE_PATH}/{table_name}"
    )

    print()
    print("=" * 80)
    print(f"TABLE : {table_name}")
    print(f"GOLD  : {gold_path}")
    print("=" * 80)

    # --------------------------------------------------------
    # Lecture Delta depuis RustFS
    # --------------------------------------------------------

    df = (
        spark.read
        .format("delta")
        .load(gold_path)
    )

    source_count = df.count()

    print(
        f"Lignes Gold : {source_count}"
    )

    print(
        f"Colonnes : {df.columns}"
    )

    if source_count == 0:
        print(
            f"WARNING : {table_name} est vide."
        )

    # --------------------------------------------------------
    # Aperçu
    # --------------------------------------------------------

    df.show(
        3,
        truncate=False,
    )

    # --------------------------------------------------------
    # Écriture PostgreSQL
    # --------------------------------------------------------

    (
        df.write
        .format("jdbc")
        .option(
            "url",
            jdbc_url,
        )
        .option(
            "dbtable",
            f"public.{table_name}",
        )
        .option(
            "user",
            BI_DB_USER,
        )
        .option(
            "password",
            BI_DB_PASSWORD,
        )
        .option(
            "driver",
            "org.postgresql.Driver",
        )
        .option(
            "batchsize",
            "5000",
        )
        .option(
            "numPartitions",
            "2",
        )
        .mode("overwrite")
        .save()
    )

    print(
        f"BI LOAD SUCCESS : "
        f"{table_name} "
        f"({source_count} lignes)"
    )

    return source_count


# ============================================================
# VALIDATION BI
# ============================================================

def validate_bi_table(
    spark,
    table_name,
    jdbc_url,
):
    """
    Relit une table PostgreSQL BI pour vérifier
    que les données ont bien été chargées.
    """

    query = f"""
        (
            SELECT COUNT(*) AS row_count
            FROM public.{table_name}
        ) AS validation
    """

    result = (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", query)
        .option("user", BI_DB_USER)
        .option("password", BI_DB_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .load()
        .collect()
    )

    row_count = result[0]["row_count"]

    print(
        f"BI VALIDATION : "
        f"{table_name} = {row_count} lignes"
    )

    return row_count


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 80)
    print("GOLD -> BI PIPELINE")
    print("=" * 80)

    # --------------------------------------------------------
    # Validation environnement
    # --------------------------------------------------------

    validate_environment()

    jdbc_url = get_jdbc_url()

    print()
    print(
        f"BI PostgreSQL : "
        f"{BI_DB_HOST}:{BI_DB_PORT}/{BI_DB_NAME}"
    )

    print(
        f"BI User       : {BI_DB_USER}"
    )

    print(
        f"Gold Path     : {GOLD_BASE_PATH}"
    )

    # --------------------------------------------------------
    # Création Spark
    # --------------------------------------------------------

    spark = create_spark_session()

    results = {}

    try:

        # ----------------------------------------------------
        # Configuration RustFS
        # ----------------------------------------------------

        configure_s3a(spark)

        # ----------------------------------------------------
        # Test PostgreSQL
        # ----------------------------------------------------

        test_postgresql_connection(
            spark,
            jdbc_url,
        )

        # ----------------------------------------------------
        # Chargement dimensions
        # ----------------------------------------------------

        dimension_tables = [
            "dim_date",
            "dim_product",
            "dim_customer",
            "dim_region",
        ]

        for table_name in dimension_tables:

            results[table_name] = (
                load_table_to_bi(
                    spark,
                    table_name,
                    jdbc_url,
                )
            )

        # ----------------------------------------------------
        # Chargement FactSales
        # ----------------------------------------------------

        results["fact_sales"] = (
            load_table_to_bi(
                spark,
                "fact_sales",
                jdbc_url,
            )
        )

        # ----------------------------------------------------
        # Validation PostgreSQL
        # ----------------------------------------------------

        print()
        print("=" * 80)
        print("VALIDATION DES TABLES BI")
        print("=" * 80)

        validation_results = {}

        for table_name in TABLES:

            validation_results[table_name] = (
                validate_bi_table(
                    spark,
                    table_name,
                    jdbc_url,
                )
            )

        # ----------------------------------------------------
        # Comparaison Gold / BI
        # ----------------------------------------------------

        print()
        print("=" * 80)
        print("COMPARAISON GOLD / BI")
        print("=" * 80)

        for table_name in TABLES:

            gold_count = results[table_name]

            bi_count = validation_results[
                table_name
            ]

            print(
                f"{table_name:<20} "
                f"Gold = {gold_count:<10} "
                f"BI = {bi_count:<10}"
            )

            if gold_count != bi_count:
                raise RuntimeError(
                    f"Nombre de lignes différent "
                    f"pour {table_name} : "
                    f"Gold={gold_count}, "
                    f"BI={bi_count}"
                )

        # ----------------------------------------------------
        # Résumé
        # ----------------------------------------------------

        print()
        print("=" * 80)
        print("BI LOAD SUMMARY")
        print("=" * 80)

        total_rows = 0

        for table_name, count in results.items():

            print(
                f"{table_name:<20} : "
                f"{count:>10} lignes"
            )

            total_rows += count

        print("-" * 80)

        print(
            f"TOTAL                : "
            f"{total_rows:>10} lignes"
        )

        print()
        print("=" * 80)
        print("GOLD -> BI : SUCCESS")
        print("=" * 80)

    except Exception as exc:

        print()
        print("=" * 80)
        print("GOLD -> BI : FAILED")
        print("=" * 80)

        print(
            f"Erreur : {exc}"
        )

        print("=" * 80)

        raise

    finally:

        spark.stop()


if __name__ == "__main__":
    main()