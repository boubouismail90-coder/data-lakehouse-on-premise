import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct


BRONZE_ROOT = "s3a://bronze/sales"


def create_spark_session():
    """
    Crée la session Spark.

    Les paramètres S3A sont ensuite appliqués dans la
    configuration Hadoop afin de ne pas écrire les secrets
    directement dans le code.
    """

    spark = (
        SparkSession.builder
        .appName("RustFSS3AHealthCheck")
        .master("spark://spark:7077")
        .getOrCreate()
    )

    return spark


def configure_s3a(spark):
    """
    Configure Hadoop S3A pour communiquer avec RustFS.

    Les identifiants sont récupérés exclusivement depuis
    les variables d'environnement Docker.
    """

    endpoint_url = os.environ.get("RUSTFS_ENDPOINT_URL")
    access_key = os.environ.get("RUSTFS_ACCESS_KEY")
    secret_key = os.environ.get("RUSTFS_SECRET_KEY")
    region = os.environ.get("RUSTFS_REGION", "us-east-1")

    missing_variables = []

    if not endpoint_url:
        missing_variables.append("RUSTFS_ENDPOINT_URL")

    if not access_key:
        missing_variables.append("RUSTFS_ACCESS_KEY")

    if not secret_key:
        missing_variables.append("RUSTFS_SECRET_KEY")

    if missing_variables:
        raise RuntimeError(
            "Variables RustFS manquantes : "
            + ", ".join(missing_variables)
        )

    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

    # Connecteur S3A
    hadoop_conf.set(
        "fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem",
    )

    # Endpoint RustFS interne au réseau Docker
    hadoop_conf.set(
        "fs.s3a.endpoint",
        endpoint_url,
    )

    # Région
    hadoop_conf.set(
        "fs.s3a.endpoint.region",
        region,
    )

    # RustFS fonctionne ici avec accès S3 path-style.
    hadoop_conf.set(
        "fs.s3a.path.style.access",
        "true",
    )

    # Notre endpoint interne est HTTP :
    # http://rustfs:9000
    hadoop_conf.set(
        "fs.s3a.connection.ssl.enabled",
        "false",
    )

    # Authentification statique S3.
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

    # IMPORTANT :
    # ne jamais afficher access_key ou secret_key


def find_latest_bronze_file(spark):
    """
    Recherche le fichier sales CSV le plus récent
    directement dans RustFS via S3A.
    """

    print("\n========================================")
    print("RECHERCHE DU DERNIER FICHIER BRONZE")
    print("========================================")

    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()

    path_class = jvm.org.apache.hadoop.fs.Path

    bronze_path = path_class(
        f"{BRONZE_ROOT}/ingestion_date=*/sales_*.csv"
    )

    file_system = bronze_path.getFileSystem(
        hadoop_conf
    )

    statuses = file_system.globStatus(
        bronze_path
    )

    if statuses is None or len(statuses) == 0:
        raise RuntimeError(
            "Aucun fichier Bronze sales trouvé dans RustFS."
        )

    files = []

    for status in statuses:
        if status.isFile():
            files.append(status)

    if not files:
        raise RuntimeError(
            "Aucun fichier CSV Bronze valide trouvé."
        )

    latest_file = max(
        files,
        key=lambda status: status.getModificationTime(),
    )

    latest_path = latest_file.getPath().toString()

    print(
        "Nombre de fichiers Bronze trouvés :",
        len(files),
    )

    print(
        "Dernier fichier Bronze :",
        latest_path,
    )

    print(
        "Taille :",
        latest_file.getLen(),
        "octets",
    )

    return latest_path


def validate_bronze_file(spark, bronze_file):
    """
    Lit directement le CSV depuis RustFS avec Spark
    et effectue quelques contrôles.
    """

    print("\n========================================")
    print("LECTURE DIRECTE RUSTFS -> SPARK")
    print("========================================")

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(bronze_file)
    )

    row_count = df.count()

    print(
        "Nombre de lignes Bronze lues via S3A :",
        row_count,
    )

    print("\n=== SCHEMA ===")

    df.printSchema()

    print("\n=== APERCU ===")

    df.show(
        10,
        truncate=False,
    )

    print("\n========================================")
    print("VALIDATION PRODUCT MAPPING")
    print("========================================")

    inconsistent_products = (
        df
        .groupBy("product_id")
        .agg(
            countDistinct("product_name")
            .alias("nombre_noms")
        )
        .filter(
            col("nombre_noms") > 1
        )
        .count()
    )

    print(
        "Nombre de product_id incohérents :",
        inconsistent_products,
    )

    if row_count != 100000:
        raise RuntimeError(
            f"Nombre de lignes inattendu : {row_count}"
        )

    if inconsistent_products != 0:
        raise RuntimeError(
            "Le mapping product_id/product_name "
            "est encore incohérent."
        )

    return row_count


def main():
    spark = create_spark_session()

    print("========================================")
    print("RUSTFS S3A HEALTH CHECK")
    print("========================================")

    try:
        configure_s3a(
            spark
        )

        latest_file = find_latest_bronze_file(
            spark
        )

        row_count = validate_bronze_file(
            spark,
            latest_file,
        )

        print("\n========================================")
        print("RESULTAT")
        print("========================================")

        print(
            "Lignes lues :",
            row_count,
        )

        print(
            "RUSTFS S3A HEALTH CHECK : SUCCESS"
        )

    except Exception as error:
        print("\n========================================")
        print("RUSTFS S3A HEALTH CHECK : FAILED")
        print("========================================")

        print(
            "Erreur :",
            str(error),
        )

        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
    