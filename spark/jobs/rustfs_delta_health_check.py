import os

from pyspark.sql import SparkSession


# ============================================================
# CONFIGURATION
# ============================================================

DELTA_TEST_PATH = "s3a://silver/delta-health-check"


# ============================================================
# CREATION SESSION SPARK
# ============================================================

def create_spark_session():
    """
    Crée une session Spark avec Delta Lake activé.
    """

    spark = (
        SparkSession.builder
        .appName("RustFSDeltaHealthCheck")
        .master("spark://spark:7077")
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

    return spark


# ============================================================
# CONFIGURATION S3A
# ============================================================

def configure_s3a(spark):
    """
    Configure Hadoop S3A pour accéder à RustFS.

    Les identifiants sont récupérés depuis les variables
    d'environnement du conteneur Docker.
    Aucun secret n'est écrit dans le code.
    """

    endpoint_url = os.environ.get("RUSTFS_ENDPOINT_URL")
    access_key = os.environ.get("RUSTFS_ACCESS_KEY")
    secret_key = os.environ.get("RUSTFS_SECRET_KEY")
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

    # Implémentation S3A
    hadoop_conf.set(
        "fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem",
    )

    # Endpoint RustFS interne Docker
    hadoop_conf.set(
        "fs.s3a.endpoint",
        endpoint_url,
    )

    # Région S3
    hadoop_conf.set(
        "fs.s3a.endpoint.region",
        region,
    )

    # Obligatoire/recommandé pour un stockage S3 compatible
    # comme RustFS.
    hadoop_conf.set(
        "fs.s3a.path.style.access",
        "true",
    )

    # L'endpoint RustFS interne utilise HTTP.
    hadoop_conf.set(
        "fs.s3a.connection.ssl.enabled",
        "false",
    )

    # Authentification par access key / secret key.
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
    # access_key et secret_key ne sont jamais affichées.


# ============================================================
# ECRITURE DELTA
# ============================================================

def write_delta_test(spark):
    """
    Crée un petit DataFrame et l'écrit directement
    dans le bucket Silver de RustFS au format Delta.
    """

    print("\n========================================")
    print("CREATION DATAFRAME DE TEST")
    print("========================================")

    data = [
        (1, "Bronze"),
        (2, "Silver"),
        (3, "Gold"),
    ]

    df = spark.createDataFrame(
        data,
        [
            "id",
            "layer",
        ],
    )

    df.show(
        truncate=False
    )

    print("\n========================================")
    print("ECRITURE DELTA DANS RUSTFS")
    print("========================================")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(DELTA_TEST_PATH)
    )

    print(
        "Table Delta écrite dans :",
        DELTA_TEST_PATH,
    )


# ============================================================
# RELECTURE DELTA
# ============================================================

def read_delta_test(spark):
    """
    Relit directement depuis RustFS la table Delta
    que nous venons d'écrire.
    """

    print("\n========================================")
    print("RELECTURE DELTA DEPUIS RUSTFS")
    print("========================================")

    df_delta = (
        spark.read
        .format("delta")
        .load(DELTA_TEST_PATH)
    )

    df_delta.show(
        truncate=False
    )

    row_count = df_delta.count()

    print(
        "Nombre de lignes Delta relues :",
        row_count,
    )

    if row_count != 3:
        raise RuntimeError(
            f"Nombre de lignes inattendu : {row_count}"
        )

    return row_count


# ============================================================
# VERIFICATION _DELTA_LOG
# ============================================================

def validate_delta_log(spark):
    """
    Vérifie que le dossier _delta_log existe réellement
    dans RustFS.

    Cela permet de confirmer que nous avons une vraie
    table Delta Lake et pas seulement des fichiers Parquet.
    """

    print("\n========================================")
    print("VERIFICATION _DELTA_LOG")
    print("========================================")

    jvm = spark.sparkContext._jvm

    hadoop_conf = (
        spark
        .sparkContext
        ._jsc
        .hadoopConfiguration()
    )

    delta_log_path_string = (
        f"{DELTA_TEST_PATH}/_delta_log"
    )

    delta_log_path = (
        jvm
        .org
        .apache
        .hadoop
        .fs
        .Path(
            delta_log_path_string
        )
    )

    file_system = (
        delta_log_path
        .getFileSystem(
            hadoop_conf
        )
    )

    delta_log_exists = (
        file_system.exists(
            delta_log_path
        )
    )

    print(
        "_delta_log existe :",
        delta_log_exists,
    )

    if not delta_log_exists:
        raise RuntimeError(
            "_delta_log introuvable dans RustFS."
        )

    statuses = (
        file_system
        .listStatus(
            delta_log_path
        )
    )

    files = []

    for status in statuses:
        files.append(
            status
            .getPath()
            .getName()
        )

    print(
        "Contenu de _delta_log :"
    )

    for file_name in files:
        print(
            " -",
            file_name,
        )

    json_files = [
        file_name
        for file_name in files
        if file_name.endswith(".json")
    ]

    if not json_files:
        raise RuntimeError(
            "Aucun fichier JSON trouvé "
            "dans _delta_log."
        )

    print(
        "Nombre de commits JSON Delta :",
        len(json_files),
    )


# ============================================================
# MAIN
# ============================================================

def main():
    spark = create_spark_session()

    print("========================================")
    print("RUSTFS DELTA LAKE HEALTH CHECK")
    print("========================================")

    try:
        # ----------------------------------------------
        # Configuration connexion RustFS
        # ----------------------------------------------

        configure_s3a(
            spark
        )

        # ----------------------------------------------
        # Ecriture Delta
        # ----------------------------------------------

        write_delta_test(
            spark
        )

        # ----------------------------------------------
        # Relecture Delta
        # ----------------------------------------------

        row_count = read_delta_test(
            spark
        )

        # ----------------------------------------------
        # Vérification du journal Delta
        # ----------------------------------------------

        validate_delta_log(
            spark
        )

        # ----------------------------------------------
        # Résultat final
        # ----------------------------------------------

        print("\n========================================")
        print("RESULTAT")
        print("========================================")

        print(
            "Nombre de lignes validées :",
            row_count,
        )

        print(
            "RUSTFS DELTA LAKE HEALTH CHECK : SUCCESS"
        )

    except Exception as error:
        print("\n========================================")
        print("RUSTFS DELTA LAKE HEALTH CHECK : FAILED")
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