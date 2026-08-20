import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    countDistinct,
    trim,
    upper,
    when,
)
from pyspark.sql.types import (
    DateType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


# ============================================================
# CONFIGURATION
# ============================================================

BRONZE_ROOT = "s3a://bronze/sales"

SILVER_PATH = "s3a://silver/sales"


# ============================================================
# SESSION SPARK
# ============================================================

def create_spark_session():
    """
    Crée une session Spark avec support Delta Lake.
    """

    spark = (
        SparkSession.builder
        .appName("BronzeToSilverRustFSDelta")
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
# CONFIGURATION S3A / RUSTFS
# ============================================================

def configure_s3a(spark):
    """
    Configure Spark/Hadoop pour accéder à RustFS via S3A.

    Les secrets restent dans les variables d'environnement.
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
# SCHEMA BRONZE
# ============================================================

def get_bronze_schema():
    """
    Schéma explicite de la source sales.
    """

    return StructType([
        StructField(
            "id",
            IntegerType(),
            True,
        ),
        StructField(
            "sale_date",
            DateType(),
            True,
        ),
        StructField(
            "product_id",
            IntegerType(),
            True,
        ),
        StructField(
            "product_name",
            StringType(),
            True,
        ),
        StructField(
            "customer_id",
            IntegerType(),
            True,
        ),
        StructField(
            "region",
            StringType(),
            True,
        ),
        StructField(
            "quantity",
            IntegerType(),
            True,
        ),
        StructField(
            "unit_price",
            DoubleType(),
            True,
        ),
        StructField(
            "created_at",
            TimestampType(),
            True,
        ),
    ])


# ============================================================
# DERNIER FICHIER BRONZE
# ============================================================

def find_latest_bronze_file(spark):
    """
    Recherche automatiquement le fichier Bronze sales
    le plus récent dans RustFS.
    """

    print("\n========================================")
    print("RECHERCHE DERNIER BRONZE")
    print("========================================")

    jvm = spark.sparkContext._jvm

    hadoop_conf = (
        spark
        .sparkContext
        ._jsc
        .hadoopConfiguration()
    )

    path_class = (
        jvm
        .org
        .apache
        .hadoop
        .fs
        .Path
    )

    search_path = path_class(
        f"{BRONZE_ROOT}/ingestion_date=*/sales_*.csv"
    )

    file_system = (
        search_path
        .getFileSystem(
            hadoop_conf
        )
    )

    statuses = file_system.globStatus(
        search_path
    )

    if statuses is None or len(statuses) == 0:
        raise RuntimeError(
            "Aucun fichier Bronze sales trouvé."
        )

    files = [
        status
        for status in statuses
        if status.isFile()
    ]

    if not files:
        raise RuntimeError(
            "Aucun fichier CSV Bronze valide trouvé."
        )

    latest_file = max(
        files,
        key=lambda status:
        status.getModificationTime(),
    )

    latest_path = (
        latest_file
        .getPath()
        .toString()
    )

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


# ============================================================
# LECTURE BRONZE
# ============================================================

def read_bronze(spark, bronze_file):
    """
    Lit directement le dernier CSV Bronze depuis RustFS.
    """

    print("\n========================================")
    print("LECTURE BRONZE DEPUIS RUSTFS")
    print("========================================")

    schema = get_bronze_schema()

    df_bronze = (
        spark.read
        .option("header", True)
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(bronze_file)
    )

    bronze_count = df_bronze.count()

    print(
        "Nombre de lignes Bronze :",
        bronze_count,
    )

    print("\n=== SCHEMA BRONZE ===")

    df_bronze.printSchema()

    return df_bronze, bronze_count


# ============================================================
# NETTOYAGE SILVER
# ============================================================

def clean_silver(df_bronze):
    """
    Nettoyage de la couche Silver :
    - dédoublonnage
    - gestion NULL
    - nettoyage texte
    - règles métier
    """

    print("\n========================================")
    print("NETTOYAGE SILVER")
    print("========================================")

    # --------------------------------------------------------
    # Dédoublonnage
    # --------------------------------------------------------

    df_clean = (
        df_bronze
        .dropDuplicates(["id"])
    )

    # --------------------------------------------------------
    # Colonnes critiques non NULL
    # --------------------------------------------------------

    df_clean = (
        df_clean
        .filter(
            col("id").isNotNull()
            & col("sale_date").isNotNull()
            & col("product_id").isNotNull()
            & col("customer_id").isNotNull()
            & col("quantity").isNotNull()
            & col("unit_price").isNotNull()
        )
    )

    # --------------------------------------------------------
    # Product name
    # --------------------------------------------------------

    df_clean = (
        df_clean
        .withColumn(
            "product_name",
            when(
                col("product_name").isNull()
                | (
                    trim(
                        col("product_name")
                    )
                    == ""
                ),
                "UNKNOWN",
            )
            .otherwise(
                trim(
                    col("product_name")
                )
            ),
        )
    )

    # --------------------------------------------------------
    # Region
    # --------------------------------------------------------

    df_clean = (
        df_clean
        .withColumn(
            "region",
            when(
                col("region").isNull()
                | (
                    trim(
                        col("region")
                    )
                    == ""
                ),
                "UNKNOWN",
            )
            .otherwise(
                upper(
                    trim(
                        col("region")
                    )
                )
            ),
        )
    )

    # --------------------------------------------------------
    # Règles métier
    # --------------------------------------------------------

    df_clean = (
        df_clean
        .filter(
            (col("quantity") > 0)
            & (col("unit_price") >= 0)
        )
    )

    return df_clean


# ============================================================
# VALIDATION SILVER
# ============================================================

def validate_silver(
    df_silver,
    bronze_count,
):
    """
    Contrôles qualité avant écriture.
    """

    print("\n========================================")
    print("VALIDATION SILVER")
    print("========================================")

    silver_count = df_silver.count()

    print(
        "Nombre de lignes Bronze :",
        bronze_count,
    )

    print(
        "Nombre de lignes Silver :",
        silver_count,
    )

    print(
        "Nombre de lignes retirées :",
        bronze_count - silver_count,
    )

    duplicate_count = (
        df_silver
        .groupBy("id")
        .count()
        .filter(
            col("count") > 1
        )
        .count()
    )

    invalid_quantity = (
        df_silver
        .filter(
            col("quantity") <= 0
        )
        .count()
    )

    invalid_price = (
        df_silver
        .filter(
            col("unit_price") < 0
        )
        .count()
    )

    inconsistent_products = (
        df_silver
        .groupBy("product_id")
        .agg(
            countDistinct(
                "product_name"
            )
            .alias("nombre_noms")
        )
        .filter(
            col("nombre_noms") > 1
        )
        .count()
    )

    print(
        "ID dupliqués :",
        duplicate_count,
    )

    print(
        "quantity <= 0 :",
        invalid_quantity,
    )

    print(
        "unit_price < 0 :",
        invalid_price,
    )

    print(
        "product_id incohérents :",
        inconsistent_products,
    )

    if duplicate_count != 0:
        raise RuntimeError(
            "Doublons présents dans Silver."
        )

    if invalid_quantity != 0:
        raise RuntimeError(
            "Quantités invalides dans Silver."
        )

    if invalid_price != 0:
        raise RuntimeError(
            "Prix invalides dans Silver."
        )

    if inconsistent_products != 0:
        raise RuntimeError(
            "Mapping produit incohérent."
        )

    print("\n=== APERCU SILVER ===")

    df_silver.show(
        10,
        truncate=False,
    )

    return silver_count


# ============================================================
# ECRITURE SILVER DELTA DANS RUSTFS
# ============================================================

def write_silver_delta(df_silver):
    """
    Écrit Silver directement dans RustFS au format Delta.
    """

    print("\n========================================")
    print("ECRITURE SILVER DELTA DANS RUSTFS")
    print("========================================")

    (
        df_silver.write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true",
        )
        .save(
            SILVER_PATH
        )
    )

    print(
        "Silver Delta écrite dans :",
        SILVER_PATH,
    )


# ============================================================
# RELECTURE SILVER
# ============================================================

def validate_silver_storage(
    spark,
    expected_count,
):
    """
    Relit la table Silver Delta directement depuis RustFS.
    """

    print("\n========================================")
    print("RELECTURE SILVER DELTA")
    print("========================================")

    df_delta = (
        spark.read
        .format("delta")
        .load(
            SILVER_PATH
        )
    )

    delta_count = df_delta.count()

    print(
        "Nombre de lignes relues depuis RustFS Silver :",
        delta_count,
    )

    if delta_count != expected_count:
        raise RuntimeError(
            "Le nombre de lignes relues "
            "ne correspond pas au Silver attendu."
        )

    return delta_count


# ============================================================
# VERIFICATION _DELTA_LOG
# ============================================================

def validate_delta_log(spark):
    """
    Vérifie que Silver possède bien son _delta_log.
    """

    print("\n========================================")
    print("VERIFICATION _DELTA_LOG SILVER")
    print("========================================")

    jvm = spark.sparkContext._jvm

    hadoop_conf = (
        spark
        .sparkContext
        ._jsc
        .hadoopConfiguration()
    )

    delta_log_string = (
        f"{SILVER_PATH}/_delta_log"
    )

    delta_log_path = (
        jvm
        .org
        .apache
        .hadoop
        .fs
        .Path(
            delta_log_string
        )
    )

    file_system = (
        delta_log_path
        .getFileSystem(
            hadoop_conf
        )
    )

    exists = file_system.exists(
        delta_log_path
    )

    print(
        "_delta_log Silver existe :",
        exists,
    )

    if not exists:
        raise RuntimeError(
            "_delta_log Silver introuvable."
        )


# ============================================================
# MAIN
# ============================================================

def main():
    spark = create_spark_session()

    print("========================================")
    print("BRONZE RUSTFS -> SILVER RUSTFS DELTA")
    print("========================================")

    try:
        configure_s3a(
            spark
        )

        bronze_file = (
            find_latest_bronze_file(
                spark
            )
        )

        (
            df_bronze,
            bronze_count,
        ) = read_bronze(
            spark,
            bronze_file,
        )

        df_silver = clean_silver(
            df_bronze
        )

        silver_count = validate_silver(
            df_silver,
            bronze_count,
        )

        write_silver_delta(
            df_silver
        )

        delta_count = validate_silver_storage(
            spark,
            silver_count,
        )

        validate_delta_log(
            spark
        )

        print("\n========================================")
        print("RESULTAT FINAL")
        print("========================================")

        print(
            "Bronze :",
            bronze_count,
            "lignes",
        )

        print(
            "Silver :",
            delta_count,
            "lignes",
        )

        print(
            "Destination :",
            SILVER_PATH,
        )

        print(
            "BRONZE RUSTFS -> SILVER RUSTFS DELTA : SUCCESS"
        )

    except Exception as error:
        print("\n========================================")
        print("BRONZE RUSTFS -> SILVER RUSTFS DELTA : FAILED")
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