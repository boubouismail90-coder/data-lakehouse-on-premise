import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    countDistinct,
    date_format,
    dayofmonth,
    first,
    month,
    quarter,
    round,
    row_number,
    year,
)
from pyspark.sql.types import DecimalType
from pyspark.sql.window import Window


# ============================================================
# CONFIGURATION DES CHEMINS RUSTFS
# ============================================================

SILVER_PATH = "s3a://silver/sales"

GOLD_BASE_PATH = "s3a://gold"

DIM_DATE_PATH = f"{GOLD_BASE_PATH}/dim_date"
DIM_PRODUCT_PATH = f"{GOLD_BASE_PATH}/dim_product"
DIM_CUSTOMER_PATH = f"{GOLD_BASE_PATH}/dim_customer"
DIM_REGION_PATH = f"{GOLD_BASE_PATH}/dim_region"
FACT_SALES_PATH = f"{GOLD_BASE_PATH}/fact_sales"


# ============================================================
# SESSION SPARK
# ============================================================

def create_spark_session():
    """
    Crée une session Spark avec support Delta Lake.
    """

    spark = (
        SparkSession.builder
        .appName("SilverToGoldRustFSDelta")
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
    Configure Spark/Hadoop pour communiquer avec RustFS.

    Les identifiants restent dans les variables
    d'environnement Docker et ne sont jamais affichés.
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
# DIMENSION DATE
# ============================================================

def create_dim_date(df_silver):
    """
    Crée DimDate.

    Une ligne correspond à une date de vente unique.
    """

    print("\n========================================")
    print("CREATION DIM_DATE")
    print("========================================")

    dim_date = (
        df_silver
        .select(
            col("sale_date").alias("full_date")
        )
        .dropDuplicates(["full_date"])
        .withColumn(
            "date_key",
            date_format(
                col("full_date"),
                "yyyyMMdd",
            ).cast("int"),
        )
        .withColumn(
            "year",
            year(col("full_date")),
        )
        .withColumn(
            "quarter",
            quarter(col("full_date")),
        )
        .withColumn(
            "month",
            month(col("full_date")),
        )
        .withColumn(
            "day",
            dayofmonth(col("full_date")),
        )
        .select(
            "date_key",
            "full_date",
            "year",
            "quarter",
            "month",
            "day",
        )
        .orderBy("date_key")
    )

    count = dim_date.count()

    print(
        "Nombre de lignes DimDate :",
        count,
    )

    dim_date.show(
        10,
        truncate=False,
    )

    return dim_date


# ============================================================
# DIMENSION PRODUIT
# ============================================================

def create_dim_product(df_silver):
    """
    Crée DimProduct.

    product_id est la clé métier provenant de la source.
    product_key est la clé technique Gold.
    """

    print("\n========================================")
    print("CREATION DIM_PRODUCT")
    print("========================================")

    inconsistent_products = (
        df_silver
        .groupBy("product_id")
        .agg(
            countDistinct("product_name")
            .alias("product_name_count")
        )
        .filter(
            col("product_name_count") > 1
        )
        .count()
    )

    print(
        "Produits avec noms incohérents :",
        inconsistent_products,
    )

    if inconsistent_products != 0:
        raise RuntimeError(
            "Incohérence produit détectée."
        )

    product_window = (
        Window.orderBy("product_id")
    )

    dim_product = (
        df_silver
        .groupBy("product_id")
        .agg(
            first(
                "product_name",
                ignorenulls=True,
            ).alias("product_name")
        )
        .withColumn(
            "product_key",
            row_number()
            .over(product_window)
            .cast("int"),
        )
        .select(
            "product_key",
            "product_id",
            "product_name",
        )
        .orderBy("product_key")
    )

    count = dim_product.count()

    print(
        "Nombre de lignes DimProduct :",
        count,
    )

    dim_product.show(
        10,
        truncate=False,
    )

    return dim_product


# ============================================================
# DIMENSION CLIENT
# ============================================================

def create_dim_customer(df_silver):
    """
    Crée DimCustomer.

    La source actuelle contient customer_id comme
    information client.
    """

    print("\n========================================")
    print("CREATION DIM_CUSTOMER")
    print("========================================")

    customer_window = (
        Window.orderBy("customer_id")
    )

    dim_customer = (
        df_silver
        .select("customer_id")
        .dropDuplicates(["customer_id"])
        .withColumn(
            "customer_key",
            row_number()
            .over(customer_window)
            .cast("int"),
        )
        .select(
            "customer_key",
            "customer_id",
        )
        .orderBy("customer_key")
    )

    count = dim_customer.count()

    print(
        "Nombre de lignes DimCustomer :",
        count,
    )

    dim_customer.show(
        10,
        truncate=False,
    )

    return dim_customer


# ============================================================
# DIMENSION REGION
# ============================================================

def create_dim_region(df_silver):
    """
    Crée DimRegion.
    """

    print("\n========================================")
    print("CREATION DIM_REGION")
    print("========================================")

    dim_region_base = (
        df_silver
        .select(
            col("region").alias("region_name")
        )
        .dropDuplicates(["region_name"])
    )

    region_window = (
        Window.orderBy("region_name")
    )

    dim_region = (
        dim_region_base
        .withColumn(
            "region_key",
            row_number()
            .over(region_window)
            .cast("int"),
        )
        .select(
            "region_key",
            "region_name",
        )
        .orderBy("region_key")
    )

    count = dim_region.count()

    print(
        "Nombre de lignes DimRegion :",
        count,
    )

    dim_region.show(
        10,
        truncate=False,
    )

    return dim_region


# ============================================================
# TABLE DE FAITS
# ============================================================

def create_fact_sales(
    df_silver,
    dim_date,
    dim_product,
    dim_customer,
    dim_region,
):
    """
    Crée FactSales.

    Grain :
    une ligne FactSales = une vente Silver.
    """

    print("\n========================================")
    print("CREATION FACT_SALES")
    print("========================================")

    silver = df_silver.alias("s")

    dates = (
        dim_date
        .select(
            "date_key",
            "full_date",
        )
        .alias("d")
    )

    products = (
        dim_product
        .select(
            "product_key",
            "product_id",
        )
        .alias("p")
    )

    customers = (
        dim_customer
        .select(
            "customer_key",
            "customer_id",
        )
        .alias("c")
    )

    regions = (
        dim_region
        .select(
            "region_key",
            "region_name",
        )
        .alias("r")
    )

    fact_sales = (
        silver
        .join(
            dates,
            col("s.sale_date")
            == col("d.full_date"),
            "left",
        )
        .join(
            products,
            col("s.product_id")
            == col("p.product_id"),
            "left",
        )
        .join(
            customers,
            col("s.customer_id")
            == col("c.customer_id"),
            "left",
        )
        .join(
            regions,
            col("s.region")
            == col("r.region_name"),
            "left",
        )
        .select(
            col("s.id")
            .alias("sale_id"),

            col("d.date_key"),

            col("p.product_key"),

            col("c.customer_key"),

            col("r.region_key"),

            col("s.quantity"),

            col("s.unit_price"),

            round(
                col("s.quantity")
                * col("s.unit_price"),
                2,
            )
            .cast(
                DecimalType(18, 2)
            )
            .alias("total_amount"),

            col("s.created_at"),
        )
    )

    count = fact_sales.count()

    print(
        "Nombre de lignes FactSales :",
        count,
    )

    fact_sales.show(
        10,
        truncate=False,
    )

    return fact_sales


# ============================================================
# VALIDATION GOLD
# ============================================================

def validate_gold(
    df_silver,
    fact_sales,
):
    """
    Contrôles qualité principaux de FactSales.
    """

    print("\n========================================")
    print("VALIDATION GOLD")
    print("========================================")

    silver_count = df_silver.count()

    fact_count = fact_sales.count()

    print(
        "Nombre de lignes Silver :",
        silver_count,
    )

    print(
        "Nombre de lignes FactSales :",
        fact_count,
    )

    if fact_count != silver_count:
        raise RuntimeError(
            "FactSales ne contient pas le même "
            "nombre de lignes que Silver."
        )

    null_date_keys = (
        fact_sales
        .filter(
            col("date_key").isNull()
        )
        .count()
    )

    null_product_keys = (
        fact_sales
        .filter(
            col("product_key").isNull()
        )
        .count()
    )

    null_customer_keys = (
        fact_sales
        .filter(
            col("customer_key").isNull()
        )
        .count()
    )

    null_region_keys = (
        fact_sales
        .filter(
            col("region_key").isNull()
        )
        .count()
    )

    print(
        "date_key NULL :",
        null_date_keys,
    )

    print(
        "product_key NULL :",
        null_product_keys,
    )

    print(
        "customer_key NULL :",
        null_customer_keys,
    )

    print(
        "region_key NULL :",
        null_region_keys,
    )

    if (
        null_date_keys != 0
        or null_product_keys != 0
        or null_customer_keys != 0
        or null_region_keys != 0
    ):
        raise RuntimeError(
            "Des clés étrangères sont NULL "
            "dans FactSales."
        )

    duplicate_sales = (
        fact_sales
        .groupBy("sale_id")
        .count()
        .filter(
            col("count") > 1
        )
        .count()
    )

    print(
        "sale_id dupliqués :",
        duplicate_sales,
    )

    if duplicate_sales != 0:
        raise RuntimeError(
            "Des sale_id dupliqués existent."
        )

    invalid_amount = (
        fact_sales
        .filter(
            col("total_amount") < 0
        )
        .count()
    )

    print(
        "total_amount négatifs :",
        invalid_amount,
    )

    if invalid_amount != 0:
        raise RuntimeError(
            "Des montants négatifs existent."
        )


# ============================================================
# ECRITURE DELTA
# ============================================================

def write_delta(
    df,
    path,
    table_name,
):
    """
    Écrit une table Gold directement dans RustFS.
    """

    print("\n========================================")
    print(
        f"ECRITURE {table_name} DANS RUSTFS"
    )
    print("========================================")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true",
        )
        .save(path)
    )

    print(
        f"{table_name} écrite dans :",
        path,
    )


# ============================================================
# RELECTURE GOLD
# ============================================================

def validate_gold_storage(spark):
    """
    Relit les 5 tables Gold directement depuis RustFS.
    """

    print("\n========================================")
    print("RELECTURE GOLD DEPUIS RUSTFS")
    print("========================================")

    tables = {
        "DimDate": DIM_DATE_PATH,
        "DimProduct": DIM_PRODUCT_PATH,
        "DimCustomer": DIM_CUSTOMER_PATH,
        "DimRegion": DIM_REGION_PATH,
        "FactSales": FACT_SALES_PATH,
    }

    results = {}

    for table_name, path in tables.items():

        df = (
            spark.read
            .format("delta")
            .load(path)
        )

        row_count = df.count()

        results[table_name] = row_count

        print(
            f"{table_name} relue depuis RustFS : "
            f"{row_count} lignes"
        )

    return results


# ============================================================
# VERIFICATION _DELTA_LOG
# ============================================================

def validate_delta_logs(spark):
    """
    Vérifie que les cinq tables possèdent un _delta_log.
    """

    print("\n========================================")
    print("VERIFICATION DES _DELTA_LOG")
    print("========================================")

    jvm = spark.sparkContext._jvm

    hadoop_conf = (
        spark
        .sparkContext
        ._jsc
        .hadoopConfiguration()
    )

    tables = {
        "DimDate": DIM_DATE_PATH,
        "DimProduct": DIM_PRODUCT_PATH,
        "DimCustomer": DIM_CUSTOMER_PATH,
        "DimRegion": DIM_REGION_PATH,
        "FactSales": FACT_SALES_PATH,
    }

    for table_name, path in tables.items():

        delta_log_string = (
            f"{path}/_delta_log"
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

        exists = (
            file_system.exists(
                delta_log_path
            )
        )

        print(
            f"{table_name} _delta_log existe :",
            exists,
        )

        if not exists:
            raise RuntimeError(
                f"_delta_log absent pour {table_name}."
            )


# ============================================================
# MAIN
# ============================================================

def main():
    spark = create_spark_session()

    df_silver = None

    print("========================================")
    print("SILVER RUSTFS -> GOLD RUSTFS DELTA")
    print("========================================")

    try:

        # ----------------------------------------------------
        # Configuration RustFS
        # ----------------------------------------------------

        configure_s3a(
            spark
        )

        # ----------------------------------------------------
        # Lecture Silver
        # ----------------------------------------------------

        print("\n========================================")
        print("LECTURE SILVER DELTA DEPUIS RUSTFS")
        print("========================================")

        df_silver = (
            spark.read
            .format("delta")
            .load(
                SILVER_PATH
            )
        )

        df_silver.cache()

        silver_count = (
            df_silver.count()
        )

        print(
            "Nombre de lignes Silver :",
            silver_count,
        )

        # ----------------------------------------------------
        # Dimensions
        # ----------------------------------------------------

        dim_date = (
            create_dim_date(
                df_silver
            )
        )

        dim_product = (
            create_dim_product(
                df_silver
            )
        )

        dim_customer = (
            create_dim_customer(
                df_silver
            )
        )

        dim_region = (
            create_dim_region(
                df_silver
            )
        )

        # ----------------------------------------------------
        # FactSales
        # ----------------------------------------------------

        fact_sales = (
            create_fact_sales(
                df_silver,
                dim_date,
                dim_product,
                dim_customer,
                dim_region,
            )
        )

        # ----------------------------------------------------
        # Validation
        # ----------------------------------------------------

        validate_gold(
            df_silver,
            fact_sales,
        )

        # ----------------------------------------------------
        # Ecriture RustFS Gold
        # ----------------------------------------------------

        write_delta(
            dim_date,
            DIM_DATE_PATH,
            "DimDate",
        )

        write_delta(
            dim_product,
            DIM_PRODUCT_PATH,
            "DimProduct",
        )

        write_delta(
            dim_customer,
            DIM_CUSTOMER_PATH,
            "DimCustomer",
        )

        write_delta(
            dim_region,
            DIM_REGION_PATH,
            "DimRegion",
        )

        write_delta(
            fact_sales,
            FACT_SALES_PATH,
            "FactSales",
        )

        # ----------------------------------------------------
        # Relecture
        # ----------------------------------------------------

        results = (
            validate_gold_storage(
                spark
            )
        )

        # ----------------------------------------------------
        # Delta logs
        # ----------------------------------------------------

        validate_delta_logs(
            spark
        )

        # ----------------------------------------------------
        # Résultat final
        # ----------------------------------------------------

        print("\n========================================")
        print("STAR SCHEMA GOLD")
        print("========================================")

        for table_name, count in results.items():

            print(
                table_name,
                ":",
                count,
                "lignes",
            )

        print("\n========================================")
        print("RESULTAT FINAL")
        print("========================================")

        print(
            "SILVER RUSTFS -> GOLD RUSTFS DELTA : SUCCESS"
        )

    except Exception as error:

        print("\n========================================")
        print("SILVER RUSTFS -> GOLD RUSTFS DELTA : FAILED")
        print("========================================")

        print(
            "Erreur :",
            str(error),
        )

        raise

    finally:

        if df_silver is not None:
            try:
                df_silver.unpersist()
            except Exception:
                pass

        spark.stop()


if __name__ == "__main__":
    main()