from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, when
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    DoubleType,
    DateType,
    TimestampType,
)


def create_spark_session():
    """
    Crée la session Spark avec le support Delta Lake.
    """
    return (
        SparkSession.builder
        .appName("BronzeToSilverDelta")
        .master("spark://spark:7077")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension"
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog"
        )
        .getOrCreate()
    )


def get_bronze_schema():
    """
    Schéma explicite de la source Bronze.

    On évite inferSchema afin de garantir un typage
    déterministe et reproductible dans la couche Silver.
    """
    return StructType([
        StructField("id", IntegerType(), True),
        StructField("sale_date", DateType(), True),
        StructField("product_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("region", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("created_at", TimestampType(), True),
    ])


def main():
    spark = create_spark_session()

    bronze_path = "/opt/spark/data/bronze/sales.csv"

    # Nouvelle destination Delta.
    # On ne touche pas à l'ancien Silver Parquet.
    silver_delta_path = "/opt/spark/data/silver-delta/sales"

    print("========================================")
    print("BRONZE -> SILVER DELTA LAKE")
    print("========================================")

    try:
        # -------------------------------------------------
        # 1. LECTURE BRONZE
        # -------------------------------------------------

        print("\n=== LECTURE BRONZE ===")

        bronze_schema = get_bronze_schema()

        df_bronze = (
            spark.read
            .option("header", True)
            .option("mode", "PERMISSIVE")
            .schema(bronze_schema)
            .csv(bronze_path)
        )

        bronze_count = df_bronze.count()

        print(
            "Nombre de lignes Bronze :",
            bronze_count
        )

        print("\n=== SCHEMA BRONZE ===")

        df_bronze.printSchema()

        # -------------------------------------------------
        # 2. DEDOUBLONNAGE
        # -------------------------------------------------

        print("\n=== DEDOUBLONNAGE ===")

        df_clean = df_bronze.dropDuplicates(["id"])

        after_duplicates_count = df_clean.count()

        print(
            "Nombre de lignes après dédoublonnage :",
            after_duplicates_count
        )

        print(
            "Nombre de doublons supprimés :",
            bronze_count - after_duplicates_count
        )

        # -------------------------------------------------
        # 3. GESTION DES NULL CRITIQUES
        # -------------------------------------------------

        print("\n=== GESTION DES NULL CRITIQUES ===")

        # Ces colonnes sont indispensables pour identifier
        # et exploiter correctement une vente.
        df_clean = df_clean.filter(
            col("id").isNotNull()
            & col("sale_date").isNotNull()
            & col("product_id").isNotNull()
            & col("customer_id").isNotNull()
            & col("quantity").isNotNull()
            & col("unit_price").isNotNull()
        )

        # -------------------------------------------------
        # 4. NETTOYAGE DES CHAMPS TEXTE
        # -------------------------------------------------

        print("\n=== NETTOYAGE TEXTE ===")

        df_clean = df_clean.withColumn(
            "product_name",
            when(
                col("product_name").isNull()
                | (trim(col("product_name")) == ""),
                "UNKNOWN"
            ).otherwise(
                trim(col("product_name"))
            )
        )

        df_clean = df_clean.withColumn(
            "region",
            when(
                col("region").isNull()
                | (trim(col("region")) == ""),
                "UNKNOWN"
            ).otherwise(
                upper(trim(col("region")))
            )
        )

        # -------------------------------------------------
        # 5. REGLES DE QUALITE METIER
        # -------------------------------------------------

        print("\n=== CONTROLES METIER ===")

        df_clean = df_clean.filter(
            (col("quantity") > 0)
            & (col("unit_price") >= 0)
        )

        # -------------------------------------------------
        # 6. CONTROLE FINAL
        # -------------------------------------------------

        silver_count = df_clean.count()

        print("\n=== RESULTAT NETTOYAGE ===")

        print(
            "Nombre de lignes Bronze :",
            bronze_count
        )

        print(
            "Nombre de lignes Silver :",
            silver_count
        )

        print(
            "Nombre total de lignes retirées :",
            bronze_count - silver_count
        )

        print("\n=== SCHEMA SILVER ===")

        df_clean.printSchema()

        print("\n=== APERCU SILVER ===")

        df_clean.show(
            10,
            truncate=False
        )

        # -------------------------------------------------
        # 7. ECRITURE DELTA LAKE
        # -------------------------------------------------

        print("\n=== ECRITURE SILVER DELTA ===")

        (
            df_clean.write
            .format("delta")
            .mode("overwrite")
            .save(silver_delta_path)
        )

        print(
            "Silver Delta écrite dans :",
            silver_delta_path
        )

        # -------------------------------------------------
        # 8. RELECTURE DELTA
        # -------------------------------------------------

        print("\n=== RELECTURE SILVER DELTA ===")

        df_delta = (
            spark.read
            .format("delta")
            .load(silver_delta_path)
        )

        delta_count = df_delta.count()

        print(
            "Nombre de lignes relues depuis Delta :",
            delta_count
        )

        # -------------------------------------------------
        # 9. VALIDATION
        # -------------------------------------------------

        print("\n=== VALIDATION ===")

        if delta_count != silver_count:
            raise RuntimeError(
                "Le nombre de lignes relues depuis Delta "
                "ne correspond pas au nombre de lignes Silver."
            )

        duplicate_count = (
            df_delta
            .groupBy("id")
            .count()
            .filter(col("count") > 1)
            .count()
        )

        invalid_quantity_count = (
            df_delta
            .filter(col("quantity") <= 0)
            .count()
        )

        invalid_price_count = (
            df_delta
            .filter(col("unit_price") < 0)
            .count()
        )

        print(
            "Nombre d'ID dupliqués :",
            duplicate_count
        )

        print(
            "Nombre de quantity <= 0 :",
            invalid_quantity_count
        )

        print(
            "Nombre de unit_price < 0 :",
            invalid_price_count
        )

        if duplicate_count != 0:
            raise RuntimeError(
                "Des ID dupliqués existent dans Silver Delta."
            )

        if invalid_quantity_count != 0:
            raise RuntimeError(
                "Des quantités invalides existent dans Silver Delta."
            )

        if invalid_price_count != 0:
            raise RuntimeError(
                "Des prix invalides existent dans Silver Delta."
            )

        print("\n========================================")
        print("BRONZE -> SILVER DELTA : SUCCESS")
        print("========================================")

    except Exception as error:
        print("\n========================================")
        print("BRONZE -> SILVER DELTA : FAILED")
        print("========================================")

        print(
            "Erreur :",
            str(error)
        )

        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    main()