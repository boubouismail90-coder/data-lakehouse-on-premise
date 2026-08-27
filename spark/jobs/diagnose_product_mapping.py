from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    collect_set,
    countDistinct,
    size,
)


SILVER_PATH = "/opt/spark/data/silver-delta/sales"


def create_spark_session():
    return (
        SparkSession.builder
        .appName("DiagnoseProductMapping")
        .master("spark://spark:7077")
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )


def main():
    spark = create_spark_session()

    print("========================================")
    print("DIAGNOSTIC PRODUCT_ID / PRODUCT_NAME")
    print("========================================")

    try:
        print("\n=== LECTURE SILVER DELTA ===")

        df = (
            spark.read
            .format("delta")
            .load(SILVER_PATH)
        )

        print(
            "Nombre de lignes Silver :",
            df.count(),
        )

        print("\n=== PRODUITS INCOHERENTS ===")

        inconsistent_products = (
            df
            .groupBy("product_id")
            .agg(
                countDistinct("product_name")
                .alias("nombre_noms"),

                collect_set("product_name")
                .alias("product_names"),
            )
            .filter(
                col("nombre_noms") > 1
            )
            .orderBy(
                col("nombre_noms").desc(),
                col("product_id"),
            )
        )

        inconsistent_count = inconsistent_products.count()

        print(
            "Nombre de product_id incohérents :",
            inconsistent_count,
        )

        print("\n=== DETAIL DES INCOHERENCES ===")

        inconsistent_products.show(
            100,
            truncate=False,
        )

        print("\n=== NOMBRE MAXIMUM DE NOMS POUR UN PRODUCT_ID ===")

        max_names = (
            inconsistent_products
            .select("nombre_noms")
            .orderBy(
                col("nombre_noms").desc()
            )
            .first()
        )

        if max_names is not None:
            print(
                "Maximum de product_name pour un product_id :",
                max_names["nombre_noms"],
            )
        else:
            print(
                "Aucune incohérence trouvée."
            )

        print("\n========================================")
        print("DIAGNOSTIC PRODUCT MAPPING : SUCCESS")
        print("========================================")

    except Exception as error:
        print("\n========================================")
        print("DIAGNOSTIC PRODUCT MAPPING : FAILED")
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