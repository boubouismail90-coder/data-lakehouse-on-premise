from pyspark.sql import SparkSession


GOLD_BASE_PATH = "/opt/spark/data/gold-delta"

TABLES = [
    "dim_date",
    "dim_product",
    "dim_customer",
    "dim_region",
    "fact_sales",
]


def main():
    spark = (
        SparkSession.builder
        .appName("Inspect-Gold-Schema")
        .getOrCreate()
    )

    try:
        print("=" * 80)
        print("GOLD DELTA SCHEMA INSPECTION")
        print("=" * 80)

        for table_name in TABLES:
            path = f"{GOLD_BASE_PATH}/{table_name}"

            print()
            print("=" * 80)
            print(f"TABLE : {table_name}")
            print(f"PATH  : {path}")
            print("=" * 80)

            df = (
                spark.read
                .format("delta")
                .load(path)
            )

            print(f"Nombre de lignes : {df.count()}")
            print()
            print("Schema :")
            df.printSchema()

            print("Colonnes :")
            print(df.columns)

            print()
            print("Exemple de données :")
            df.show(5, truncate=False)

        print()
        print("=" * 80)
        print("GOLD SCHEMA INSPECTION : SUCCESS")
        print("=" * 80)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()