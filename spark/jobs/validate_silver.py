from pyspark.sql import SparkSession
from pyspark.sql.functions import col


spark = (
    SparkSession.builder
    .appName("ValidateSilver")
    .master("spark://spark:7077")
    .getOrCreate()
)

bronze_path = "/opt/spark/data/bronze/sales.csv"
silver_path = "/opt/spark/data/silver/sales"

print("========================================")
print("VALIDATION BRONZE / SILVER")
print("========================================")

print("\n=== LECTURE BRONZE ===")

df_bronze = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(bronze_path)
)

bronze_count = df_bronze.count()

print("Nombre de lignes Bronze :", bronze_count)

print("\n=== LECTURE SILVER ===")

df_silver = spark.read.parquet(silver_path)

silver_count = df_silver.count()

print("Nombre de lignes Silver :", silver_count)

print("\n=== LIGNES SUPPRIMEES ===")

removed_rows = bronze_count - silver_count

print("Nombre de lignes supprimées :", removed_rows)

print("\n=== SCHEMA SILVER ===")

df_silver.printSchema()

print("\n=== DOUBLONS SUR ID ===")

duplicate_count = (
    df_silver
    .groupBy("id")
    .count()
    .filter(col("count") > 1)
    .count()
)

print("Nombre d'ID dupliqués dans Silver :", duplicate_count)

print("\n=== NULL PAR COLONNE ===")

for column_name in df_silver.columns:
    null_count = (
        df_silver
        .filter(col(column_name).isNull())
        .count()
    )

    print(f"{column_name} : {null_count} NULL")

print("\n=== VALIDATION QUANTITY ===")

invalid_quantity = (
    df_silver
    .filter(col("quantity") <= 0)
    .count()
)

print(
    "Nombre de quantity <= 0 :",
    invalid_quantity
)

print("\n=== VALIDATION UNIT_PRICE ===")

invalid_unit_price = (
    df_silver
    .filter(col("unit_price") < 0)
    .count()
)

print(
    "Nombre de unit_price < 0 :",
    invalid_unit_price
)

print("\n=== APERCU SILVER ===")

df_silver.show(
    10,
    truncate=False
)

print("\n========================================")
print("FIN VALIDATION SILVER")
print("========================================")

spark.stop()