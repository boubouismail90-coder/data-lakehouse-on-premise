from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, upper, when


spark = (
    SparkSession.builder
    .appName("BronzeToSilver")
    .master("spark://spark:7077")
    .getOrCreate()
)

input_path = "/opt/spark/data/bronze/sales.csv"
output_path = "/opt/spark/data/silver/sales"

print("=== LECTURE BRONZE ===")

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)

print("Nombre de lignes Bronze :", df.count())

df.printSchema()

print("=== NETTOYAGE SILVER ===")

# Suppression des doublons
df_clean = df.dropDuplicates(["id"])

# Suppression des lignes sans identifiant
df_clean = df_clean.filter(
    col("id").isNotNull()
)

# Nettoyage des chaînes
df_clean = df_clean.withColumn(
    "product_name",
    trim(col("product_name"))
)

df_clean = df_clean.withColumn(
    "region",
    trim(col("region"))
)

# Uniformisation des régions
df_clean = df_clean.withColumn(
    "region",
    upper(col("region"))
)

# Suppression des valeurs métier invalides
df_clean = df_clean.filter(
    (col("quantity") > 0)
    & (col("unit_price") >= 0)
)

# Gestion simple des valeurs NULL
df_clean = df_clean.withColumn(
    "product_name",
    when(
        col("product_name").isNull(),
        "UNKNOWN"
    ).otherwise(col("product_name"))
)

df_clean = df_clean.withColumn(
    "region",
    when(
        col("region").isNull(),
        "UNKNOWN"
    ).otherwise(col("region"))
)

print(
    "Nombre de lignes Silver :",
    df_clean.count()
)

df_clean.show(10, truncate=False)

print("=== ECRITURE SILVER ===")

(
    df_clean.write
    .mode("overwrite")
    .parquet(output_path)
)

print(
    "Données Silver écrites dans :",
    output_path
)

spark.stop()