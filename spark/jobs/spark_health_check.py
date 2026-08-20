from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("SparkHealthCheck")
    .master("spark://spark:7077")
    .getOrCreate()
)

data = [
    (1, "Bronze"),
    (2, "Silver"),
    (3, "Gold"),
]

df = spark.createDataFrame(
    data,
    ["id", "layer"]
)

print("=== TEST PYSPARK ===")
df.show()

print("Nombre de lignes :", df.count())

spark.stop()