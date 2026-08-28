from pyspark.sql import SparkSession


def create_spark_session():
    return (
        SparkSession.builder
        .appName("DeltaHealthCheck")
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


def main():
    spark = create_spark_session()

    delta_path = "/opt/spark/data/delta-test"

    print("========================================")
    print("DELTA LAKE HEALTH CHECK")
    print("========================================")

    print("\n=== ENVIRONNEMENT ===")
    print("Spark version :", spark.version)
    print("Delta path    :", delta_path)

    print("\n=== CREATION DATAFRAME ===")

    data = [
        (1, "Bronze"),
        (2, "Silver"),
        (3, "Gold"),
    ]

    df = spark.createDataFrame(
        data,
        ["id", "layer"]
    )

    df.show(truncate=False)

    print("\n=== ECRITURE DELTA ===")

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .save(delta_path)
    )

    print(
        "Ecriture Delta terminee dans :",
        delta_path
    )

    print("\n=== LECTURE DELTA ===")

    df_delta = (
        spark.read
        .format("delta")
        .load(delta_path)
    )

    df_delta.show(truncate=False)

    row_count = df_delta.count()

    print(
        "Nombre de lignes Delta :",
        row_count
    )

    print("\n=== VERIFICATION ===")

    if row_count == 3:
        print("DELTA LAKE HEALTH CHECK : SUCCESS")
    else:
        raise RuntimeError(
            f"Nombre de lignes inattendu : {row_count}"
        )

    print("\n========================================")
    print("FIN DELTA LAKE HEALTH CHECK")
    print("========================================")

    spark.stop()


if __name__ == "__main__":
    main()