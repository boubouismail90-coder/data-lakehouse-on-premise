import os
import sys

from pyspark.sql import SparkSession


def get_required_env(name: str) -> str:
    value = os.getenv(name)

    if not value:
        raise RuntimeError(
            f"Variable d'environnement obligatoire absente : {name}"
        )

    return value


def main() -> None:
    host = get_required_env("BI_DB_HOST")
    port = get_required_env("BI_DB_PORT")
    database = get_required_env("BI_DB_NAME")
    user = get_required_env("BI_DB_USER")
    password = get_required_env("BI_DB_PASSWORD")

    jdbc_url = f"jdbc:postgresql://{host}:{port}/{database}"

    print("=" * 70)
    print("POSTGRESQL JDBC CONNECTION TEST")
    print("=" * 70)
    print(f"Host     : {host}")
    print(f"Port     : {port}")
    print(f"Database : {database}")
    print(f"User     : {user}")
    print("Password : SET")
    print(f"JDBC URL : {jdbc_url}")
    print("=" * 70)

    spark = (
        SparkSession.builder
        .appName("PostgreSQL-JDBC-Test")
        .getOrCreate()
    )

    try:
        print("Connexion à PostgreSQL via JDBC...")

        test_df = (
            spark.read
            .format("jdbc")
            .option("url", jdbc_url)
            .option(
                "dbtable",
                "(SELECT current_database() AS database_name, "
                "current_user AS user_name) AS connection_test"
            )
            .option("user", user)
            .option("password", password)
            .option("driver", "org.postgresql.Driver")
            .load()
        )

        test_df.show(truncate=False)

        print("=" * 70)
        print("POSTGRESQL JDBC TEST : SUCCESS")
        print("=" * 70)

    except Exception as exc:
        print("=" * 70)
        print("POSTGRESQL JDBC TEST : FAILED")
        print("=" * 70)
        print(f"Erreur : {type(exc).__name__}")
        print(f"Message : {exc}")
        print("=" * 70)

        raise

    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception:
        sys.exit(1)