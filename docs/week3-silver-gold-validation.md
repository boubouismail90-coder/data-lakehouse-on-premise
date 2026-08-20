# Week 3 Validation - Silver & Gold Transformation

## 1. Objectif

La semaine 3 du projet Data Lakehouse On-Premise a pour objectif de transformer les données brutes de la couche Bronze en données nettoyées dans la couche Silver, puis de construire un Data Mart Gold basé sur un schéma en étoile.

Le pipeline utilise :

- Apache Airflow 3.3.0 pour l'orchestration
- Apache Spark 3.5.1 / PySpark pour les transformations
- Delta Lake 3.2.0 pour le format de stockage
- Hadoop AWS 3.3.4 / S3A pour la communication Spark <-> RustFS
- RustFS comme stockage objet S3-compatible

---

## 2. Architecture validée

```text
PostgreSQL source_db.sales
        |
        v
Airflow DAG #1
bronze_sales_ingestion
        |
        v
RustFS Bronze
s3a://bronze/sales/
        |
        v
PySpark
bronze_to_silver_rustfs_delta.py
        |
        v
RustFS Silver Delta
s3a://silver/sales
        |
        v
PySpark
silver_to_gold_rustfs_delta.py
        |
        v
RustFS Gold Delta
        |
        +-- dim_date
        +-- dim_product
        +-- dim_customer
        +-- dim_region
        +-- fact_sales