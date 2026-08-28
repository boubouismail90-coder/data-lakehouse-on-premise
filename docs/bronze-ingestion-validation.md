# Validation de l'ingestion Bronze

## Objectif

Automatiser l'extraction des données depuis une source PostgreSQL
et stocker les données brutes dans le bucket Bronze de RustFS.

## Source de données

Base PostgreSQL :

`source_db`

Table :

`sales`

Volume de test :

`100000 lignes`

## DAG Airflow

DAG :

`bronze_sales_ingestion`

Tâche :

`extract_and_load_sales_to_bronze`

Planification finale :

`@daily`

## Pipeline

PostgreSQL
→ Airflow
→ génération CSV
→ API S3
→ RustFS Bronze

## Destination Bronze

Les fichiers sont stockés dans une structure similaire à :

`bronze/sales/ingestion_date=YYYY-MM-DD/sales_YYYYMMDD_HHMMSS.csv`

## Logging

Le DAG journalise notamment :

- le début de l'ingestion ;
- la connexion à PostgreSQL ;
- le nombre de lignes extraites ;
- la génération du fichier CSV ;
- la taille du fichier ;
- la connexion à RustFS ;
- l'upload dans Bronze ;
- les erreurs éventuelles.

## Reprise sur erreur

Le DAG est configuré avec :

- `3 retries` ;
- `1 minute` entre les tentatives.

Un test volontaire a été réalisé avec une base PostgreSQL incorrecte.

La tâche est passée en état `À réessayer`, ce qui valide le mécanisme de retry.

## Planification automatique

Le DAG a été temporairement configuré avec :

`*/5 * * * *`

afin de vérifier l'exécution automatique toutes les cinq minutes.

Airflow a déclenché le DAG automatiquement sans intervention manuelle.

Après validation, la planification a été remise à :

`@daily`

## Résultat

Les validations suivantes ont réussi :

- source PostgreSQL contenant 100000 lignes ;
- connexion Airflow vers PostgreSQL ;
- extraction des données ;
- génération du fichier CSV brut ;
- upload dans RustFS Bronze ;
- logging ;
- mécanisme de retry ;
- reprise sur erreur ;
- exécution automatique par le Scheduler.

L'alimentation automatisée de la couche Bronze est validée.