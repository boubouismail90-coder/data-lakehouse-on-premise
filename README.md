# Data Lakehouse On-Premise

Projet de stage d’initiation consacré à la mise en place d’une architecture Data Lakehouse 100 % On-Premise basée sur Docker, Apache Airflow, PostgreSQL et RustFS.

## Objectif du projet

L’objectif est de construire un pipeline de données automatisé organisé selon l’architecture Medallion :

```text
Source PostgreSQL ou API
        |
        v
Apache Airflow
        |
        v
RustFS
├── bronze : données brutes
├── silver : données nettoyées et structurées
└── gold   : données agrégées pour la décision