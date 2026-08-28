# Data Lakehouse On-Premise

Projet de stage consacré à la mise en place d'une architecture **Data Lakehouse 100 % On-Premise**, basée sur Docker, Apache Airflow, PostgreSQL, RustFS, Apache Spark, PySpark, Delta Lake et Metabase.

L'objectif du projet est de construire un pipeline de données automatisé permettant d'extraire des données depuis PostgreSQL, de les stocker selon une architecture Medallion, de les transformer avec Apache Spark et Delta Lake, puis de les exposer dans PostgreSQL pour l'analyse décisionnelle avec Metabase.

---

## 1. Objectif du projet

L'objectif principal est de mettre en place un pipeline Data Lakehouse permettant de :

- récupérer les données depuis une base PostgreSQL source ;
- automatiser les traitements avec Apache Airflow ;
- stocker les données brutes dans RustFS ;
- organiser les données selon l'architecture Medallion ;
- nettoyer et transformer les données avec Apache Spark / PySpark ;
- utiliser Delta Lake pour les couches Silver et Gold ;
- construire un modèle dimensionnel en étoile ;
- charger les données Gold dans une base PostgreSQL dédiée à la BI ;
- connecter PostgreSQL à Metabase ;
- créer des analyses et des dashboards ;
- mettre en place des contrôles de qualité ;
- développer des scripts de validation et de diagnostic ;
- automatiser le pipeline de traitement des données.

---

## 2. Architecture globale

Le pipeline final est organisé comme suit :

```text
PostgreSQL Source
       |
       v
Apache Airflow
       |
       v
RustFS Bronze
       |
       v
Apache Spark / PySpark
       |
       v
RustFS Silver
       |
       v
Spark + Delta Lake
       |
       v
RustFS Gold
       |
       v
PostgreSQL BI
       |
       v
Metabase
```

L'architecture suit le principe **Medallion Architecture** :

```text
                         DATA LAKEHOUSE
                              |
              +---------------+---------------+
              |               |               |
            Bronze          Silver           Gold
              |               |               |
             Raw           Cleaned         Business
             Data            Data            Data
```

---

## 3. Architecture Medallion

### 3.1 Couche Bronze

La couche Bronze contient les données brutes provenant de la base PostgreSQL source.

L'ingestion est orchestrée par Apache Airflow.

```text
PostgreSQL Source
       |
       v
Apache Airflow
       |
       v
RustFS Bronze
```

Les données sont extraites depuis la table `sales` de PostgreSQL puis stockées dans RustFS.

Exemple de structure :

```text
bronze/
└── sales/
    └── ingestion_date=YYYY-MM-DD/
        └── sales_YYYYMMDD_HHMMSS.csv
```

La couche Bronze conserve les données dans leur état brut avant leur transformation.

---

### 3.2 Couche Silver

La couche Silver contient les données nettoyées et structurées.

Les traitements sont réalisés avec Apache Spark et PySpark.

Les principales opérations comprennent :

- lecture des données Bronze ;
- correction et conversion des types ;
- gestion des valeurs NULL ;
- suppression des doublons ;
- contrôle des valeurs invalides ;
- contrôle de la cohérence des données ;
- validation de la qualité des données ;
- écriture des données nettoyées dans RustFS ;
- stockage au format Delta Lake.

```text
RustFS Bronze
      |
      v
Apache Spark / PySpark
      |
      v
Nettoyage
      |
      v
Validation
      |
      v
RustFS Silver
```

---

### 3.3 Couche Gold

La couche Gold contient les données préparées pour l'analyse décisionnelle.

Elle est construite à partir des données Silver avec Apache Spark et Delta Lake.

Le modèle Gold utilise un **schéma en étoile**.

Il est composé de quatre dimensions et d'une table de faits.

#### Dimensions

```text
DimDate
DimProduct
DimCustomer
DimRegion
```

#### Table de faits

```text
FactSales
```

Architecture du modèle :

```text
                    DimDate
                       |
                       |
DimProduct -------- FactSales -------- DimCustomer
                       |
                       |
                   DimRegion
```

La table `FactSales` contient les mesures nécessaires à l'analyse des ventes ainsi que les clés étrangères permettant de relier les différentes dimensions.

---

## 4. Modèle de données Gold

### 4.1 DimDate

Dimension temporelle utilisée pour analyser les ventes selon les dates.

```text
date_key
full_date
```

La clé `date_key` est utilisée dans la table de faits afin de relier chaque vente à sa date.

---

### 4.2 DimProduct

Dimension produit.

```text
product_key
product_id
product_name
```

`product_key` est la clé technique de la dimension.

`product_id` représente l'identifiant du produit provenant des données sources.

`product_name` représente le nom du produit.

Une validation est réalisée afin de détecter les incohérences entre `product_id` et `product_name`.

---

### 4.3 DimCustomer

Dimension client.

```text
customer_key
customer_id
```

`customer_key` est la clé technique utilisée dans le modèle Gold.

`customer_id` représente l'identifiant client provenant de la source.

---

### 4.4 DimRegion

Dimension région.

```text
region_key
region_name
```

Cette dimension permet d'effectuer des analyses selon la région.

---

### 4.5 FactSales

Table de faits contenant les ventes.

```text
sale_id
date_key
product_key
customer_key
region_key
quantity
unit_price
total_amount
created_at
```

Le grain de `FactSales` est :

```text
1 ligne FactSales = 1 vente Silver
```

Le montant total est calculé à partir de :

```text
total_amount = quantity × unit_price
```

La table de faits permet ensuite de réaliser des analyses croisées avec les dimensions Date, Produit, Client et Région.

---

## 5. Contrôles de qualité des données

Des contrôles de qualité sont réalisés au cours du pipeline afin de garantir la cohérence des données.

### 5.1 Validation Silver

Les données Silver sont contrôlées avant leur utilisation dans la couche Gold.

Les contrôles comprennent notamment :

- vérification du nombre de lignes ;
- vérification des colonnes attendues ;
- contrôle des valeurs NULL ;
- contrôle des types de données ;
- détection des doublons ;
- contrôle des valeurs invalides ;
- contrôle de la cohérence des données.

---

### 5.2 Validation Gold

Les contrôles de la couche Gold comprennent notamment :

#### Vérification du nombre de lignes

Le nombre de lignes de `FactSales` doit correspondre au nombre de lignes Silver.

```text
Silver count = FactSales count
```

#### Vérification des clés étrangères

Les clés suivantes ne doivent pas être NULL :

```text
date_key
product_key
customer_key
region_key
```

#### Vérification des doublons

`FactSales` ne doit pas contenir plusieurs lignes pour un même :

```text
sale_id
```

#### Vérification des montants

Les montants négatifs sont détectés.

```text
total_amount >= 0
```

---

## 6. Delta Lake

Delta Lake est utilisé pour les couches Silver et Gold.

Les tables Gold sont stockées au format Delta dans RustFS.

Chaque table Delta possède un répertoire `_delta_log`.

Exemple :

```text
gold/
└── fact_sales/
    ├── _delta_log/
    │   ├── 00000000000000000000.json
    │   └── ...
    │
    └── part-*.snappy.parquet
```

Le répertoire `_delta_log` contient les informations nécessaires au suivi des transactions et des versions des tables Delta.

Les tables Gold suivantes sont stockées dans RustFS :

```text
dim_date
dim_product
dim_customer
dim_region
fact_sales
```

---

## 7. Business Intelligence

Une base PostgreSQL dédiée à la BI est utilisée afin d'exposer les données Gold à Metabase.

Le flux est :

```text
RustFS Gold
     |
     v
Apache Spark
     |
     v
PostgreSQL BI
     |
     v
Metabase
```

Les cinq tables suivantes sont chargées dans PostgreSQL BI :

```text
dim_date
dim_product
dim_customer
dim_region
fact_sales
```

Ces tables servent de source aux analyses et dashboards Metabase.

Les données peuvent être utilisées pour créer :

- des indicateurs ;
- des analyses ;
- des graphiques ;
- des tableaux ;
- des dashboards ;
- des analyses par date ;
- des analyses par produit ;
- des analyses par client ;
- des analyses par région.

---

## 8. Orchestration avec Apache Airflow

Apache Airflow est utilisé pour orchestrer les différentes étapes du pipeline.

Les principaux DAGs du projet sont :

```text
environment_health_check
rustfs_connection_health_check
bronze_sales_ingestion
silver_gold_transformation
```

### DAG d'ingestion Bronze

```text
bronze_sales_ingestion
```

Ce DAG permet d'extraire les données depuis PostgreSQL et de les déposer dans RustFS Bronze.

---

### DAG de transformation

```text
silver_gold_transformation
```

Ce DAG orchestre les principales étapes de transformation.

Le pipeline suit l'ordre :

```text
bronze_to_silver_delta
        |
        v
silver_to_gold_delta
        |
        v
gold_to_bi
```

Ainsi :

1. les données Bronze sont transformées en Silver ;
2. les données Silver sont transformées en Gold ;
3. les données Gold sont chargées dans PostgreSQL BI.

---

### DAG de vérification de l'environnement

```text
environment_health_check
```

Ce DAG permet de vérifier l'état général de l'environnement Airflow.

---

### DAG de vérification RustFS

```text
rustfs_connection_health_check
```

Ce DAG permet de vérifier la connexion entre Airflow et RustFS.

---

## 9. Scripts Spark

Plusieurs scripts ont été développés pour les traitements, les validations et les diagnostics.

```text
spark/jobs/
├── bronze_to_silver.py
├── bronze_to_silver_delta.py
├── delta_health_check.py
├── diagnose_product_mapping.py
├── gold_to_bi.py
├── inspect_gold.py
├── postgres_jdbc_test.py
├── silver_to_gold_delta.py
└── validate_silver.py
```

### bronze_to_silver.py

Script utilisé pour le traitement Bronze vers Silver.

### bronze_to_silver_delta.py

Script utilisé pour la transformation Bronze vers Silver avec Delta Lake.

### silver_to_gold_delta.py

Script principal de création du modèle Gold et des tables Delta.

### gold_to_bi.py

Script permettant de charger les tables Gold dans PostgreSQL BI via JDBC.

### delta_health_check.py

Script permettant de vérifier l'état des tables Delta.

### diagnose_product_mapping.py

Script permettant de diagnostiquer les problèmes éventuels de correspondance entre les produits.

### inspect_gold.py

Script permettant d'inspecter les tables Gold.

### postgres_jdbc_test.py

Script permettant de tester la connexion JDBC avec PostgreSQL.

### validate_silver.py

Script permettant de réaliser les contrôles de validation de la couche Silver.

---

## 10. Technologies utilisées

| Technologie | Utilisation |
|---|---|
| Docker | Conteneurisation de l'environnement |
| Docker Compose | Orchestration des services |
| Apache Airflow | Orchestration des pipelines |
| PostgreSQL | Base de données source et base BI |
| RustFS | Stockage objet S3-compatible |
| Apache Spark | Traitement des données |
| PySpark | Développement des traitements Spark |
| Delta Lake | Stockage transactionnel des couches Silver et Gold |
| Metabase | Business Intelligence et visualisation |
| Python | Développement des scripts |
| Git | Gestion des versions |
| GitHub | Hébergement du projet et gestion des Pull Requests |

---

## 11. Services Docker

L'environnement Docker Compose contient les principaux services suivants :

```text
postgres
airflow-init
airflow-api-server
airflow-scheduler
airflow-dag-processor
rustfs
spark
spark-worker
metabase
```

La configuration principale est définie dans :

```text
docker-compose.yml
```

---

## 12. Interfaces des services

### Apache Airflow

```text
http://localhost:8080
```

Airflow permet de consulter, exécuter et superviser les DAGs.

---

### RustFS

```text
http://localhost:9001/rustfs/console/
```

RustFS permet d'inspecter les buckets et les données stockées.

Les principaux buckets utilisés sont :

```text
bronze
silver
gold
```

---

### Apache Spark

#### Spark Master

```text
http://localhost:8081
```

#### Spark Worker

```text
http://localhost:8082
```

Ces interfaces permettent notamment de vérifier l'état du cluster Spark.

---

### Metabase

Metabase est utilisé comme interface de Business Intelligence.

Il permet d'exploiter les données présentes dans PostgreSQL BI afin de créer les analyses et dashboards du projet.

---

## 13. Structure du projet

La structure principale du projet est organisée comme suit :

```text
data-lakehouse-on-premise/
│
├── airflow/
│   ├── config/
│   │
│   ├── dags/
│   │   ├── bronze_sales_ingestion.py
│   │   ├── environment_health_check.py
│   │   ├── rustfs_connection_health_check.py
│   │   └── silver_gold_transformation.py
│   │
│   └── logs/
│
├── spark/
│   └── jobs/
│       ├── bronze_to_silver.py
│       ├── bronze_to_silver_delta.py
│       ├── delta_health_check.py
│       ├── diagnose_product_mapping.py
│       ├── gold_to_bi.py
│       ├── inspect_gold.py
│       ├── postgres_jdbc_test.py
│       ├── silver_to_gold_delta.py
│       └── validate_silver.py
│
├── data/
│
├── Dockerfile.airflow
├── docker-compose.yml
├── docker-compose.postgres.backup.yml
├── .env.example
├── .gitignore
├── README.md
└── psql
```

Les données générées localement, les logs, les caches et les fichiers temporaires ne sont pas destinés à être versionnés.

---

## 14. Configuration

Les paramètres sensibles sont stockés dans :

```text
.env
```

Le fichier `.env` n'est pas versionné dans Git.

Un modèle de configuration est fourni dans :

```text
.env.example
```

Les variables d'environnement peuvent notamment concerner :

```text
PostgreSQL
RustFS
Spark
Airflow
PostgreSQL BI
Metabase
```

---

## 15. Lancement du projet

Depuis le répertoire du projet :

```powershell
docker compose up -d
```

Cette commande démarre les services définis dans `docker-compose.yml` en arrière-plan.

Pour vérifier l'état des services :

```powershell
docker compose ps
```

Pour arrêter les services :

```powershell
docker compose down
```

Pour afficher les logs :

```powershell
docker compose logs
```

Pour afficher les logs d'un service spécifique :

```powershell
docker compose logs airflow-scheduler
```

ou :

```powershell
docker compose logs spark
```

---

## 16. Vérification de l'environnement

Après le démarrage de Docker Compose, l'état des services peut être vérifié avec :

```powershell
docker compose ps
```

Les principaux services doivent être disponibles avant l'exécution du pipeline.

Les DAGs de contrôle permettent également de vérifier les composants de l'environnement.

```text
environment_health_check
rustfs_connection_health_check
```

---

## 17. Exécution du pipeline

Le pipeline peut être exécuté et supervisé depuis Apache Airflow.

L'ordre général des traitements est :

```text
1. PostgreSQL Source
          |
          v
2. Bronze Ingestion
          |
          v
3. Bronze → Silver
          |
          v
4. Silver → Gold
          |
          v
5. Gold → PostgreSQL BI
          |
          v
6. Analyse avec Metabase
```

Chaque étape possède des contrôles permettant de vérifier la cohérence des données avant de poursuivre le traitement.

---

## 18. Pipeline complet

Le pipeline final peut être résumé ainsi :

```text
                         SOURCE
                           |
                           v
                    PostgreSQL Source
                           |
                           v
                       Airflow
                           |
                           v
                    +-------------+
                    |   BRONZE    |
                    |    RustFS   |
                    |   Raw Data  |
                    +-------------+
                           |
                           v
                    Apache Spark
                           |
                           v
                    +-------------+
                    |   SILVER    |
                    | RustFS/Delta|
                    | Clean Data  |
                    +-------------+
                           |
                           v
                     Spark + Delta
                           |
                           v
                    +-------------+
                    |    GOLD     |
                    | RustFS/Delta|
                    | Star Schema |
                    +-------------+
                           |
                           v
                    PostgreSQL BI
                           |
                           v
                       Metabase
                           |
                           v
                 Analyses / KPI /
                    Dashboards
```

---

## 19. Résultat final

Le projet permet de disposer d'une architecture Data Lakehouse entièrement exécutée dans un environnement local et conteneurisé.

Le pipeline réalise les étapes suivantes :

```text
Extraction
    ↓
Ingestion
    ↓
Stockage Bronze
    ↓
Nettoyage Silver
    ↓
Validation
    ↓
Transformation Gold
    ↓
Modèle en étoile
    ↓
Chargement PostgreSQL BI
    ↓
Analyse Metabase
```

Les données finales sont disponibles dans les cinq tables BI :

```text
dim_date
dim_product
dim_customer
dim_region
fact_sales
```

Les données Gold sont stockées dans RustFS au format Delta Lake puis chargées dans PostgreSQL BI pour leur exploitation par Metabase.

Le pipeline a été exécuté avec succès et les données sont disponibles dans Metabase pour les analyses et dashboards.

---

## 20. Organisation du projet par étapes

Le projet a été réalisé progressivement.

### Étape 1 - Mise en place de l'environnement

- installation et configuration de Docker ;
- création de l'environnement Docker Compose ;
- mise en place de PostgreSQL ;
- mise en place d'Apache Airflow ;
- mise en place de RustFS ;
- configuration du réseau Docker ;
- configuration des volumes ;
- configuration des services ;
- vérification de l'environnement.

---

### Étape 2 - Ingestion Bronze

- sélection de PostgreSQL comme source ;
- utilisation de la table `sales` ;
- développement du DAG d'ingestion ;
- extraction des données avec Python et `psycopg2` ;
- génération des fichiers CSV ;
- stockage dans RustFS Bronze ;
- ajout des logs ;
- configuration des retries ;
- validation de l'ingestion.

---

### Étape 3 - Transformation Silver

- lecture des données Bronze avec Spark ;
- nettoyage des données ;
- correction des types ;
- gestion des valeurs NULL ;
- suppression des doublons ;
- contrôle de qualité ;
- création de la couche Silver ;
- utilisation de Delta Lake ;
- validation des données Silver.

---

### Étape 4 - Transformation Gold

- lecture des données Silver ;
- création de la dimension Date ;
- création de la dimension Produit ;
- création de la dimension Client ;
- création de la dimension Région ;
- création de la table de faits FactSales ;
- construction du schéma en étoile ;
- validation des clés étrangères ;
- validation du nombre de lignes ;
- validation des doublons ;
- validation des montants ;
- écriture des tables Gold dans RustFS ;
- vérification des `_delta_log`.

---

### Étape 5 - BI et Dashboard

- création de la base PostgreSQL BI ;
- chargement des tables Gold vers PostgreSQL BI ;
- utilisation de JDBC pour le transfert ;
- connexion de PostgreSQL à Metabase ;
- validation des cinq tables BI ;
- création des analyses ;
- création des dashboards ;
- validation du pipeline complet.

---

## 21. Architecture technique finale

```text
+-----------------------+
| PostgreSQL Source     |
+-----------------------+
            |
            v
+-----------------------+
| Apache Airflow        |
| Orchestration         |
+-----------------------+
            |
            v
+-----------------------+
| RustFS - Bronze       |
| Données brutes        |
+-----------------------+
            |
            v
+-----------------------+
| Apache Spark / PySpark|
| Transformation        |
+-----------------------+
            |
            v
+-----------------------+
| RustFS - Silver       |
| Données nettoyées     |
| Delta Lake            |
+-----------------------+
            |
            v
+-----------------------+
| Apache Spark          |
| + Delta Lake          |
+-----------------------+
            |
            v
+-----------------------+
| RustFS - Gold         |
| Schéma en étoile      |
| Delta Lake            |
+-----------------------+
            |
            v
+-----------------------+
| PostgreSQL BI         |
+-----------------------+
            |
            v
+-----------------------+
| Metabase              |
| BI / Dashboards       |
+-----------------------+
```

---

## 22. Gestion du code source

Le projet utilise Git et GitHub pour assurer le suivi des modifications.

Les principales branches sont :

```text
main
dev
feature/*
```

La stratégie de développement est la suivante :

```text
feature/*
     |
     v
Pull Request
     |
     v
dev
     |
     v
Pull Request
     |
     v
main
```

Les modifications ne sont pas poussées directement vers `dev` ou `main`.

Chaque fonctionnalité ou étape importante est développée dans une branche dédiée puis intégrée avec une Pull Request.

Cette organisation permet de conserver une branche `main` stable et une branche `dev` utilisée pour l'intégration des fonctionnalités.

---

## 23. Conclusion

Ce projet met en œuvre une architecture **Data Lakehouse On-Premise** complète permettant de gérer le cycle de traitement des données depuis leur extraction jusqu'à leur exploitation décisionnelle.

L'association de :

```text
Docker
Apache Airflow
PostgreSQL
RustFS
Apache Spark
PySpark
Delta Lake
Metabase
```

permet de construire une plateforme locale capable de :

- centraliser le stockage des données ;
- automatiser les traitements ;
- nettoyer et transformer les données ;
- contrôler leur qualité ;
- construire un modèle analytique en étoile ;
- stocker les données analytiques avec Delta Lake ;
- exposer les données dans PostgreSQL BI ;
- réaliser des analyses et dashboards avec Metabase.

Le pipeline final est :

```text
PostgreSQL
    ↓
Airflow
    ↓
RustFS Bronze
    ↓
Spark / PySpark
    ↓
RustFS Silver + Delta Lake
    ↓
Spark + Delta Lake
    ↓
RustFS Gold + Delta Lake
    ↓
PostgreSQL BI
    ↓
Metabase
```

**Data Lakehouse On-Premise : de la donnée brute à la décision.**