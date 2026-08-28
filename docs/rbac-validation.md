# Validation RBAC Apache Airflow

## Objectif

Valider la séparation des droits entre un administrateur et un utilisateur
en lecture seule dans l’environnement Apache Airflow du Data Lakehouse.

## Configuration

Le Simple Auth Manager d’Airflow est utilisé avec les utilisateurs suivants :

| Utilisateur | Rôle | Utilisation |
|---|---|---|
| admin | admin | Administration complète de l’environnement |
| viewer | viewer | Consultation en lecture seule |

La configuration est déclarée dans `docker-compose.yml` :

```yaml
AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_USERS: "admin:admin,viewer:viewer"